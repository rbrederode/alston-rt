import logging
import json
import map
import os
import re
from pathlib import Path
import socket
import time
import threading
from datetime import datetime, timezone, timedelta

# Import google api tools
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpRequest

# Import application modules
from api import tm_dig, tm_sdp, tm_dm
from env.app import App
from env.events import ConnectEvent, DisconnectEvent, DataEvent, ConfigEvent, ObsEvent
from ipc.message import AppMessage, APIMessage
from ipc.action import Action
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.app import AppModel
from models.base import BaseModel
from models.comms import CommunicationStatus, InterfaceType
from models.dig import DigitiserModel
from models.dsh import DishManagerModel, Feed, Capability, DishMode, PointingState
from models.obs import Observation, ObsTransition, ObsState
from models.oda import ODAModel, ObsList, ScanStore
from models.health import HealthState
from models.scan import ScanModel, ScanState
from models.sdp import ScienceDataProcessorModel
from models.telescope import TelescopeModel
from models.tm import ResourceType, AllocationState
from obs.oet import ObservationExecutionTool
from util import log, util
from util.timer import Timer, TimerManager
from util.xbase import XBase, XStreamUnableToExtract, XUnknownEntity
from webhook_handler import WebhookHandler

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The SHEET ID for the ALSTON RADIO TELESCOPE google sheet
ALSTON_RADIO_TELESCOPE = "1r73N0VZHSQC6RjRv94gzY50pTgRvaQWfctGGOMZVpzc"

TM_UI_API = "TM_UI_API!"            # Range for UI-TM API data
TM_UI_UPDATE_INTERVAL_S = 45        # Update interval in seconds

ODT_OBS_LIST = TM_UI_API + "D2"     # Range for Observation Design Tool
DIG001_CONFIG = TM_UI_API + "B3"    # Range for Digitiser 001 configuration

class TelescopeManager(App):

    telmodel = TelescopeModel()

    def __init__(self, app_name: str = "tm"):

        super().__init__(app_name=app_name, app_model=self.telmodel.tel_mgr.app)

        # Lock for thread-safe allocation of shared resources
        self._rlock = threading.RLock()  

        # Observation Execution Tool is an internal component of the TM used to manage observation workflows
        self.oet = ObservationExecutionTool(telmodel=self.telmodel, tm=self)

        # Dish Manager interface
        self.dm_system = "dm"
        self.dm_api = tm_dm.TM_DM()
        # Dish Manager TCP Client
        self.dm_endpoint = TCPClient(description=self.dm_system, queue=self.get_queue(), host=self.get_args().dm_host, port=self.get_args().dm_port)
        self.dm_endpoint.connect()
        # Register Dish Manager interface with the App
        self.register_interface(self.dm_system, self.dm_api, self.dm_endpoint, InterfaceType.APP_APP)
        # Initialise Dish Manager comms status
        self.telmodel.dsh_mgr.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.dm_connected = CommunicationStatus.NOT_ESTABLISHED

        # Digitiser interface
        self.dig_system = "dig"
        self.dig_api = tm_dig.TM_DIG()
        # Digitiser TCP Server
        self.dig_endpoint = TCPServer(description=self.dig_system, queue=self.get_queue(), host=self.get_args().dig_host, port=self.get_args().dig_port)
        self.dig_endpoint.start()
        # Register Digitiser interface with the App
        self.register_interface(self.dig_system, self.dig_api, self.dig_endpoint, InterfaceType.ENTITY_DRIVER)
        # Entity drivers maintain comms status per entity, so no need to initialise comms status here
        
        # Science Data Processor interface 
        self.sdp_system = "sdp"
        self.sdp_api = tm_sdp.TM_SDP()
        # Science Data Processor TCP Client
        self.sdp_endpoint = TCPClient(description=self.sdp_system, queue=self.get_queue(), host=self.get_args().sdp_host, port=self.get_args().sdp_port)
        self.sdp_endpoint.connect()
        # Register Science Data Processor interface with the App
        self.register_interface(self.sdp_system, self.sdp_api, self.sdp_endpoint, InterfaceType.APP_APP)
        # Initialise Science Data Processor comms status
        self.telmodel.sdp.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.sdp_connected = CommunicationStatus.NOT_ESTABLISHED

    def add_args(self, arg_parser): 
        """ Specifies the digitiser's command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--dig_host", type=str, required=False, help="TCP server host to listen for Digitiser connections", default="localhost")
        arg_parser.add_argument("--dig_port", type=int, required=False, help="TCP server port to listen for Digitiser connections", default=50000) 

        arg_parser.add_argument("--sdp_host", type=str, required=False, help="TCP server host to connect to the Science Data Processor",default="localhost")
        arg_parser.add_argument("--sdp_port", type=int, required=False, help="TCP server port to connect to the Science Data Processor", default=50001)

        arg_parser.add_argument("--dm_host", type=str, required=False, help="TCP server host to connect to the Dish Manager", default="localhost")
        arg_parser.add_argument("--dm_port", type=int, required=False, help="TCP server port to connect to the Dish Manager", default=50002) 

    def process_init(self) -> Action:
        """ Processes initialisation event on startup once all app processors are running.
            Runs in single threaded mode and switches to multi-threading mode after this method completes.
        """
        logger.debug(f"TM initialisation event")

        # Load Digitiser configuration from disk
        # Config file is located in ./config/<profile>/<model>.json
        # Config file defines initial list of digitisers to be processed by the TM
        input_dir = f"./config/{self.get_args().profile}"
        filename = "DigitiserList.json"

        try:
            dig_store = self.telmodel.dig_store.load_from_disk(input_dir=input_dir, filename=filename)
        except FileNotFoundError:
            dig_store = None

        if dig_store is not None:
            self.telmodel.dig_store = dig_store
            logger.info(f"Telescope Manager loaded Digitiser configuration from directory {input_dir} file {filename}")
        else:
            logger.warning(f"Telescope Manager could not load Digitiser configuration from directory {input_dir} file {filename}")

        action = Action()
        return action

    def process_config(self, event: ConfigEvent) -> Action:
        """ Processes configuration update events.
        """
        logger.info(f"Telescope Manager received updated configuration: {event}")

        action = Action()

        if event.category.upper() == "DIG": # Digitiser Config Event
            action = self.update_dig_configuration(event.old_config, event.new_config, action)

        elif event.category.upper() == "DSH": # Scheduler Config Event
            action = self.update_dsh_configuration(event.old_config, event.new_config, action)

        elif event.category.upper() == "ODT": # Observation Design Tool Config Event
            # Observation Design Tool (ODT) is the source of truth for new (ObsState = EMPTY) observations
            # Observation Data Archive (ODA) is the source of truth for in progress (ObsState != EMPTY) observations

            # Extract a list of ObsState = EMPTY observations from the incoming ODT configuration event (JSON)
            odt = ObsList.from_dict(event.new_config)
            odt_empty_obs = [obs for obs in odt.obs_list if obs.obs_state == ObsState.EMPTY]
            
            # Create dictionary of EMPTY ODT observation ids for quick lookup
            odt_empty_obs_dict = {obs.obs_id: obs for obs in odt_empty_obs}
            odt_empty_obs_ids = set(odt_empty_obs_dict.keys())

            logger.info(f"Received {len(odt.obs_list)} ODT observations, with {len(odt_empty_obs)} in ObsState.EMPTY")
            
            # Iterate through existing ODA observations and update/remove EMPTY observations as needed
            for i, existing_obs in enumerate(self.telmodel.oda.obs_store.obs_list):

                if existing_obs.obs_state == ObsState.EMPTY:
                    
                    if existing_obs.obs_id in odt_empty_obs_dict:
                        # Update existing EMPTY observations in the ODA with new data from ODT
                        logger.info(f"Updating existing EMPTY observation {existing_obs.obs_id} with new data from ODT")
                        self.telmodel.oda.obs_store.obs_list[i] = odt_empty_obs_dict[existing_obs.obs_id]
                    else: 
                        # Remove EMPTY observations from ODA that are no longer in ODT
                        logger.info(f"Removing existing EMPTY observation {existing_obs.obs_id} as it is no longer present in ODT")
                        obs = self.telmodel.oda.obs_store.obs_list.pop(i)
                                                   
            # Add new EMPTY observations from ODT to ODA
            for odt_obs in odt_empty_obs:
                if not any(existing_obs.obs_id == odt_obs.obs_id for existing_obs in self.telmodel.oda.obs_store.obs_list):
                    logger.info(f"Adding new observation {odt_obs.obs_id} from ODT to ODA")

                    # START DEBUG CODE, REMOVE LATER
                    current_dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")                
                    new_obs_id = re.sub(r"^.*?(-Dish\d{3})", current_dt_str + r"\1", odt_obs.obs_id)
    
                    odt_obs.obs_id = new_obs_id
                    odt_obs.scheduling_block_start = datetime.now(timezone.utc) + timedelta(seconds=10)
                    odt_obs.scheduling_block_end = odt_obs.scheduling_block_start + timedelta(seconds=610)

                    # Inform the Science Data Processor that we are resetting these observations (in case they have been run already)
                    old_config = {}
                    new_config = {}

                    old_config['obs_reset'] = None
                    new_config['obs_reset'] = new_obs_id
                    new_config['sdp_id'] = self.telmodel.sdp.sdp_id

                    self.update_sdp_configuration(old_config, new_config, action)
                    # END DEBUG CODE, REMOVE LATER

                    self.telmodel.oda.obs_store.obs_list.append(odt_obs)

            # Start timer to initiate the next scheduled observation if applicable
            self.oet.start_next_obs_timer(action)

        else:
            logger.info(f"Telescope Manager updated configuration received for {event.category}.")

        return action

    def process_obs_event(self, event: ObsEvent) -> Action:
        """ Defer workflow transitions on observations to the Observation Execution Tool (OET).
            Returns an Action object with actions to be performed.
        """
        return self.oet.process_obs_event(event)

    def process_dm_connected(self, event) -> Action:
        """ Processes Dish Manager connected events.
        """
        logger.info(f"Telescope Manager connected to Dish Manager: {event.remote_addr}")

        self.telmodel.dsh_mgr.tm_connected = CommunicationStatus.ESTABLISHED
        self.telmodel.tel_mgr.dm_connected = CommunicationStatus.ESTABLISHED

        action = Action()
        return action

    def process_dm_disconnected(self, event) -> Action:
        """ Processes Dish Manager disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Dish Manager: {event.remote_addr}")

        self.telmodel.dsh_mgr.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.dm_connected = CommunicationStatus.NOT_ESTABLISHED

        return self.abort_all_observations()

    def process_dm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Dish Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received Dish Manager {api_call['msg_type']} message with action code: {api_call['action_code']}")
        
        action = Action()

        # Extract datetime, dish id from API message and lookup Dish Model
        dt = api_msg.get("timestamp")
        dsh_id = api_msg.get("entity", None) 
        dsh_model = self.telmodel.dsh_mgr.get_dish_by_id(dsh_id) if dsh_id is not None and dsh_id != "" else None

        # If the Dish ID is specified in the API message but not found in the Dish Manager model, raise an exception
        if dsh_id is not None and dsh_model is None:
            raise XUnknownEntity(f"Telescope Manager received Dish Manager API message for unknown dish {dsh_id}.\n{api_call}")
        
        # If the api call indicates that an error occured
        if api_call.get('status','') == tm_dm.STATUS_ERROR:
            
            logger.error(f"Telescope Manager received error response from Dish Manager for dish {dsh_id}.\n{api_call}")
            dsh_model.last_err_msg = api_call['message'] if 'message' in api_call else dsh_model.last_err_msg
            dsh_model.last_err_dt = datetime.fromisoformat(dt) if dt is not None else datetime.now(timezone.utc)

            # If the message contains additional observation data, trigger the observation workflow
            obs_data = api_call.get('obs_data', None)
            obs_id = obs_data.get('obs_id', None) if obs_data is not None and isinstance(obs_data, dict) else None
            obs = self.telmodel.oda.obs_store.get_obs_by_id(obs_id) if obs_id is not None else None
                
            # If the related observation was identified, trigger the workflow to move to ABORT
            if obs is not None:
                action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)

        # If the api call does not indicate that an error occured
        elif api_call.get('status','') != tm_dm.STATUS_ERROR:

            # If the api call is a dish mode set property rsp message, update the Dish Model
            if api_call.get('property','') == tm_dm.PROPERTY_MODE:
                dsh_model.mode = DishMode(api_call['value'])

            # If the api call is a capability state set property rsp message, update the Dish Model
            elif api_call.get('property','') == tm_dm.PROPERTY_CAPABILITY:
                dsh_model.capability = Capability(api_call['value'])

            # If the api call is a status update message, update the Dish Manager model
            elif api_call.get('property','') == tm_dm.PROPERTY_STATUS:
                logger.debug(f"Telescope Manager received Dish Manager STATUS update: {api_call['value']}")
                self.telmodel.dsh_mgr = DishManagerModel.from_dict(api_call['value'])

                # If the status update message contains additional observation data, trigger the observation workflow
                obs_data = api_call.get('obs_data', None)
                obs_id = obs_data.get('obs_id', None) if obs_data is not None and isinstance(obs_data, dict) else None
                obs = self.telmodel.oda.obs_store.get_obs_by_id(obs_id) if obs_id is not None else None
                    
                # If the observation is still in CONFIGURING state, trigger the workflow to attempt to move to READY
                if obs is not None and obs.obs_state == ObsState.CONFIGURING:
                    action.set_obs_transition(obs=obs, transition=ObsTransition.CONFIGURE_RESOURCES)

                logger.info(f"Telescope Manager received Dish Manager status update{f' for observation {obs_id}' if obs_id is not None else ''}.")

            # Update the last update timestamp on the Dish Manager model
            self.telmodel.dsh_mgr.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message, stop the corresponding retry timers
        if api_call['msg_type'] == tm_dm.MSG_TYPE_RSP:
            if dt is not None and dsh_id is not None: # Do not set this to None (it breaks things !)
                action.set_timer_action(Action.Timer(name=f"{dsh_id}_req_timer_retry:{dt}", timer_action=Action.Timer.TIMER_STOP))
                action.set_timer_action(Action.Timer(name=f"{dsh_id}_req_timer_final:{dt}", timer_action=Action.Timer.TIMER_STOP))

        return action

    def get_dig_entity(self, event) -> (str, BaseModel):
        """ Determines the digitiser entity ID based on the remote address of a ConnectEvent, DisconnectEvent, or DataEvent.
            Returns a tuple of the entity ID and entity if found, else None, None.
        """
        logger.debug(f"Telescope Manager finding digitiser entity ID for remote address: {event.remote_addr[0]}")

        for digitiser in self.telmodel.dig_store.dig_list:

            if isinstance(digitiser.app.arguments, dict) and "local_host" in digitiser.app.arguments:

                if digitiser.app.arguments["local_host"] == event.remote_addr[0]:
                    logger.info(f"Telescope Manager found digitiser entity ID: {digitiser.dig_id} for remote address: {event.remote_addr}")
                    return digitiser.dig_id, digitiser
            else:
                logger.warning(f"Telescope Manager digitiser {digitiser.dig_id} is not configured with a valid local_host argument to match against remote address: {event.remote_addr[0]}")

        return None, None

    def process_dig_entity_connected(self, event, entity) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Telescope Manager connected to Digitiser entity on {event.remote_addr}\n{entity}")
        digitiser: DigitiserModel = entity if entity is not None and isinstance(entity, DigitiserModel) else None

        if digitiser is not None:
            digitiser.tm_connected = CommunicationStatus.ESTABLISHED
            digitiser.last_update = datetime.now(timezone.utc)

    def process_dig_entity_disconnected(self, event, entity) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Digitiser entity on {event.remote_addr}\n{entity}")
        digitiser: DigitiserModel = entity if entity is not None and isinstance(entity, DigitiserModel) else None

        if digitiser is not None:
            digitiser.tm_connected = CommunicationStatus.NOT_ESTABLISHED
            digitiser.last_update = datetime.now(timezone.utc)

            # Abort all ongoing observations that are using this digitiser
            return self.abort_all_observations(dig_id=digitiser.dig_id)

    def process_dig_entity_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray, entity: BaseModel) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received digitiser {api_call['msg_type']} msg with action code: {api_call['action_code']} on entity: {api_msg['entity']}")

        dt = api_msg.get("timestamp")
        digitiser: DigitiserModel = entity

        # If the Digitiser entity could not be identified, raise an exception
        if digitiser is None:
            raise XUnknownEntity(f"Telescope Manager received Digitiser API message for unknown (None) digitiser.\n{api_call}")

        action = Action()

        # If the api call indicates that an error occured
        if api_call.get('status','') == tm_dig.STATUS_ERROR:
            
            logger.error(f"Telescope Manager received error response from Digitiser {digitiser.dig_id}.\n{api_call}")
            digitiser.last_err_msg = api_call['message'] if 'message' in api_call else digitiser.last_err_msg
            digitiser.last_err_dt = datetime.fromisoformat(dt) if dt is not None else datetime.now(timezone.utc)

        # If the api call does not indicate that an error occured
        elif api_call.get('status','') != tm_dig.STATUS_ERROR:

            # If the api call is a status update message, update the Digitiser model
            if api_call.get('property','') == tm_dig.PROPERTY_STATUS:
                logger.debug(f"Telescope Manager received Digitiser STATUS update: {api_call['value']}")
                digitiser.update_from_model(DigitiserModel.from_dict(api_call['value']))

            elif api_call.get('property','') == tm_dig.PROPERTY_SDP_COMMS:
                digitiser.sdp_connected = CommunicationStatus(api_call['value'])

            elif api_call.get('property','') in digitiser.schema.schema:
                try:
                    setattr(digitiser, api_call.get('property',''), api_call['value'])
                except XSoftwareFailure as e:
                    logger.error(f"Telescope Manager error setting attribute {api_call.get('property','')} on Digitiser: {e}")
                    return action
            else:
                logger.warning(f"Telescope Manager received unknown Digitiser property update: {api_call['property']}")
                return action

        # Update Telescope Model timestamps based on received Digitiser api_call
        self.telmodel.dig_store.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)
        digitiser.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message
        if api_call['msg_type'] == tm_dig.MSG_TYPE_RSP:
            if dt is not None:
                # Stop the corresponding retry timers
                action.set_timer_action(Action.Timer(name=f"{digitiser.dig_id}_req_timer_retry:{dt}", timer_action=Action.Timer.TIMER_STOP))
                action.set_timer_action(Action.Timer(name=f"{digitiser.dig_id}_req_timer_final:{dt}", timer_action=Action.Timer.TIMER_STOP))

            # If the status update message contains additional observation data, extract the related observation 
            obs_data = api_call.get('obs_data', None)
            obs_id = obs_data.get('obs_id', None) if obs_data is not None and isinstance(obs_data, dict) else None
            obs = self.telmodel.oda.obs_store.get_obs_by_id(obs_id) if obs_id is not None else None

            # If the observation was identified and is in CONFIGURING state, trigger a review of the configuration updates
            if obs is not None and obs.obs_state == ObsState.CONFIGURING:

                config_mismatched = False

                # Check for remaining mismatches between desired and current configuration properties
                for config_key, new_value in obs_data.items():
                    if config_key in digitiser.schema.schema:
                        current_value = getattr(digitiser, config_key, None)
                        if current_value != new_value:
                            config_mismatched = True
                            break

                # If no mismatches remain, the configuration update has been applied successfully
                if not config_mismatched:
                    logger.info(f"Telescope Manager configuration update for observation {obs_id} has been applied successfully by Digitiser {digitiser.dig_id}.")
                    action.set_obs_transition(obs=obs, transition=ObsTransition.CONFIGURE_RESOURCES)
                        
        return action

    def process_sdp_connected(self, event) -> Action:
        """ Processes Science Data Processor connected events.
        """
        logger.info(f"Telescope Manager connected to Science Data Processor: {event.remote_addr}")

        self.telmodel.sdp.tm_connected = CommunicationStatus.ESTABLISHED
        self.telmodel.tel_mgr.sdp_connected = CommunicationStatus.ESTABLISHED

    def process_sdp_disconnected(self, event) -> Action:
        """ Processes Science Data Processor disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Science Data Processor: {event.remote_addr}")

        self.telmodel.sdp.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.sdp_connected = CommunicationStatus.NOT_ESTABLISHED

        return self.abort_all_observations()

    def process_sdp_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Science Data Processor service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received Science Data Processor {api_call['msg_type']} message with action code: {api_call['action_code']}")
        action = Action()

        # If the api call indicates that an error occured
        if api_call.get('status','') == tm_sdp.STATUS_ERROR: 
            logger.error(self.set_last_err(f"Telescope Manager received error response from Science Data Processor.\n{api_call}"))
        
        elif api_call.get('status','') != tm_sdp.STATUS_ERROR:

            # If a status update is received, update the Science Data Processor Model 
            if api_call.get('property','') == tm_sdp.PROPERTY_STATUS:
                self.telmodel.sdp = ScienceDataProcessorModel.from_dict(api_call['value'])

            elif api_call.get('property','') == tm_sdp.PROPERTY_SCAN_CONFIG:
                logger.info(f"Telescope Manager received Science Data Processor SCAN_CONFIG update: {api_call['value']}")

                dig_config = api_call['value']

                # Copy key value pairs from scan config rsp msg into SDP model digitiser store configuration for the related digitiser
                dig_id = dig_config.get('dig_id', None) if dig_config is not None and isinstance(dig_config, dict) else None
                dig_model = self.telmodel.sdp.dig_store.get_dig_by_id(dig_id) if dig_id is not None else None

                if dig_model is not None:
                    for key in dig_config.keys():
                        if key in dig_model.schema.schema.keys():
                            setattr(dig_model, key, dig_config[key])
                else:
                    logger.warning(f"Telescope Manager received Science Data Processor SCAN_CONFIG rsp for unknown digitiser {dig_id}\n{api_call}")

            elif api_call.get('property','') == tm_sdp.PROPERTY_OBS_RESET:
                logger.info(f"Telescope Manager received Science Data Processor OBS_RESET response: {api_call['value']}")

            elif api_call.get('property','') == tm_sdp.PROPERTY_OBS_COMPLETE:
                logger.info(f"Telescope Manager received Science Data Processor OBS_COMPLETE response: {api_call['value']}")

            # Else if a scan complete advice is received, process it 
            elif api_call.get('property','') == tm_sdp.PROPERTY_SCAN_COMPLETE:
                logger.info(f"Telescope Manager received Science Data Processor SCAN_COMPLETE update: {api_call['value']}")
                # Copy all key value pairs from the api_call scan complete msg into the Science Data Processor model
                completed_scan = ScanModel.from_dict(api_call['value'])

                obs_id = completed_scan.obs_id
                scan_id = completed_scan.scan_id

                obs=self.telmodel.oda.obs_store.get_obs_by_id(obs_id) if obs_id is not None else None
                scan = obs.get_target_scan_by_id(scan_id) if obs is not None else None

                # If we identified the observation that the scan belongs to, transition its workflow accordingly
                if obs is not None:
                    action.set_obs_transition(obs=obs, transition=ObsTransition.SCAN_COMPLETED)
                    
                    # If we identified the scan within the observation, update its metadata
                    if scan is not None:
                        scan.update_from_model(completed_scan)

                        filename = util.gen_file_prefix(
                            dt=completed_scan.read_start,
                            entity_id=completed_scan.dig_id,
                            gain=completed_scan.gain,
                            duration=completed_scan.duration,
                            sample_rate=completed_scan.sample_rate,
                            center_freq=completed_scan.center_freq,
                            channels=completed_scan.channels,
                            instance_id=scan.scan_id, 
                            filetype="meta") + ".json"

                        scan.save_to_disk(output_dir=self.telmodel.get_scan_store_dir(), filename=filename)

                    status, message = tm_sdp.STATUS_SUCCESS, f"Telescope Manager processed SCAN_COMPLETE for observation {obs_id} scan {scan_id}"
                    logger.info(message)

                else:
                    status, message = tm_sdp.STATUS_ERROR, f"Telescope Manager received SCAN_COMPLETE for unknown or non-scanning observation {obs_id} scan {scan_id}"
                    logger.warning(message)

                sdp_rsp = self._construct_rsp_to_sdp(status, message, api_msg, api_call)
                action.set_msg_to_remote(sdp_rsp)

            # Else update an individual property if it exists in the Science Data Processor model
            elif api_call.get('property','') in self.telmodel.sdp.schema.schema:
                try:
                    setattr(self.telmodel.sdp, api_call.get('property',''), api_call['value'])
                except XSoftwareFailure as e:
                    logger.error(self.set_last_err(f"Telescope Manager error setting attribute {api_call.get('property','')} on Science Data Processor: {e}"))
                    return action
            else:
                logger.warning(f"Telescope Manager received unknown Science Data Processor property update:\n{api_call}")
                return action

        # Update Telescope Model timestamps based on received Science Data Processor api_call
        dt = api_msg.get("timestamp")
        self.telmodel.sdp.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message
        if api_call['msg_type'] == tm_sdp.MSG_TYPE_RSP:
            if dt is not None:
                # Stop the corresponding retry timers
                action.set_timer_action(Action.Timer(name=f"{self.telmodel.sdp.sdp_id}_req_timer_retry:{dt}", timer_action=Action.Timer.TIMER_STOP))
                action.set_timer_action(Action.Timer(name=f"{self.telmodel.sdp.sdp_id}_req_timer_final:{dt}", timer_action=Action.Timer.TIMER_STOP))
            
            # If the status update message contains additional observation data, extract the related observation 
            obs_data = api_call.get('obs_data', None)
            obs_id = obs_data.get('obs_id', None) if obs_data is not None and isinstance(obs_data, dict) else None
            obs = self.telmodel.oda.obs_store.get_obs_by_id(obs_id) if obs_id is not None else None

            # If the observation was identified and is in CONFIGURING state, trigger a review of the configuration updates
            if obs is not None and obs.obs_state == ObsState.CONFIGURING:

                config_mismatched = False

                # Check for remaining mismatches between desired and current configuration properties
                for config_key, new_value in obs_data.items():
                    if config_key in self.telmodel.sdp.schema.schema:
                        current_value = getattr(self.telmodel.sdp, config_key, None)
                        if current_value != new_value:
                            config_mismatched = True
                            break

                # If no mismatches remain, the configuration update has been applied successfully
                if not config_mismatched:
                    logger.info(f"Telescope Manager configuration update for observation {obs_id} has been applied successfully by Science Data Processor.")
                    action.set_obs_transition(obs=obs, transition=ObsTransition.CONFIGURE_RESOURCES)
        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"Telescope Manager timer event: {event}")

        action = Action()

        # Handle an initial request msg timer retry e.g. dig001_req_timer_retry:<timestamp> or sdp001_req_timer_retry:<timestamp>
        if "req_timer_retry" in event.name:
            
            logger.warning(f"Telescope Manager timed out waiting for response msg {event.name}, retrying request msg")

            # Resend the API request if the timer user_ref is set (containing the original request message)
            if event.user_ref is not None:

                req_msg: APIMessage = event.user_ref
                final_timer = re.sub(r':.*$', f':{req_msg.get_timestamp()}', event.name.replace("retry", "final"))

                action.set_msg_to_remote(req_msg)
                action.set_timer_action(Action.Timer(
                    name=final_timer, 
                    timer_action=self.telmodel.tel_mgr.app.msg_timeout_ms,
                    echo_data=req_msg))

        # Handle a final request msg timer e.g. dig002_req_timer_final:<timestamp> or sdp002_req_timer_final:<timestamp>
        elif "req_timer_final" in event.name:
            
            logger.warning(f"Telescope Manager timed out waiting for response msg after final retry, aborting retries.\n{event}")

            if event.user_ref is not None:

                req_msg: APIMessage = event.user_ref
                echo = req_msg.get_echo_data()

                if echo is not None and isinstance(echo, dict):
                    new_config = echo["echo_data"] if "echo_data" in echo else echo
                    obs_id = new_config["obs_id"] if new_config is not None and "obs_id" in new_config else None

                    obs=self.telmodel.oda.obs_store.get_obs_by_id(obs_id)

                    # If the observation is still in CONFIGURING state, ABORT the observation
                    if obs is not None and obs.obs_state == ObsState.CONFIGURING:
                        action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)

        # Handle observation start timer event
        elif event.name.startswith("obs_start_timer"):
            logger.info(f"Telescope Manager observation timer event: {event}")

            now = datetime.now(timezone.utc)

            # Transition observations that are scheduled for the current scheduling block from ObsState = EMPTY to ObsState = IDLE
            # It is possible that multiple observations are scheduled for the current scheduling block and that some cannot be resourced
            # Example: A dish has become UNAVAILABLE, so only some observations can be resourced
            for obs in self.telmodel.oda.obs_store.obs_list:

                # Calculate difference between now and the observation scheduling block start time in seconds
                start_offset = abs((obs.scheduling_block_start - now).total_seconds())
  
                # Transition observations scheduled to start within 60 seconds
                if obs.obs_state == ObsState.EMPTY and start_offset <= 60:
                    action.set_obs_transition(obs=obs, transition=ObsTransition.START)
                    logger.info(f"Telescope Manager starting observation {obs.obs_id} scheduled to start at {obs.scheduling_block_start}")

            # Start timer to initiate the next scheduled observation if applicable
            self.oet.start_next_obs_timer(action)

        # Handle observation configuring timeout timer event
        elif event.name.startswith("obs_configuring_timer"):
            logger.info(f"Telescope Manager observation configuring timer event: {event}")

            obs: Observation = event.user_ref if isinstance(event.user_ref, Observation) else None

            if obs is not None and obs.obs_state == ObsState.CONFIGURING:
                logger.warning(f"Telescope Manager observation {obs.obs_id} configuration timeout occurred, aborting observation")
                action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)

        # Handle observation scanning timeout timer event
        elif event.name.startswith("obs_scanning_timer"):
            logger.info(f"Telescope Manager observation scanning timer event: {event}")

            obs: Observation = event.user_ref if isinstance(event.user_ref, Observation) else None

            if obs is not None and obs.obs_state == ObsState.SCANNING:
                logger.warning(f"Telescope Manager observation {obs.obs_id} scanning timeout occurred, ending scan")
                action.set_obs_transition(obs=obs, transition=ObsTransition.SCAN_ENDED)

        # Handle observation aborting timeout timer event
        elif event.name.startswith("obs_abort_timer"):
            logger.info(f"Telescope Manager observation abort timer fired: {event.name}")

            obs: Observation = event.user_ref if isinstance(event.user_ref, Observation) else None

            if obs is not None and obs.obs_state == ObsState.ABORTED:
                logger.warning(f"Telescope Manager observation {obs.obs_id} abort timeout occurred, releasing resources")
                action.set_obs_transition(obs=obs, transition=ObsTransition.RELEASE_RESOURCES)

        return action

    def update_sdp_configuration(self, old_config, new_config, action):
        """ Constructs and sends property set requests to the Science Data Processor.
            Only properties that changed values are sent.
            Parameters:
                old_config: dict of previous configuration values
                new_config: dict of desired configuration values
                action: Action object to append messages and timers to
            Returns updated Action object.
        """

        if self.telmodel.tel_mgr.sdp_connected != CommunicationStatus.ESTABLISHED:
            logger.warning(f"Telescope Manager cannot send Science Data Processor configuration update, not connected\n{new_config}")
            return action

        # Extract sdp_id from the incoming SDP configuration event (JSON)
        sdp_id = new_config.get("sdp_id", None)

        for config_key in new_config.keys():
            config_value = new_config[config_key]

            # If key value is unchanged, skip it
            if old_config and config_key in old_config and old_config[config_key] == config_value:
                continue

            logger.info(f"Science Data Processor configuration update for key: {config_key}, value: {config_value}")

            property = value = None
            (property, value) = map.get_property_name_value(config_key, config_value)

            if property is None:
                logger.warning(f"Telescope Manager ignoring science data processor configuration item: {config_key}")
                continue
        
            sdp_req = self._construct_req_to_sdp(property=property, value=value, message="")
            
            # Attach all new configuration to the request for tracking
            api_call = sdp_req.get_api_call()
            api_call['obs_data'] = new_config.copy() # Shallow copy

            action.set_msg_to_remote(sdp_req)
            action.set_timer_action(Action.Timer(
                name=f"{sdp_id}_req_timer_retry:{sdp_req.get_timestamp()}", 
                timer_action=self.telmodel.tel_mgr.app.msg_timeout_ms, 
                echo_data=sdp_req))
                
        return action

    def update_dig_configuration(self, old_config, new_config, action):
        """ Constructs and sends property set requests to the Digitiser.
            Only properties that changed values are sent.
            Parameters:
                old_config: dict of previous configuration values
                new_config: dict of desired configuration values
                action: Action object to append messages and timers to
            Returns updated Action object.
        """

        # Extract dig_id from the incoming DIG configuration event (JSON)
        dig_id = new_config.get("dig_id", None)
        digitiser = self.telmodel.dig_store.get_dig_by_id(dig_id) if dig_id is not None else None

        if digitiser is not None and digitiser.tm_connected != CommunicationStatus.ESTABLISHED:
            logger.warning(f"Telescope Manager cannot send Digitiser configuration update, not connected\n{new_config}")
            return action

        for config_key in new_config.keys():
            config_value = new_config[config_key]

            # If key value is unchanged, skip it
            if old_config and config_key in old_config and old_config[config_key] == config_value:
                continue

            logger.info(f"Digitiser configuration update for key: {config_key}, value: {config_value}")

            property = method = value = None

            (method, value) = map.get_method_name_value(config_key, config_value)
            (property, value) = map.get_property_name_value(config_key, config_value) if method is None else (None, config_value)

            if method is None and property is None:
                logger.warning(f"Telescope Manager ignoring digitiser configuration item: {config_key}")
                continue
        
            dig_req = self._construct_req_to_dig(entity=dig_id, property=property, method=method, value=value, message="")

            # Attach all new configuration to the request for tracking
            api_call = dig_req.get_api_call()
            api_call['obs_data'] = new_config.copy() # Shallow copy

            action.set_msg_to_remote(dig_req)
            action.set_timer_action(Action.Timer(
                name=f"{dig_id}_req_timer_retry:{dig_req.get_timestamp()}", 
                timer_action=self.telmodel.tel_mgr.app.msg_timeout_ms, 
                echo_data=dig_req))
                
        return action

    def update_dsh_configuration(self, old_config, new_config, action):
        """ Constructs and sends property set requests to the Dish Manager.
            Only properties that changed values are sent.
            Parameters:
                old_config: dict of previous configuration values
                new_config: dict of desired configuration values
                action: Action object to append messages and timers to
            Returns updated Action object.
        """

        if self.telmodel.tel_mgr.dm_connected != CommunicationStatus.ESTABLISHED:
            logger.warning(f"Telescope Manager cannot send Dish Manager configuration update, not connected\n{new_config}")
            return action

        # Extract dsh_id from the incoming DM configuration event (JSON)
        dsh_id = new_config.get("dsh_id", None)

        for config_key in new_config.keys():
            config_value = new_config[config_key]

            # If key value is unchanged, skip it
            if old_config and config_key in old_config and old_config[config_key] == config_value:
                continue

            logger.info(f"Telescope Manager configuration update for dish {dsh_id} key: {config_key}, value: {config_value}")

            property = value = None
            (property, value) = map.get_property_name_value(config_key, config_value)

            if property is None:
                logger.warning(f"Telescope Manager ignoring dish configuration item: {config_key}")
                continue
        
            dm_req = self._construct_req_to_dm(entity=dsh_id, property=property, value=value, message="")

            # Attach all new configuration to the request for tracking
            api_call = dm_req.get_api_call()
            api_call['obs_data'] = new_config.copy() # Shallow copy

            action.set_msg_to_remote(dm_req)
            action.set_timer_action(Action.Timer(
                name=f"{dsh_id}_req_timer_retry:{dm_req.get_timestamp()}", 
                timer_action=self.telmodel.tel_mgr.app.msg_timeout_ms, 
                echo_data=dm_req))
                
        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.telmodel.tel_mgr.sdp_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.telmodel.tel_mgr.dm_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif any(dig.tm_connected != CommunicationStatus.ESTABLISHED for dig in self.telmodel.dig_store.dig_list):
            return HealthState.DEGRADED
        else:
            return HealthState.OK

    def process_status_event(self, event) -> Action:
        """ Processes status update events. 
            Calls get_app_processor_state() to update the Telescope Model status.
            Reads the scan store directory to update the scan store file lists.
        """
        status = self.get_app_processor_state()

        scan_store_dir = self.telmodel.sdp.app.arguments.get('scan_store_dir','~/') if self.telmodel.sdp.app.arguments is not None else '~/'
        scan_store_dir = os.path.expanduser(scan_store_dir)

        if Path(scan_store_dir).exists():

            logger.info(f"Telescope Manager reading scan store directory: {scan_store_dir}")    

            # Read scan store directory listing
            spr_files = list(Path(scan_store_dir).glob("*spr.csv"))
            load_files = list(Path(scan_store_dir).glob("*load.csv"))
            tsys_files = list(Path(scan_store_dir).glob("*tsys.csv"))
            gain_files = list(Path(scan_store_dir).glob("*gain.csv"))
            meta_files = list(Path(scan_store_dir).glob("*meta.json"))

            # Sort by creation date in reverse order (newest first)
            spr_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            load_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            tsys_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            gain_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            meta_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)

            # Limit to the latest 10 files of each type
            spr_files = spr_files[:10]
            load_files = load_files[:10]
            tsys_files = tsys_files[:10]
            gain_files = gain_files[:10]
            meta_files = meta_files[:10]

            # Combine into a single list of scan files
            scan_files = spr_files + load_files + tsys_files + gain_files + meta_files

            self.telmodel.oda.scan_store.spr_files = []
            self.telmodel.oda.scan_store.load_files = []
            self.telmodel.oda.scan_store.tsys_files = []
            self.telmodel.oda.scan_store.gain_files = []
            self.telmodel.oda.scan_store.meta_files = []

            for scan_file in scan_files:
                if scan_file.name.endswith("spr.csv"):
                    self.telmodel.oda.scan_store.spr_files.append(scan_file.name)
                elif scan_file.name.endswith("load.csv"):
                    self.telmodel.oda.scan_store.load_files.append(scan_file.name)
                elif scan_file.name.endswith("tsys.csv"):
                    self.telmodel.oda.scan_store.tsys_files.append(scan_file.name)
                elif scan_file.name.endswith("gain.csv"):
                    self.telmodel.oda.scan_store.gain_files.append(scan_file.name)
                elif scan_file.name.endswith("meta.json"):
                    self.telmodel.oda.scan_store.meta_files.append(scan_file.name)

            self.telmodel.oda.scan_store.last_update = datetime.now(timezone.utc)
            self.telmodel.oda.last_update = datetime.now(timezone.utc)

        self.telmodel.tel_mgr.last_update = datetime.now(timezone.utc)

    def _construct_req_to_dig(self, entity=None, property=None, method=None, value=None, message=None) -> APIMessage:
        """ Constructs a request message to the Digitiser.
        """

        dig_req = APIMessage(api_version=self.dig_api.get_api_version())

        # If property is get_auto_gain or read_samples
        if method is not None:
            dig_req.set_json_api_header(
                api_version=self.dig_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_model.app_name, 
                to_system="dig", 
                entity=entity if entity else "<undefined>",
                api_call={
                    "msg_type": "req", 
                    "action_code": "method", 
                    "method": method, 
                    "params": value if value is not None else {}
            })
        elif property is not None:
            dig_req.set_json_api_header(
                api_version=self.dig_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_model.app_name, 
                to_system="dig", 
                entity=entity if entity else "<undefined>",
                api_call={
                    "msg_type": "req", 
                    "action_code": "set", 
                    "property": property, 
                    "value": value if value is not None else 0, 
                    "message": message if message else ""
            })

        return dig_req

    def _construct_req_to_sdp(self, property=None, value=None, message=None) -> APIMessage:
        """ Constructs a request message to the Science Data Processor.
        """

        sdp_req = APIMessage(api_version=self.sdp_api.get_api_version())

        if property is not None:
            sdp_req.set_json_api_header(
                api_version=self.sdp_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_model.app_name, 
                to_system="sdp", 
                api_call={
                    "msg_type": "req", 
                    "action_code": "set", 
                    "property": property, 
                    "value": value if value is not None else 0, 
                    "message": message if message else ""
            })

        return sdp_req

    def _construct_req_to_dm(self, entity=None, property=None, value=None, message=None) -> APIMessage:
        """ Constructs a request message to the Dish Manager.
        """

        dm_req = APIMessage(api_version=self.dm_api.get_api_version())
        if property is not None:
            dm_req.set_json_api_header(
                api_version=self.dm_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_model.app_name, 
                to_system="dm",
                entity=entity if entity else "<undefined>",
                api_call={
                    "msg_type": "req", 
                    "action_code": "set", 
                    "property": property, 
                    "value": value if value is not None else 0, 
                    "message": message if message else ""
            })

        return dm_req

    def _construct_rsp_to_sdp(self, status, message, api_msg: dict, api_call: dict) -> APIMessage:
        """ Constructs a response message to the Science Data Processor.
        """
        # Prepare rsp msg to sdp containing result of an api call
        sdp_rsp = APIMessage(api_msg=api_msg, api_version=self.sdp_api.get_api_version())
        sdp_rsp.switch_from_to()
        sdp_rsp_api_call = {
            "msg_type": "rsp", 
            "action_code": api_call['action_code'], 
            "status": status, 
        }
        if api_call.get('property') is not None:
            sdp_rsp_api_call["property"] = api_call['property']

        if api_call.get('value') is not None:
            sdp_rsp_api_call["value"] = api_call['value']

        if message is not None:
            sdp_rsp_api_call["message"] = message

        sdp_rsp.set_api_call(sdp_rsp_api_call)  
        return sdp_rsp

    def abort_all_observations(self, dig_id: str = None, dsh_id: str=None, action=None) -> Action:
        """ Aborts all ongoing observations matching the given digitiser ID or dish manager ID.
            If both IDs are None, aborts all ongoing observations.
            Returns an Action object containing the observation abort transition
        """
        action = Action() if action is None else action

        dish1 = self.telmodel.dsh_mgr.get_dish_by_id(dsh_id) if dsh_id is not None else None
        dish2 = self.telmodel.dsh_mgr.get_dish_by_dig_id(dig_id) if dig_id is not None else None

        ids = []
        if dish1 is not None:
            ids.append(dish1.dsh_id)
        if dish2 is not None and dish2.dsh_id not in ids:
            ids.append(dish2.dsh_id)

        for obs in self.telmodel.oda.obs_store.obs_list:
            if obs.obs_state in [ObsState.CONFIGURING, ObsState.READY, ObsState.SCANNING]:

                if obs.dsh_id in ids or (dig_id is None and dsh_id is None):
                    
                    dish = self.telmodel.dsh_mgr.get_dish_by_id(obs.dsh_id)
                    dig_id = dish.dig_id if dish is not None else None

                    logger.info(f"Telescope Manager aborting observation {obs.obs_id}.\nConnection status:\n" + \
                        f"- Dish Manager {obs.dsh_id}: {self.telmodel.dsh_mgr.tm_connected.name}\n" + \
                        f"- Digitiser {dig_id}: {self.telmodel.dig_store.get_dig_by_id(dig_id).tm_connected.name if dig_id is not None else 'N/A'}\n" + \
                        f"- Science Data Processor: {self.telmodel.sdp.tm_connected.name}")
                    action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)

        return action

# Retry decorator for handling transient network errors
def retry_on_timeout(max_retries=3, delay=5):
    """
    Decorator to retry a function call on timeout errors
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except TimeoutError as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Timeout error on attempt {attempt + 1}/{max_retries}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Failed after {max_retries} attempts due to timeout")
                        raise
                except socket.timeout as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Socket timeout on attempt {attempt + 1}/{max_retries}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Failed after {max_retries} attempts due to socket timeout")
                        raise
                except Exception as e:
                    # Don't retry on other types of errors
                    raise
            return None
        return wrapper
    return decorator

# Helper function to execute Google Sheets API requests with retry
@retry_on_timeout(max_retries=3, delay=5)
def execute_sheets_request(request):
    """
    Execute a Google Sheets API request with timeout handling
    """
    return request.execute()

def main():
  
    tm = TelescopeManager()
    tm.start()
    
    # Start webhook handler in background thread
    webhook_handler = WebhookHandler(event_queue=tm.get_queue(), host='127.0.0.1', port=5001)
    webhook_handler.start()
    logger.info("Webhook handler initialized and running on port 5001")

    """Uses the Google Sheets API to authenticate with Google """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        # Build service 
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    except HttpError as err:
        logger.error(f"HTTP Error: {err}")

    sheet = service.spreadsheets()

    # Initialize last config pull and model push datetimes
    last_odt_config_pull = last_tm_model_push = last_dig_model_push = last_sdp_model_push = last_dm_model_push = last_oda_model_push = datetime.min.replace(tzinfo=timezone.utc)
    last_odt_config_snapshot = None

    try:
        while True:

            # If we do not have an ODT config snapshot, pull ODT observation list from Google Sheets
            if last_odt_config_snapshot is None or (datetime.now(timezone.utc) - last_odt_config_pull).total_seconds() >= 30:

                try:
                    result = execute_sheets_request(sheet.values().get(
                        spreadsheetId=ALSTON_RADIO_TELESCOPE,
                        range=ODT_OBS_LIST
                    ))
                except Exception as err:
                    logger.error(f"Error retrieving ODT observation list from Google Sheets: {err}")
                    result = {"error": {'details': [{'errorMessage': str(err)}]}}

                if 'error' in result:
                    error = result['error']['details'][0]
                    error_msg = error.get('errorMessage', 'Unknown error')
                    logger.error(f'TM - error getting ODT configuration: {error_msg}')
                else:
                    values = result.get("values", [])

                    try:
                        json_config = json.loads(values[0][0])
                    except IndexError:
                        logger.error(f"TM - no data in sheet range {ODT_OBS_LIST}")
                        json_config = None
                    except json.JSONDecodeError as e:
                        logger.error(f"TM - invalid JSON in sheet row {values[0]}: {e}")
                        json_config = None
       
                    if json_config != last_odt_config_snapshot:

                        config = ConfigEvent(
                            category="ODT",
                            old_config=last_odt_config_snapshot,
                            new_config=json_config,
                            timestamp=datetime.now(timezone.utc)
                        )
                        tm.get_queue().put(config)

                        last_odt_config_snapshot = json_config

            # If comms to the Dish Manager is established then exchange Dish Manager model data with the UI 
            if tm.telmodel.dsh_mgr.tm_connected == CommunicationStatus.ESTABLISHED:

                dm_latest_update = tm.telmodel.dsh_mgr.last_update if tm.telmodel.dsh_mgr.last_update else datetime.now(timezone.utc)

                # Push updated Dish Manager model to Google Sheets if there are updates
                if dm_latest_update > last_dm_model_push:

                    dm_dict = tm.telmodel.dsh_mgr.to_dict()
                    dm_str = json.dumps(dm_dict, indent=4)
                    try:    
                        execute_sheets_request(sheet.values().update(
                            spreadsheetId=ALSTON_RADIO_TELESCOPE,
                            range=TM_UI_API + "F2",                      
                            valueInputOption="USER_ENTERED", # allow Sheets to parse as datetime
                            body={"values": [[dm_str]]}
                        ))
                    except Exception as err:
                        logger.error(f"Error updating Dish Manager model in Google Sheets: {err}")

                    last_dm_model_push = dm_latest_update

            else:
                # Reset the snapshot such that config is re-read upon reconnection
                last_dm_config_snapshot = None

            # If comms to the SDP is established then exchange SDP model data with the UI 
            if tm.telmodel.sdp.tm_connected == CommunicationStatus.ESTABLISHED:

                sdp_latest_update = tm.telmodel.sdp.last_update if tm.telmodel.sdp.last_update else datetime.now(timezone.utc)

                # Push updated SDP model to Google Sheets if there are updates
                if sdp_latest_update > last_sdp_model_push:

                    sdp_dict = tm.telmodel.sdp.to_dict()
                    sdp_str = json.dumps(sdp_dict, indent=4)

                    try:    
                        execute_sheets_request(sheet.values().update(
                            spreadsheetId=ALSTON_RADIO_TELESCOPE,
                            range=TM_UI_API + "C2",
                            valueInputOption="USER_ENTERED", # allow Sheets to parse as datetime
                            body={"values": [[sdp_str]]}
                        ))
                    except Exception as err:
                        logger.error(f"Error updating SDP model in Google Sheets: {err}")
                    
                    last_sdp_model_push = sdp_latest_update

            # Exchange DIG model data with the UI
            dig_latest_update = tm.telmodel.dig_store.last_update if tm.telmodel.dig_store.last_update else datetime.now(timezone.utc)

            if dig_latest_update > last_dig_model_push:

                dig_dict = tm.telmodel.dig_store.to_dict()
                dig_str = json.dumps(dig_dict, indent=4)
                try:    
                    execute_sheets_request(sheet.values().update(
                        spreadsheetId=ALSTON_RADIO_TELESCOPE,
                        range=TM_UI_API + "B2",                      
                        valueInputOption="USER_ENTERED", # allow Sheets to parse as datetime
                        body={"values": [[dig_str]]}
                    ))
                except Exception as err:
                    logger.error(f"Error updating Digitiser model in Google Sheets: {err}")

                last_dig_model_push = dig_latest_update

            # Exchange ODA model data with the UI
            oda_latest_update = tm.telmodel.oda.last_update if tm.telmodel.oda.last_update else datetime.now(timezone.utc)

            if oda_latest_update > last_oda_model_push:

                oda_dict = tm.telmodel.oda.to_dict()
                oda_str = json.dumps(oda_dict, indent=4)

                try:    
                    execute_sheets_request(sheet.values().update(
                        spreadsheetId=ALSTON_RADIO_TELESCOPE,
                        range=TM_UI_API + "E2",
                        valueInputOption="USER_ENTERED", # allow Sheets to parse as datetime
                        body={"values": [[oda_str]]}
                    ))
                except Exception as err:
                    logger.error(f"Error updating ODA models in Google Sheets: {err}")

                last_oda_model_push = oda_latest_update

            # Exchange TM model data with the UI

            tm_latest_update = tm.telmodel.tel_mgr.last_update if tm.telmodel.tel_mgr.last_update else datetime.now(timezone.utc)

            if tm_latest_update > last_tm_model_push:

                # Update TM model in Google Sheets
                tm_dict = tm.telmodel.tel_mgr.to_dict()
                tm_str = json.dumps(tm_dict, indent=4)

                try:
                    execute_sheets_request(sheet.values().update(
                        spreadsheetId=ALSTON_RADIO_TELESCOPE,
                        range=TM_UI_API + "A2",
                        valueInputOption="USER_ENTERED",  # allow Sheets to parse as datetime
                        body={"values": [[tm_str]]}
                    ))
                except Exception as err:
                    logger.error(f"Error updating TM model in Google Sheets: {err}")

                last_tm_model_push = tm_latest_update

            # TM to UI Poll interval
            time.sleep(TM_UI_UPDATE_INTERVAL_S) 
    except KeyboardInterrupt:
        pass
    finally:
        tm.stop()

if __name__ == "__main__":
    main()
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
from models.dsh import DishManagerModel, Feed, CapabilityState, DishMode, PointingState
from models.obs import Observation, ObsTransition, ObsState
from models.oda import ODAModel, ObsList, ScanStore
from models.health import HealthState
from models.sdp import ScienceDataProcessorModel
from models.telescope import TelescopeModel
from models.tm import ResourceType, AllocationState
from util import log, util
from util.timer import Timer, TimerManager
from util.xbase import XBase, XStreamUnableToExtract
from webhook_handler import WebhookHandler

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The SHEET ID for the ALSTON RADIO TELESCOPE google sheet
ALSTON_RADIO_TELESCOPE = "1r73N0VZHSQC6RjRv94gzY50pTgRvaQWfctGGOMZVpzc"

TM_UI_API = "TM_UI_API!"            # Range for UI-TM API data
TM_UI_UPDATE_INTERVAL_S = 10           # Update interval in seconds

ODT_OBS_LIST = TM_UI_API + "D2"     # Range for Observation Design Tool
DIG001_CONFIG = TM_UI_API + "B3"    # Range for Digitiser 001 configuration

OUTPUT_DIR = '/Users/r.brederode/samples'  # Directory to store observations

class TelescopeManager(App):

    telmodel = TelescopeModel()

    def __init__(self, app_name: str = "tm"):

        super().__init__(app_name=app_name, app_model=self.telmodel.tel_mgr.app)

        # Lock for thread-safe allocation of shared resources
        self._rlock = threading.RLock()  

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
        """ Processes initialisation events.
        """
        logger.debug(f"TM initialisation event")

        # Load Digitiser configuration from disk
        # Config file is located in ./config/<profile>/<model>.json
        # Config file defines initial list of digitisers to be processed by the TM
        input_dir = f"./config/{self.get_args().profile}"
        dig_store = self.telmodel.dig_store.load_from_disk(input_dir=input_dir, filename="DigitiserList.json")

        if dig_store is not None:
            self.telmodel.dig_store = dig_store
            logger.info(f"Telescope Manager loaded Digitiser configuration from {input_dir}")
        else:
            logger.warning(f"Telescope Manager could not load Digitiser configuration from {input_dir}")

        action = Action()
        return action

    def process_config(self, event: ConfigEvent) -> Action:
        """ Processes configuration update events.
        """
        logger.info(f"Telescope Manager received updated configuration: {event}")

        action = Action()

        if event.category.upper() == "DIG": # Digitiser Config Event

            action = self.update_dig_configuration(event.old_config, event.new_config, action)

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
                    odt_obs.scheduling_block_start = datetime.now(timezone.utc) + timedelta(seconds=10)
                    odt_obs.scheduling_block_end = odt_obs.scheduling_block_start + timedelta(seconds=610)
                    # END DEBUG CODE, REMOVE LATER

                    self.telmodel.oda.obs_store.obs_list.append(odt_obs)

            action = self.obs_start_next_timer(action)

        else:
            logger.info(f"Telescope Manager updated configuration received for {event.category}.")

        return action

    def process_obs_event(self, event: ObsEvent) -> Action:
        """ Processes a workflow transition on an observation.
            Returns an Action object with actions to be performed.
        """
        logger.info(f"Telescope Manager processing an Observation event: {event}")

        action = Action()

        # Handle observation event transitions
        if event.transition == ObsTransition.START:

            # Transition to assigning resources
            event.obs.obs_state = ObsState.IDLE

            # For each target config in the observation, determine the required resources
            for tgt_config in event.obs.target_configs:
                tgt_config.determine_scans()

            action.set_obs_transition(obs=event.obs, transition=ObsTransition.ASSIGN_RESOURCES)

        elif event.transition == ObsTransition.ASSIGN_RESOURCES:

            event.obs.obs_state = ObsState.IDLE
            
            # Grant resources for this observation if possible, otherwise request resources i.e. get in the queue
            # Resource availability will be checked each time this method is called, resources will only be requested once 
            # Returns true if all resources were granted, false if any resource had to be requested
            if self.obs_assign_resources(event.obs, action):
                action.set_obs_transition(obs=event.obs, transition=ObsTransition.CONFIGURE_RESOURCES)
            else:
                logger.info(f"Observation {event.obs.obs_id} blocked waiting for resources.")

        elif event.transition == ObsTransition.RELEASE_RESOURCES:

            event.obs.obs_state = ObsState.IDLE

            # Release resources for this observation
            # Returns true if at least one active resource was released, false otherwise
            if self.obs_release_resources(event.obs, action):

                now = datetime.now(timezone.utc)
                # Find observations with ObsState = IDLE that should be observing now
                waiting_obs = [obs for obs in self.telmodel.oda.obs_store.obs_list if obs.obs_state == ObsState.IDLE and obs.scheduling_block_start <= now and obs.scheduling_block_end > now]

                # Check if there are other observations waiting for the resources just released so that they can be assigned
                for obs in waiting_obs:
                    if obs.obs_id != event.obs.obs_id and obs.dish_id == event.obs.dish_id and obs.dig_id == event.obs.dig_id:
                        action.set_obs_transition(obs=obs, transition=ObsTransition.ASSIGN_RESOURCES)

        elif event.transition == ObsTransition.CONFIGURE_RESOURCES:

            event.obs.obs_state = ObsState.CONFIGURING

            # Determine outstanding configuration actions for this observation
            # Returns true if all resources are already configured, false if any resource still requires configuration
            if self.obs_configure_resources(event.obs, action):
                action.set_obs_transition(obs=event.obs, transition=ObsTransition.READY)
        
        elif event.transition == ObsTransition.READY:
            event.obs.obs_state = ObsState.READY

            if self.obs_start_scanning(event.obs, action):
                action.set_obs_transition(obs=event.obs, transition=ObsTransition.SCAN_STARTED)

        elif event.transition == ObsTransition.SCAN_STARTED:
            event.obs.obs_state = ObsState.SCANNING

        elif event.transition == ObsTransition.SCAN_COMPLETED:
            event.obs.obs_state = ObsState.READY

        elif event.transition == ObsTransition.SCAN_ENDED:
            event.obs.obs_state = ObsState.READY

        elif event.transition == ObsTransition.ABORT:
            event.obs.obs_state = ObsState.ABORTED

        elif event.transition == ObsTransition.FAULT_OCCURRED:
            event.obs.obs_state = ObsState.FAULT

        elif event.transition == ObsTransition.RESET:
            event.obs.obs_state = ObsState.IDLE

        else:
            logger.warning(f"Telescope Manager received unknown observation event transition: {event.transition}")
        
        return action

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

    def process_dm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Dish Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received dish manager {api_call['msg_type']} message with action code: {api_call['action_code']}")
        
        action = Action()

        if api_call.get('status','') != tm_dig.STATUS_ERROR:

            if api_call.get('property','') == tm_dm.PROPERTY_STATUS:
                logger.debug(f"Telescope Manager received Dish Manager STATUS update: {api_call['value']}")

                self.telmodel.dsh_mgr = DishManagerModel.from_dict(api_call['value'])

        # Update Telescope Model based on received Dish Manager api_call
        dt = api_msg.get("timestamp")
        self.telmodel.dsh_mgr.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)
        return action

    def get_dig_entity(self, event) -> (str, BaseModel):
        """ Determines the digitiser entity ID based on the remote address of a ConnectEvent, DisconnectEvent, or DataEvent.
            Returns a tuple of the entity ID and entity if found, else None, None.
        """
        logger.debug(f"Finding digitiser entity ID for remote address: {event.remote_addr[0]}")

        for digitiser in self.telmodel.dig_store.dig_list:

            if isinstance(digitiser.app.arguments, dict) and "local_host" in digitiser.app.arguments:

                if digitiser.app.arguments["local_host"] == event.remote_addr[0]:
                    logger.info(f"Found digitiser entity ID: {digitiser.dig_id} for remote address: {event.remote_addr}")
                    return digitiser.dig_id, digitiser
            else:
                logger.warning(f"Digitiser {digitiser.dig_id} is not configured with a valid local_host argument to match against remote address: {event.remote_addr[0]}")

        return None, None

    def process_dig_entity_connected(self, event, entity) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Telescope Manager connected to Digitiser entity on {event.remote_addr}\n{entity}")

    def process_dig_entity_disconnected(self, event, entity) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Digitiser entity on {event.remote_addr}\n{entity}")

    def process_dig_entity_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray, entity: BaseModel) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received digitiser {api_call['msg_type']} msg with action code: {api_call['action_code']} on entity: {api_msg['entity']}")

        digitiser: DigitiserModel = entity
        action = Action()

        # Extract datetime from API message
        dt = api_msg.get("timestamp")

        if api_call.get('status','') != tm_dig.STATUS_ERROR:

            if api_call.get('property','') == tm_dig.PROPERTY_STATUS:

                # Copy all key value pairs from the api_call status msg into the digitiser model
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
                action.set_timer_action(Action.Timer(name=f"dig_req_timer_retry:{dt}", timer_action=Action.Timer.TIMER_STOP))
                action.set_timer_action(Action.Timer(name=f"dig_req_timer_final:{dt}", timer_action=Action.Timer.TIMER_STOP))
            
            # Check if the response was due to an observation configuration update event (present in echo data)
            echo = api_msg.get("echo_data")

            if echo is not None and isinstance(echo, dict):
                new_config = echo["echo_data"] if "echo_data" in echo else echo

                obs_id = new_config["obs_id"] if "obs_id" in new_config else None
                if obs_id is not None:

                    config_mismatched = False

                    # Check for remaining mismatches between desired and current configuration properties
                    for config_key, new_value in new_config.items():
                        if config_key in digitiser.schema.schema:
                            current_value = getattr(digitiser, config_key, None)
                            if current_value != new_value:
                                config_mismatched = True
                                break

                    # If no mismatches remain, the configuration update has been applied successfully
                    if not config_mismatched:
                        logger.info(f"Telescope Manager digitiser configuration update for observation {obs_id} has been applied successfully.")
                        obs=self.telmodel.oda.obs_store.get_obs_by_id(obs_id)

                        # If the observation is still in CONFIGURING state, trigger the workflow to attempt to move to READY
                        if obs is not None and obs.obs_state == ObsState.CONFIGURING:
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

    def process_sdp_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Science Data Processor service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received Science Data Processor {api_call['msg_type']} message with action code: {api_call['action_code']}")
        action = Action()

        if api_call.get('status','') != tm_sdp.STATUS_ERROR:

            # If a status update is received, update the Science Data Processor Model 
            if api_call.get('property','') == tm_sdp.PROPERTY_STATUS:
                self.telmodel.sdp = ScienceDataProcessorModel.from_dict(api_call['value'])
            # Else update an individual property if it exists in the Science Data Processor model
            elif api_call.get('property','') in self.telmodel.sdp.schema.schema:
                try:
                    setattr(self.telmodel.sdp, api_call.get('property',''), api_call['value'])
                except XSoftwareFailure as e:
                    logger.error(f"Telescope Manager error setting attribute {api_call.get('property','')} on Science Data Processor: {e}")
                    return action
            else:
                logger.warning(f"Telescope Manager received unknown Science Data Processor property update: {api_call['property']}")
                return action

        # Update Telescope Model timestamps based on received Science Data Processor api_call
        dt = api_msg.get("timestamp")
        self.telmodel.sdp.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message
        if api_call['msg_type'] == tm_sdp.MSG_TYPE_RSP:
            if dt is not None:
                # Stop the corresponding retry timers
                action.set_timer_action(Action.Timer(name=f"sdp_req_timer_retry:{dt}", timer_action=Action.Timer.TIMER_STOP))
                action.set_timer_action(Action.Timer(name=f"sdp_req_timer_final:{dt}", timer_action=Action.Timer.TIMER_STOP))
            
            # Check if the response was due to an observation configuration update event (present in echo data)
            echo = api_msg.get("echo_data")

            if echo is not None and isinstance(echo, dict):
                new_config = echo["echo_data"] if "echo_data" in echo else echo

                obs_id = new_config["obs_id"] if "obs_id" in new_config else None
                if obs_id is not None:

                    config_mismatched = False

                    # Check for remaining mismatches between desired and current configuration properties
                    for config_key, new_value in new_config.items():
                        if config_key in self.telmodel.sdp.schema.schema:
                            current_value = getattr(self.telmodel.sdp, config_key, None)
                            if current_value != new_value:
                                config_mismatched = True
                                break

                    # If no mismatches remain, the configuration update has been applied successfully
                    if not config_mismatched:
                        logger.info(f"Telescope Manager science data processor configuration update for observation {obs_id} has been applied successfully.")
                        obs=self.telmodel.oda.obs_store.get_obs_by_id(obs_id)

                        # If the observation is still in CONFIGURING state, trigger the workflow to attempt to move to READY
                        if obs is not None and obs.obs_state == ObsState.CONFIGURING:
                            action.set_obs_transition(obs=obs, transition=ObsTransition.CONFIGURE_RESOURCES)
        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"Telescope Manager timer event: {event}")

        action = Action()

        # Handle an initial request msg timer retry e.g. dig_req_timer_retry:<timestamp> or sdp_req_timer_retry:<timestamp>
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

        # Handle a final request msg timer e.g. dig_req_timer_final:<timestamp> or sdp_req_timer_final:<timestamp>
        elif "req_timer_final" in event.name:
            
            logger.warning(f"Telescope Manager timed out waiting for response msg after final retry, aborting retries for {event.name}")

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

            # Start a timer to trigger on the next observation start time
            action = self.obs_start_next_timer(action)

        return action

    def obs_start_next_timer(self, action) -> Action:

        # Find observations with ObsState = EMPTY that are scheduled to start in the future
        empty_obs = [obs for obs in self.telmodel.oda.obs_store.obs_list if obs.obs_state == ObsState.EMPTY and obs.scheduling_block_start >= datetime.now(timezone.utc)]
        next_obs = min(empty_obs, key=lambda obs: obs.scheduling_block_start) if len(empty_obs) > 0 else None
        
        if next_obs is not None:
            # Observation start time is in the future, reset timer
            time_until_start_ms = int((next_obs.scheduling_block_start - datetime.now(timezone.utc)).total_seconds() * 1000)
            
            action.set_timer_action(Action.Timer(
                name=f"obs_start_timer", 
                timer_action=time_until_start_ms,
                echo_data=next_obs))
            logger.info(f"Telescope Manager next observation {next_obs.obs_id} starting at {next_obs.scheduling_block_start} in {time_until_start_ms} ms")

        return action

    def obs_assign_resources(self, obs: Observation, action: Action) -> bool:
        """ Process an observation resource allocation request.
            Grants an allocation request if the resource is available.
            Requests an allocation if the resource is busy.
            Will not create new allocation request if an existing request is pending.
            Returns True if resources were successfully granted, False otherwise.
        """
        # Lookup the dish using the observation's dish_id
        dish = next((dsh for dsh in self.telmodel.dsh_mgr.dish_store.dish_list if dsh.dsh_id == obs.dish_id), None)

        if dish is None:
            logger.error(
                f"Telescope Manager could not find Dish {obs.dish_id} in Dish Manager model. "
                f"Cannot assign dish for observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        elif dish.capability_state not in [CapabilityState.OPERATE_FULL, CapabilityState.OPERATE_DEGRADED]:
            logger.error(
                f"Telescope Manager found Dish {obs.dish_id}, but it is not currently operational. Capability state {dish.capability_state.name}. "
                f"Cannot assign dish for observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        elif dish.mode not in [DishMode.STANDBY_LP, DishMode.STANDBY_FP, DishMode.OPERATE, DishMode.CONFIG]:
            logger.error(
                f"Telescope Manager found Dish {obs.dish_id}, but it is not in an operational mode. Current mode {dish.mode.name}. "
                f"Cannot assign dish for observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        if self.telmodel.dsh_mgr.tm_connected != CommunicationStatus.ESTABLISHED:
            logger.error(
                f"Telescope Manager is not connected to Dish Manager. "
                f"Cannot assign dish for observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False
        elif self.telmodel.dsh_mgr.app.health not in [HealthState.OK, HealthState.DEGRADED]:
            logger.error(
                f"Telescope Manager found Dish Manager, but it is not currently healthy. Health state {self.telmodel.dsh_mgr.app.health.name}. "
                f"Cannot assign resources to observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        # Lookup the digitiser using the dig_id associated with the dish
        digitiser = next((dig for dig in self.telmodel.dig_store.dig_list if dig.dig_id == dish.dig_id), None)

        if digitiser is None:
            logger.error(
                f"Telescope Manager found Dish {obs.dish_id}, but it is not associated with a Digitiser. "
                f"Cannot assign digitiser to observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        elif digitiser.app.health not in [HealthState.OK, HealthState.DEGRADED]:
            logger.error(
                f"Telescope Manager found Digitiser {digitiser.dig_id}, but it is not currently healthy. Health state {digitiser.app.health.name}. "
                f"Cannot assign digitiser to observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        sdp = self.telmodel.sdp
        if self.telmodel.sdp.tm_connected != CommunicationStatus.ESTABLISHED:
            logger.error(
                f"Telescope Manager is not connected to Science Data Processor. "
                f"Cannot assign resources to observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False
        elif sdp.app.health not in [HealthState.OK, HealthState.DEGRADED]:
            logger.error(
                f"Telescope Manager found Science Data Processor, but it is not currently healthy. Health state {sdp.app.health.name}. "
                f"Cannot assign resources to observation {obs.obs_id}. Aborting observation.")
            action.set_obs_transition(obs=obs, transition=ObsTransition.ABORT)
            return False

        with self._rlock:

            granted_all_resources = True    # Flag indicating if all resources were granted
        
            # Request new resource allocation for dish resources i.e. get in the queue
            dish_req = self.telmodel.tel_mgr.allocations.request_allocation(
                resource_type=ResourceType.DISH.value, 
                resource_id=dish.dsh_id, 
                allocated_type=ResourceType.OBS.value, 
                allocated_id=obs.obs_id,
                expires=obs.scheduling_block_end)

            # Get current active allocation for dish resources 
            dish_alloc = self.telmodel.tel_mgr.allocations.get_active_allocation(
                resource_type=ResourceType.DISH.value, 
                resource_id=dish.dsh_id)

            if not self.telmodel.tel_mgr.allocations.handle_resource_allocation(
                resource_type=ResourceType.DISH.value,
                resource_id=dish.dsh_id,
                resource_req=dish_req,
                resource_alloc=dish_alloc
            ):
                granted_all_resources = False

            # Request new resource allocation for digitiser resources i.e. get in the queue
            dig_req = self.telmodel.tel_mgr.allocations.request_allocation(
                resource_type=ResourceType.DIGITISER.value, 
                resource_id=digitiser.dig_id, 
                allocated_type=ResourceType.OBS.value, 
                allocated_id=obs.obs_id,
                expires=obs.scheduling_block_end)

            # Get current active allocation for digitiser resources 
            dig_alloc = self.telmodel.tel_mgr.allocations.get_active_allocation(
                resource_type=ResourceType.DIGITISER.value, 
                resource_id=digitiser.dig_id)

            if not self.telmodel.tel_mgr.allocations.handle_resource_allocation(
                resource_type=ResourceType.DIGITISER.value,
                resource_id=digitiser.dig_id,
                resource_req=dig_req,
                resource_alloc=dig_alloc
            ):
                granted_all_resources = False

            return granted_all_resources

    def obs_release_resources(self, obs: Observation, action: Action) -> bool:
        """ Process an observation resource release request.
            Returns true if at least one active resource was released, false otherwise.
        """
        released_active_resources = False
        
        # Find resource allocations for this observation
        obs_allocs = self.telmodel.tel_mgr.allocations.get_allocations(allocated_type=ResourceType.OBS.value, allocated_id=obs.obs_id)
        
        # Release each allocation
        for alloc in obs_allocs:

            if alloc.state == AllocationState.ACTIVE:
                released_active_resources = True

            logger.info(
                f"Telescope Manager releasing resource {alloc.resource_type} {alloc.resource_id} "
                f"allocated to {alloc.allocated_type} {alloc.allocated_id} in state {alloc.state.name} "
                f"with expiry {alloc.expires}")

            self.telmodel.tel_mgr.allocations.release_allocation(alloc)
            
        return released_active_resources

    def obs_configure_resources(self, obs: Observation,  action: Action) -> bool:
        """ Process an observation resource configuration request.
            Returns true if all resources are already configured, false if any resource still requires configuration.
        """
        logger.info(f"Telescope Manager processing Configure Resources for observation {obs.obs_id} scheduled to start at {obs.scheduling_block_start}")

        already_configured = True

        # Lookup the next target config for the observation
        target_config = next((tgt_cfg for tgt_cfg in obs.target_configs if tgt_cfg.index == obs.next_tgt_index), None)

        if target_config is None:
            logger.error(f"Telescope Manager could not find next target config {obs.next_tgt_index} to execute for observation {obs.obs_id}. " + \
                f"Nothing to configure.")
            return already_configured

        # Lookup the next scan in the target config, using the observation's next_tgt_scan index
        freq_scan = obs.next_tgt_scan // target_config.scan_iterations
        scan_iter = obs.next_tgt_scan % target_config.scan_iterations

        scan_id = f"{freq_scan:03d}-{scan_iter:03d}"

        target_scan = next((scan for scan in target_config.scans if scan.scan_id == scan_id), None)

        if target_scan is None:
            logger.error(f"Telescope Manager could not find next target scan with index {obs.next_tgt_scan} " + \
                f"(freq_scan={freq_scan}, scan_iter={scan_iter}) to execute for observation {obs.obs_id}. " + \
                f"Nothing to configure.")
            return already_configured

        # Lookup the digitiser and dish model for this observation
        dish = next((dsh for dsh in self.telmodel.dsh_mgr.dish_store.dish_list if dsh.dsh_id == obs.dish_id), None)

        if dish is not None:
             # TBD: Add dish configuration logic here
            pass

        digitiser = next((dig for dig in self.telmodel.dig_store.dig_list if dig.dig_id == dish.dig_id), None)

        # If we found a valid digitiser, check if it needs to be configured
        if digitiser is not None:

            old_dig_config = {}
            new_dig_config = {}

            # Check if target config parameters on the digitiser need to be adjusted
            if digitiser.center_freq != target_scan.center_freq:
                old_dig_config['center_freq'] = digitiser.center_freq
                new_dig_config['center_freq'] = target_scan.center_freq
            if digitiser.bandwidth != target_config.bandwidth:
                old_dig_config['bandwidth'] = digitiser.bandwidth
                new_dig_config['bandwidth'] = target_config.bandwidth
            if digitiser.sample_rate != target_config.sample_rate:
                old_dig_config['sample_rate'] = digitiser.sample_rate
                new_dig_config['sample_rate'] = target_config.sample_rate
            if digitiser.gain != target_config.gain:
                old_dig_config['gain'] = digitiser.gain
                new_dig_config['gain'] = target_config.gain

            if len(new_dig_config) > 0:

                already_configured = False

                old_dig_config['dig_id'] = digitiser.dig_id
                new_dig_config['dig_id'] = digitiser.dig_id
                new_dig_config['obs_id'] = obs.obs_id

                # Send configuration requests to the Digitiser if we are not already waiting for previous requests to complete
                if not any(timer.active for timer in Timer.manager.get_timers_by_keyword("dig_req_timer")):
                    logger.info(f"Telescope Manager sending Digitiser configuration requests for observation {obs.obs_id} target config index {obs.next_tgt_index}")
                    action = self.update_dig_configuration(old_dig_config, new_dig_config, action)
            else:
                logger.info(f"Telescope Manager found Digitiser already configured for observation {obs.obs_id} target config index {obs.next_tgt_index}")
  
        sdp = self.telmodel.sdp
        if sdp is not None:

            old_sdp_config = {}
            new_sdp_config = {}

            if sdp.channels != target_config.spectral_resolution:
                old_sdp_config['channels'] = sdp.channels
                new_sdp_config['channels'] = target_config.spectral_resolution
            if sdp.scan_duration != target_config.scan_duration:
                old_sdp_config['scan_duration'] = sdp.scan_duration
                new_sdp_config['scan_duration'] = target_config.scan_duration

            if len(new_sdp_config) > 0:

                already_configured = False

                old_sdp_config['sdp_id'] = sdp.sdp_id
                new_sdp_config['sdp_id'] = sdp.sdp_id
                new_sdp_config['obs_id'] = obs.obs_id

                # Send configuration requests to the Science Data Processor if we are not already waiting for previous requests to complete
                if not any(timer.active for timer in Timer.manager.get_timers_by_keyword("sdp_req_timer")):
                    logger.info(f"Telescope Manager sending Science Data Processor configuration requests for observation {obs.obs_id} target config index {obs.next_tgt_index}")
                    action = self.update_sdp_configuration(old_sdp_config, new_sdp_config, action)
            else:
                logger.info(f"Telescope Manager found Science Data Processor already configured for observation {obs.obs_id} target config index {obs.next_tgt_index}")

        if dish is None or digitiser is None or sdp is None:
            raise XSoftwareFailure(f"Telescope Manager could not configure missing critical resource for observation {obs.obs_id}. " + \
                f"Dish found: {dish is not None}, Digitiser found: {digitiser is not None}, Science Data Processor found: {sdp is not None}.")

        return already_configured

    def update_sdp_configuration(self, old_config, new_config, action):
        """ Constructs and sends property set requests to the Science Data Processor.
            Only properties that changed values are sent.
            Parameters:
                old_config: dict of previous configuration values
                new_config: dict of desired configuration values
                action: Action object to append messages and timers to
            Returns updated Action object.
        """

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

            logger.info(f"Sending science data processor configuration update for property: {property}, value: {value}")
        
            sdp_req = self._construct_req_to_sdp(property=property, value=value, message="")
            sdp_req.set_echo_data(new_config)
            action.set_msg_to_remote(sdp_req)

            action.set_timer_action(Action.Timer(
                name=f"sdp_req_timer_retry:{sdp_req.get_timestamp()}", 
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

            logger.info(f"Sending digitiser configuration update for method: {method}, property: {property}, value: {value}")
        
            dig_req = self._construct_req_to_dig(entity=dig_id, property=property, method=method, value=value, message="")
            dig_req.set_echo_data(new_config)
            action.set_msg_to_remote(dig_req)

            action.set_timer_action(Action.Timer(
                name=f"dig_req_timer_retry:{dig_req.get_timestamp()}", 
                timer_action=self.telmodel.tel_mgr.app.msg_timeout_ms, 
                echo_data=dig_req))
                
        return action

    def obs_start_scanning(self, obs: Observation, action: Action) -> bool:
        """ Process an observation start scanning request.
            Returns true if scanning started successfully, false otherwise.
        """
        logger.info(f"Telescope Manager processing Start Scanning for observation {obs.obs_id} scheduled to start at {obs.scheduling_block_start}")

        # Lookup the digitiser and dish model for this observation
        dish = next((dsh for dsh in self.telmodel.dsh_mgr.dish_store.dish_list if dsh.dsh_id == obs.dish_id), None)

        if dish is None:
            logger.error(f"Telescope Manager could not find Dish {obs.dish_id} in Dish Manager model. Cannot start scanning on dish for observation {obs.obs_id}.")
            return False

        digitiser = next((dig for dig in self.telmodel.dig_store.dig_list if dig.dig_id == dish.dig_id), None)

        # If we found a valid digitiser, check if it needs to be configured
        if digitiser is not None:

            old_dig_config = {}
            new_dig_config = {}

            # Check if target config parameters on the digitiser need to be adjusted
            if digitiser.streaming != True:
                old_dig_config['streaming'] = digitiser.streaming
                new_dig_config['streaming'] = True

                old_dig_config['dig_id'] = digitiser.dig_id
                new_dig_config['dig_id'] = digitiser.dig_id
                new_dig_config['obs_id'] = obs.obs_id

                # Send configuration requests to the Digitiser if we are not already waiting for previous requests to complete
                if not any(timer.active for timer in Timer.manager.get_timers_by_keyword("dig_req_timer")):
                    logger.info(f"Telescope Manager sending Digitiser start scanning request for observation {obs.obs_id} target config index {obs.next_tgt_index}")
                    action = self.update_dig_configuration(old_dig_config, new_dig_config, action)
            else:
                logger.info(f"Telescope Manager found Digitiser already scanning for observation {obs.obs_id} target config index {obs.next_tgt_index}")

        return True

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

        scan_store_dir = self.telmodel.sdp.app.arguments.get('output_dir','~/') if self.telmodel.sdp.app.arguments is not None else '~/'
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
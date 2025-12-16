import logging
import json
import map
import os
from pathlib import Path
import socket
import time
from datetime import datetime, timezone

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
from env.events import ConnectEvent, DisconnectEvent, DataEvent, ConfigEvent
from ipc.message import AppMessage, APIMessage
from ipc.action import Action
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.app import AppModel
from models.comms import CommunicationStatus
from models.dig import DigitiserModel
from models.dsh import DishManagerModel, Feed
from models.obs import Observation, ObsEvent, ObsState
from models.oda import ODAModel, ObsList, ScanStore
from models.health import HealthState
from models.sdp import ScienceDataProcessorModel
from models.telescope import TelescopeModel
from util import log, util
from util.xbase import XBase, XStreamUnableToExtract
from webhook_handler import WebhookHandler

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The SHEET ID for the ALSTON RADIO TELESCOPE google sheet
ALSTON_RADIO_TELESCOPE = "1r73N0VZHSQC6RjRv94gzY50pTgRvaQWfctGGOMZVpzc"

TM_UI_API = "TM_UI_API!"            # Range for UI-TM API data
TM_UI_POLL_INTERVAL_S = 5           # Poll interval in seconds

ODT_OBS_LIST = TM_UI_API + "D2"     # Range for Observation Design Tool
DIG001_CONFIG = TM_UI_API + "B3"    # Range for Digitiser 001 configuration

OUTPUT_DIR = '/Users/r.brederode/samples'  # Directory to store observations

class TelescopeManager(App):

    telmodel = TelescopeModel()

    def __init__(self, app_name: str = "tm"):

        super().__init__(app_name=app_name, app_model=self.telmodel.tel_mgr.app)

        # Dish Manager interface
        self.dm_system = "dm"
        self.dm_api = tm_dm.TM_DM()
        # Dish Manager TCP Client
        self.dm_endpoint = TCPClient(description=self.dm_system, queue=self.get_queue(), host=self.get_args().dm_host, port=self.get_args().dm_port)
        self.dm_endpoint.connect()
        # Register Dish Manager interface with the App
        self.register_interface(self.dm_system, self.dm_api, self.dm_endpoint)
        # Initialise Dish Manager comms status
        self.telmodel.dsh_mgr.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.dm_connected = CommunicationStatus.NOT_ESTABLISHED

        # Digitiser interface
        self.dig_system = "dig"
        self.dig_api = tm_dig.TM_DIG()
        # Digitiser TCP Client
        self.dig_endpoint = TCPClient(description=self.dig_system, queue=self.get_queue(), host=self.get_args().dig_host, port=self.get_args().dig_port)
        self.dig_endpoint.connect()
        # Register Digitiser interface with the App
        self.register_interface(self.dig_system, self.dig_api, self.dig_endpoint)
        # Initialise Digitiser comms status
        self.telmodel.dig_mgr.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.dig_connected = CommunicationStatus.NOT_ESTABLISHED
        
        # Science Data Processor interface 
        self.sdp_system = "sdp"
        self.sdp_api = tm_sdp.TM_SDP()
        # Science Data Processor TCP Client
        self.sdp_endpoint = TCPClient(description=self.sdp_system, queue=self.get_queue(), host=self.get_args().sdp_host, port=self.get_args().sdp_port)
        self.sdp_endpoint.connect()
        # Register Science Data Processor interface with the App
        self.register_interface(self.sdp_system, self.sdp_api, self.sdp_endpoint)
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

        action = Action()
        return action

    def process_config(self, event: ConfigEvent) -> Action:
        """ Processes configuration update events.
        """
        logger.info(f"Telescope Manager received updated configuration: {event}")

        action = Action()

        if event.category == "DIG" and self.telmodel.dig_mgr.tm_connected == CommunicationStatus.ESTABLISHED:

            for config_key in event.new_config.keys():
                config_value = event.new_config[config_key]

                # If key value is unchanged, skip it
                if event.old_config and config_key in event.old_config and event.old_config[config_key] == config_value:
                    continue

                logger.info(f"Digitiser configuration update for key: {config_key}, value: {config_value}")

                property = method = value = None

                (method, value) = map.get_method_name_value(config_key, config_value)
                (property, value) = map.get_property_name_value(config_key, config_value) if method is None else (None, config_value)

                # Currently only support updating dig001
                if config_key == "dig_id":
                    if config_value.lower().startswith("dig001"):
                        continue
                    else:
                        logger.warning(f"Telescope Manager only supports configuration of dig001, ignoring config for {config_value}")
                        break

                if method is None and property is None:
                    logger.warning(f"Telescope Manager received unknown configuration item: {config_key}")
                    continue

                logger.info(f"Sending digitiser configuration update for method: {method}, property: {property}, value: {value}")
            
                dig_req = self._construct_req_to_dig(property=property, method=method, value=value, message="")
                action.set_msg_to_remote(dig_req)

                action.set_timer_action(Action.Timer(
                    name=f"dig_req_timer:{dig_req.get_timestamp()}", 
                    timer_action=self.telmodel.tel_mgr.app.msg_timeout_ms, 
                    echo_data=dig_req))

        elif event.category == "ODT": # Observation Design Tool

            # Observation Design Tool (ODT) is the source of truth for new (ObsState = EMPTY) observations
            # Observation Data Archive (ODA) is the source of truth for in progress (ObsState != EMPTY) observations

            # Extract a list of ObsState = EMPTY observations from the incoming ODT configuration event (JSON)
            odt = ObsList.from_dict(event.new_config)
            odt_empty_obs = [obs for obs in odt.obs_list if obs.obs_state == ObsState.EMPTY]
            
            # Create dictionary of EMPTY ODT observations for quick lookup
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
                        
                        obs.obs_state = ObsState.ABORTED
                        obs.last_update = datetime.now(timezone.utc)
                        obs.save_to_disk(OUTPUT_DIR)
                           
            # Add new EMPTY observations from ODT to ODA
            for odt_obs in odt_empty_obs:
                if not any(existing_obs.obs_id == odt_obs.obs_id for existing_obs in self.telmodel.oda.obs_store.obs_list):
                    logger.info(f"Adding new observation {odt_obs.obs_id} from ODT to ODA")
                    self.telmodel.oda.obs_store.obs_list.append(odt_obs)

            # Start a timer to trigger on the next observation start time
            action = self.obs_start_next_timer(action)

        else:
            logger.info(f"Telescope Manager updated configuration received for {event.category}.")

        return action

    def process_obs_event(self, event: ObsEvent, action: Action):
        """ Processes observations in the ODA observation store.
            Adds actions to the provided Action object as needed.
        """
        logger.debug(f"Telescope Manager processing observations in ODA store")

        now = datetime.now(timezone.utc)

        for obs in self.telmodel.oda.obs_store.obs_list:
            if obs.obs_state == ObsState.RESOURCING:
                self.process_resourcing_obs(obs, action)
            elif obs.obs_state == ObsState.CONFIGURING:
                self.process_configuring_obs(obs, action)
            elif obs.obs_state == ObsState.IDLE:
                self.process_idle_obs(obs, action)
            elif obs.obs_state == ObsState.ABORTED:
                self.process_aborted_obs(obs, action)
            elif obs.obs_state == ObsState.FAULT:
                self.process_fault_obs(obs, action)
            elif obs.obs_state == ObsState.READY:
                self.process_ready_obs(obs, action)
            elif obs.obs_state == ObsState.SCANNING:
                self.process_scanning_obs(obs, action)
        
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

    def process_dig_connected(self, event) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Telescope Manager connected to Digitiser: {event.remote_addr}")

        self.telmodel.dig_mgr.tm_connected = CommunicationStatus.ESTABLISHED
        self.telmodel.tel_mgr.dig_connected = CommunicationStatus.ESTABLISHED

        action = Action()
        return action

    def process_dig_disconnected(self, event) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Digitiser: {event.remote_addr}")

        self.telmodel.dig_mgr.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tel_mgr.dig_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_dig_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received digitiser {api_call['msg_type']} message with action code: {api_call['action_code']}")
        
        action = Action()

        if api_call.get('status','') != tm_dig.STATUS_ERROR:

            if api_call.get('property','') == tm_dig.PROPERTY_FEED:
                self.telmodel.dsh_mgr.feed = Feed(api_call['value'])
                self.telmodel.dig_mgr.feed = Feed(api_call['value'])
            elif api_call.get('property','') == tm_dig.PROPERTY_STREAMING:
                self.telmodel.dig_mgr.streaming = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_GAIN:
                self.telmodel.dig_mgr.gain = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_SAMPLE_RATE:
                self.telmodel.dig_mgr.sample_rate = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_BANDWIDTH:
                self.telmodel.dig_mgr.bandwidth = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_CENTER_FREQ:
                self.telmodel.dig_mgr.center_freq = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_FREQ_CORRECTION:
                self.telmodel.dig_mgr.freq_correction = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_SDP_COMMS:
                self.telmodel.dig_mgr.sdp_connected = CommunicationStatus(api_call['value'])
            elif api_call.get('property','') == tm_dig.PROPERTY_STATUS:
                logger.debug(f"Telescope Manager received Digitiser STATUS update: {api_call['value']}")

                self.telmodel.dig_mgr = DigitiserModel.from_dict(api_call['value'])

        # Update Telescope Model based on received Digitiser api_call
        dt = api_msg.get("timestamp")
        self.telmodel.dig_mgr.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message, stop the corresponding timer
        if api_call['msg_type'] == tm_dig.MSG_TYPE_RSP and dt is not None:
            action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dt}", timer_action=Action.Timer.TIMER_STOP))

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

            # If a status update is received, update the Telescope Model 
            if api_call.get('property','') == tm_sdp.PROPERTY_STATUS:
                self.telmodel.sdp = ScienceDataProcessorModel.from_dict(api_call['value'])

        dt = api_msg.get("timestamp")
        self.telmodel.sdp.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message, stop the corresponding timer
        if api_call['msg_type'] == tm_sdp.MSG_TYPE_RSP and dt is not None:
            action.set_timer_action(Action.Timer(name=f"sdp_req_timer:{dt}", timer_action=Action.Timer.TIMER_STOP))

        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"Telescope Manager timer event: {event}")

        action = Action()

        if event.name.startswith("dig_req_timer"):
            
            logger.warning(f"Telescope Manager timed out waiting for acknowledgement from Digitiser for request {event}")

        elif event.name.startswith("observation_start_timer"):
            logger.info(f"Telescope Manager observation timer event: {event}")

            now = datetime.now(timezone.utc)

            # Transition observations that are scheduled for the current scheduling block from ObsState = EMPTY to ObsState = RESOURCING
            # It is possible that multiple observations are scheduled for the current scheduling block and that some cannot be resourced
            # Example: A dish has become UNAVAILABLE, so only some observations can be resourced
            for obs in self.telmodel.oda.obs_store.obs_list:

                # Calculate difference between now and the observation scheduling block start time in seconds
                start_offset = abs((obs.scheduling_block_start - now).total_seconds())
  
                # Start resourcing observations scheduled to start now (within 60 seconds)
                if obs.obs_state == ObsState.EMPTY and start_offset <= 60:
                    obs.obs_state = ObsState.IDLE
                    obs.last_update = datetime.now(timezone.utc)
                    logger.info(f"Telescope Manager resourcing observation {obs.obs_id} scheduled for {obs.scheduling_block_start}")
            
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
                name=f"observation_start_timer", 
                timer_action=time_until_start_ms,
                echo_data=next_obs))
            logger.info(f"Telescope Manager next observation {next_obs.obs_id} starting at {next_obs.scheduling_block_start} in {time_until_start_ms} ms")

        return action

    def obs_resource_alloc(self, obs: Observation, action: Action) -> Action:
        """ Process an observation resource allocation request.
        """
        logger.info(f"Telescope Manager processing RESOURCE ALLOCATION for observation {obs.obs_id}")

        # Extract required resources from observation
        dish_id = obs.dish_id
        dig_id = obs.dig_id

        # Check for resource contention with existing observations
        contending_obs = [existing_obs for existing_obs in self.telmodel.oda.obs_store.obs_list if existing_obs.dish_id == obs.dish_id and existing_obs.obs_id != obs.obs_id]

        # Handle each observation that is contending for resources
        for existing_obs in contending_obs:
                
            if existing_obs.obs_state in [ObsState.CONFIGURING, ObsState.READY, ObsState.SCANNING, ObsState.ABORTED, ObsState.FAULT]:    
                logger.error(f"Telescope Manager resource contention between observation {obs.obs_id} and {existing_obs.obs_id} for Dish {obs.dish_id}")
                action = self.obs_abort(existing_obs, action)
                action = self.obs_resource_dealloc(obs, action)
            elif existing_obs.obs_state == ObsState.IDLE:
                logger.info(f"Telescope Manager deallocating resource from observation {existing_obs.obs_id} to assign to observation {obs.obs_id}")
                action = self.obs_resource_dealloc(existing_obs, action)

        # Check for resource contention with existing observations AGAIN
        contending_obs = [existing_obs for existing_obs in self.telmodel.oda.obs_store.obs_list if existing_obs.dish_id == obs.dish_id and existing_obs.obs_id != obs.obs_id]

        if len(contending_obs) > 0:
            logger.error(f"Telescope Manager unable to allocate resources for observation {obs.obs_id} due to existing resource contention")
            obs.obs_state = ObsState.FAULT
            obs.last_update = datetime.now(timezone.utc)
        else:
            logger.info(f"Telescope Manager successfully allocated {obs.dish_id} for observation {obs.obs_id}")
            obs.obs_state = ObsState.CONFIGURING
            obs.last_update = datetime.now(timezone.utc)

        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.telmodel.tel_mgr.dig_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.telmodel.tel_mgr.sdp_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.telmodel.tel_mgr.dm_connected != CommunicationStatus.ESTABLISHED:
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

            # Limit to the latest 50 files of each type
            spr_files = spr_files[:50]
            load_files = load_files[:50]
            tsys_files = tsys_files[:50]
            gain_files = gain_files[:50]
            meta_files = meta_files[:50]

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

        self.telmodel.tel_mgr.last_update = datetime.now(timezone.utc)

    def _construct_req_to_dig(self, property=None, method=None, value=None, message=None) -> APIMessage:
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
                api_call={
                    "msg_type": "req", 
                    "action_code": "set", 
                    "property": property, 
                    "value": value if value is not None else 0, 
                    "message": message if message else ""
            })

        return dig_req

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

            # If comms to the Digitiser is established then exchange Digitiser model data with the UI 
            if tm.telmodel.dig_mgr.tm_connected == CommunicationStatus.ESTABLISHED:

                dig_latest_update = tm.telmodel.dig_mgr.last_update if tm.telmodel.dig_mgr.last_update else datetime.now(timezone.utc)

                # Push updated Digitiser model to Google Sheets if there are updates
                if dig_latest_update > last_dig_model_push:

                    dig_dict = tm.telmodel.dig_mgr.to_dict()
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

            else:
                # Reset the snapshot such that config is re-read upon reconnection
                last_dig_config_snapshot = None

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
            time.sleep(TM_UI_POLL_INTERVAL_S) 
    except KeyboardInterrupt:
        pass
    finally:
        tm.stop()

if __name__ == "__main__":
    main()
import logging
import json
import map
import os
from pathlib import Path
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
from api import tm_dig, tm_sdp
from env.app import App
from env.events import ConnectEvent, DisconnectEvent, DataEvent, ConfigEvent
from ipc.message import AppMessage, APIMessage
from ipc.action import Action
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.app import AppModel
from models.dsh import Feed
from models.health import HealthState
from models.comms import CommunicationStatus
from models.telescope import TelescopeModel
from util import log
from util.xbase import XBase, XStreamUnableToExtract

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The SHEET ID for the ALSTON RADIO TELESCOPE google sheet
ALSTON_RADIO_TELESCOPE = "1r73N0VZHSQC6RjRv94gzY50pTgRvaQWfctGGOMZVpzc"
DIG001_CONFIG = "DIG001!D4:E11"     # Range for Digitiser 001 configuration
DIG002_CONFIG = "DIG002!D4:E11"     # Range for Digitiser 002 configuration
TM_UI_API = "TM_UI_API!"            # Range for UI-TM API data
TM_UI_POLL_INTERVAL_S = 5            # Poll interval in seconds

class TelescopeManager(App):

    telmodel = TelescopeModel()

    def __init__(self, app_name: str = "tm"):

        super().__init__(app_name=app_name, app_model=self.telmodel.tm.app)

        # Digitiser interface
        self.dig_system = "dig"
        self.dig_api = tm_dig.TM_DIG()
        # Digitiser TCP Client
        self.dig_endpoint = TCPClient(description=self.dig_system, queue=self.get_queue(), host=self.get_args().dig_host, port=self.get_args().dig_port)
        self.dig_endpoint.connect()
        # Register Digitiser interface with the App
        self.register_interface(self.dig_system, self.dig_api, self.dig_endpoint)
        # Initialise Digitiser comms status
        self.telmodel.dig.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tm.dig_connected = CommunicationStatus.NOT_ESTABLISHED
        
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
        self.telmodel.tm.sdp_connected = CommunicationStatus.NOT_ESTABLISHED

    def add_args(self, arg_parser): 
        """ Specifies the digitiser's command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--dig_host", type=str, required=False, help="TCP server host to listen for Digitiser connections", default="localhost")
        arg_parser.add_argument("--dig_port", type=int, required=False, help="TCP server port to listen for Digitiser connections", default=50000) 

        arg_parser.add_argument("--sdp_host", type=str, required=False, help="TCP server host to connect to the Science Data Processor",default="localhost")
        arg_parser.add_argument("--sdp_port", type=int, required=False, help="TCP server port to connect to the Science Data Processor", default=50001)

    def process_init(self) -> Action:
        """ Processes initialisation events.
        """
        logger.debug(f"TM initialisation event")

        action = Action()
        return action

    def process_config(self, event: ConfigEvent) -> Action:
        """ Processes configuration events.
        """
        logger.info(f"Telescope Manager received configuration: {event}")

        action = Action()

        # Compare each item in the new configuration with the old configuration
        for i, new_item in enumerate(event.new_config):

            property = method = value = None

            # Map from configuration item to method/property and value
            (method, value) = map.get_method_name(new_item[0], new_item[1])
            (property, value) = map.get_property_name(new_item[0], new_item[1]) if method is None else (None, None)
                
            if method is None and property is None:
                logger.warning(f"Telescope Manager received unknown configuration item at row {i}: {new_item} with value {new_item[1]}")
                continue

            update = False

            if event.old_config is not None:
                if i < len(event.old_config):
                    old_item = event.old_config[i]
                    if new_item != old_item:
                        update = True
                        logger.info(f"Telescope property '{property}' changed at row {i}: from {old_item} to {new_item}")
                else:
                    update = True
                    logger.info(f"Telescope property '{property}' added at row {i}: {new_item}")
            else:
                update = True
                logger.info(f"Telescope property '{property}' initialising at row {i}: {new_item}")

            if update and self.telmodel.dig.tm_connected == CommunicationStatus.ESTABLISHED:
                dig_req = self._construct_req_to_dig(property=property, method=method, value=value, message="")
                action.set_msg_to_remote(dig_req)

                action.set_timer_action(Action.Timer(
                    name=f"dig_req_timer:{dig_req.get_timestamp()}", 
                    timer_action=self.telmodel.tm.app.msg_timeout_ms, 
                    echo_data=dig_req))

        return action

    def process_dig_connected(self, event) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Telescope Manager connected to Digitiser: {event.remote_addr}")

        self.telmodel.dig.tm_connected = CommunicationStatus.ESTABLISHED
        self.telmodel.tm.dig_connected = CommunicationStatus.ESTABLISHED

        action = Action()
        # TBD, remove this once we have a UI to control reading samples
        action.set_timer_action(Action.Timer(name="read_samples", timer_action=10000))
        return action

    def process_dig_disconnected(self, event) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Digitiser: {event.remote_addr}")

        self.telmodel.dig.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tm.dig_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_dig_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received digitiser {api_call['msg_type']} message with action code: {api_call['action_code']}")
        
        action = Action()

        if api_call.get('status','') != tm_dig.STATUS_ERROR:

            if api_call.get('property','') == tm_dig.PROPERTY_FEED:
                self.telmodel.dsh.feed = Feed(api_call['value'])
            elif api_call.get('property','') == tm_dig.PROPERTY_STREAMING:
                self.telmodel.dig.streaming = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_GAIN:
                self.telmodel.dig.gain = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_SAMPLE_RATE:
                self.telmodel.dig.sample_rate = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_BANDWIDTH:
                self.telmodel.dig.bandwidth = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_CENTER_FREQ:
                self.telmodel.dig.center_freq = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_FREQ_CORRECTION:
                self.telmodel.dig.freq_correction = api_call['value']
            elif api_call.get('property','') == tm_dig.PROPERTY_SDP_COMMS:
                self.telmodel.dig.sdp_connected = CommunicationStatus(api_call['value'])
            elif api_call.get('property','') == tm_dig.PROPERTY_STATUS:
                logger.debug(f"Telescope Manager received Digitiser STATUS update: {api_call['value']}")

                self.telmodel.dig.from_dict(api_call['value'])

        # Update Telescope Model based on received Digitiser api_call
        dt = api_msg.get("timestamp")
        self.telmodel.dig.last_update = datetime.fromisoformat(dt) if dt else datetime.now(timezone.utc)

        # If the api call is a rsp message, stop the corresponding timer
        if api_call['msg_type'] == tm_dig.MSG_TYPE_RSP and dt is not None:
            action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dt}", timer_action=Action.Timer.TIMER_STOP))

        return action

    def process_sdp_connected(self, event) -> Action:
        """ Processes Science Data Processor connected events.
        """
        logger.info(f"Telescope Manager connected to Science Data Processor: {event.remote_addr}")

        self.telmodel.sdp.tm_connected = CommunicationStatus.ESTABLISHED
        self.telmodel.tm.sdp_connected = CommunicationStatus.ESTABLISHED

    def process_sdp_disconnected(self, event) -> Action:
        """ Processes Science Data Processor disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Science Data Processor: {event.remote_addr}")

        self.telmodel.sdp.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        self.telmodel.tm.sdp_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_sdp_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Science Data Processor service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received Science Data Processor {api_call['msg_type']} message with action code: {api_call['action_code']}")

        action = Action()

        if api_call.get('status','') != tm_sdp.STATUS_ERROR:

            # If a status update is received, update the Telescope Model 
            if api_call.get('property','') == tm_sdp.PROPERTY_STATUS:
                logger.debug(f"Telescope Manager received Science Data Processor STATUS update: {api_call['value']}")

                self.telmodel.sdp.from_dict(api_call['value'])

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

        if event.name.startswith("read_samples"):
            if self.telmodel.dig.tm_connected == CommunicationStatus.ESTABLISHED:
                dig_req = self._construct_req_to_dig(method="read_samples")
                action.set_msg_to_remote(dig_req)

                action.set_timer_action(Action.Timer(
                    name=f"dig_req_timer:{dig_req.get_timestamp()}", 
                    timer_action=self.telmodel.tm.app.msg_timeout_ms, 
                    echo_data=dig_req))
            else:
                action.set_timer_action(Action.Timer(name="read_samples", timer_action=self.telmodel.tm.app.msg_timeout_ms))

        elif event.name.startswith("dig_req_timer"):
            logger.warning(f"Telescope Manager timed out waiting for acknowledgement from Digitiser for request {event}")

        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.telmodel.tm.dig_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.telmodel.tm.sdp_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        else:
            return HealthState.OK

    def process_status_event(self, event) -> Action:
        """ Processes status update events. 
            Calls get_app_processor_state() to update the Telescope Model status.
            Reads the scan store directory to update the scan store file lists.
        """
        status = self.get_app_processor_state()

        scan_store_dir = self.telmodel.sdp.app.arguments.get('output_dir','~/')

        if Path(scan_store_dir).exists():

            # Read scan store directory listing
            spr_files = list(Path(scan_store_dir).glob("*spr.csv"))
            load_files = list(Path(scan_store_dir).glob("*load.csv"))
            tsys_files = list(Path(scan_store_dir).glob("*tsys.csv"))
            gain_files = list(Path(scan_store_dir).glob("*gain.csv"))
            meta_files = list(Path(scan_store_dir).glob("*meta.json"))

            # Sort by file name in reverse order (newest last)
            spr_files.sort(key=lambda x: x.name, reverse=True)
            load_files.sort(key=lambda x: x.name, reverse=True)
            tsys_files.sort(key=lambda x: x.name, reverse=True)
            gain_files.sort(key=lambda x: x.name, reverse=True)
            meta_files.sort(key=lambda x: x.name, reverse=True)

            # Limit to the latest 100 files of each type
            spr_files = spr_files[:100]
            load_files = load_files[:100]
            tsys_files = tsys_files[:100]
            gain_files = gain_files[:100]
            meta_files = meta_files[:100]

            # Combine into a single list of scan files
            scan_files = spr_files + load_files + tsys_files + gain_files + meta_files

            self.telmodel.tm.scan_store.spr_files = []
            self.telmodel.tm.scan_store.load_files = []
            self.telmodel.tm.scan_store.tsys_files = []
            self.telmodel.tm.scan_store.gain_files = []
            self.telmodel.tm.scan_store.meta_files = []

            for scan_file in scan_files:
                if scan_file.name.endswith("spr.csv"):
                    self.telmodel.tm.scan_store.spr_files.append(scan_file.name)
                elif scan_file.name.endswith("load.csv"):
                    self.telmodel.tm.scan_store.load_files.append(scan_file.name)
                elif scan_file.name.endswith("tsys.csv"):
                    self.telmodel.tm.scan_store.tsys_files.append(scan_file.name)
                elif scan_file.name.endswith("gain.csv"):
                    self.telmodel.tm.scan_store.gain_files.append(scan_file.name)
                elif scan_file.name.endswith("meta.json"):
                    self.telmodel.tm.scan_store.meta_files.append(scan_file.name)

            self.telmodel.tm.scan_store.last_update = datetime.now(timezone.utc)

        self.telmodel.tm.last_update = datetime.now(timezone.utc)

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
                except SocketTimeout as e:
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

    last_dig_snapshot = None
    last_sdp_snapshot = None

    try:
        while True:

            # Exchange Digitiser data with the UI if comms is established
            if tm.telmodel.tm.dig_connected == CommunicationStatus.ESTABLISHED:
                
                result = execute_sheets_request(sheet.values().get(
                    spreadsheetId=ALSTON_RADIO_TELESCOPE, 
                    range=DIG001_CONFIG,
                    valueRenderOption="UNFORMATTED_VALUE")) # Retrieve values in their original form e.g. int, float, string, date

                if 'error' in result:
                    error = result['error']['details'][0]
                    error_msg = error.get('errorMessage', 'Unknown error')
                    logger.error(f'TM - error getting DIG configuration: {error_msg}')
                else:
                    values = result.get("values", [])

                    if values != last_dig_snapshot:

                        config = ConfigEvent(old_config=last_dig_snapshot, new_config=values, timestamp=datetime.now(timezone.utc))
                        tm.get_queue().put(config)

                        last_dig_snapshot = values

                dig_dict = tm.telmodel.dig.to_dict()
                dig_str = json.dumps(dig_dict, indent=4)

                execute_sheets_request(sheet.values().update(
                    spreadsheetId=ALSTON_RADIO_TELESCOPE,
                    range=TM_UI_API + "B2",                      
                    valueInputOption="USER_ENTERED", # allow Sheets to parse as datetime
                    body={"values": [[dig_str]]}
                ))

            else:
                # Reset the snapshot such that config is re-read upon reconnection
                last_dig_snapshot = None

            # Exchange SDP data with the UI if comms is established
            if tm.telmodel.tm.sdp_connected == CommunicationStatus.ESTABLISHED:

                sdp_dict = tm.telmodel.sdp.to_dict()
                sdp_str = json.dumps(sdp_dict, indent=4)

                execute_sheets_request(sheet.values().update(
                    spreadsheetId=ALSTON_RADIO_TELESCOPE,
                    range=TM_UI_API + "C2",
                    valueInputOption="USER_ENTERED", # allow Sheets to parse as datetime
                    body={"values": [[sdp_str]]}
                ))
                
            # Update TM model in Google Sheets
            tm_dict = tm.telmodel.tm.to_dict()
            tm_str = json.dumps(tm_dict, indent=4)

            execute_sheets_request(sheet.values().update(
                spreadsheetId=ALSTON_RADIO_TELESCOPE,
                range=TM_UI_API + "A2",
                valueInputOption="USER_ENTERED",  # allow Sheets to parse as datetime
                body={"values": [[tm_str]]}
            ))

            # TM to UI Poll interval
            time.sleep(TM_UI_POLL_INTERVAL_S) 
    except KeyboardInterrupt:
        pass
    finally:
        tm.stop()

if __name__ == "__main__":
    main()
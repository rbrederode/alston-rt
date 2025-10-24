import logging
import json
import map
import os
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
from models.dsh import Feed
from util import log
from util.xbase import XBase, XStreamUnableToExtract

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The SHEET ID for the ALSTON RADIO TELESCOPE google sheet
ALSTON_RADIO_TELESCOPE = "1r73N0VZHSQC6RjRv94gzY50pTgRvaQWfctGGOMZVpzc"
DIG_CONFIG = "DIG!A4:B15"  # Range for Digitiser configuration

MSG_TIMEOUT = 10000 # Timeout in milliseconds for messages awaiting acknowledgement

class TelescopeManager(App):

    def __init__(self, app_name: str = "tm"):

        super().__init__(app_name=app_name)

        # Digitiser interface
        self.dig_system = "dig"
        self.dig_api = tm_dig.TM_DIG()
        # Digitiser TCP Client
        self.dig_endpoint = TCPClient(description=self.dig_system, queue=self.get_queue(), host=self.get_args().dig_host, port=self.get_args().dig_port)
        self.dig_endpoint.connect()
        self.dig_connected = False
        # Register Digitiser interface with the App
        self.register_interface(self.dig_system, self.dig_api, self.dig_endpoint)

        # Science Data Processor interface 
        self.sdp_system = "sdp"
        self.sdp_api = tm_sdp.TM_SDP()
        # Science Data Processor TCP Client
        self.sdp_endpoint = TCPClient(description=self.sdp_system, queue=self.get_queue(), host=self.get_args().sdp_host, port=self.get_args().sdp_port)
        self.sdp_endpoint.connect()
        self.sdp_connected = False
        # Register Science Data Processor interface with the App
        self.register_interface(self.sdp_system, self.sdp_api, self.sdp_endpoint)

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
        action.set_timer_action(Action.Timer(name="read_samples", timer_action=MSG_TIMEOUT))
        return action

    def process_config(self, event: ConfigEvent) -> Action:
        """ Processes configuration events.
        """
        logger.info(f"Telescope Manager received configuration: {event}")

        action = Action()

        # Compare each item in the new configuration with the old configuration
        for i, new_item in enumerate(event.new_config):

            property = map.get_property_name(new_item[0])
            method = None
            value = new_item[1]

            if property is None:
                logger.warning(f"Telescope Manager received unknown configuration item at row {i}: {new_item}")
                continue

            update = False

            if event.old_config is not None:
                if i < len(event.old_config):
                    old_item = event.old_config[i]
                    if new_item != old_item:
                        update = True
                        logger.info(f"Telescope Manager property {property} changed at row {i}: from {old_item} to {new_item}")
                else:
                    update = True
                    logger.info(f"Telescope Manager property {property} added at row {i}: {new_item}")
            else:
                update = True
                logger.info(f"Telescope Manager property {property} initialising at row {i}: {new_item}")

            if property == tm_dig.PROPERTY_GAIN:
                # Convert value to uppercase string for comparison
                if str(value).upper() == "AUTO":
                    method = tm_dig.METHOD_GET_AUTO_GAIN
                    value = {"time_in_secs": 0.5}
                else:
                    method = None
                    # Convert value to integer if not AUTO
                    try:
                        value = int(value)
                    except ValueError:
                        logger.error(f"Telescope Manager invalid GAIN value at row {i}: {new_item}")
                        continue
            elif property == tm_dig.PROPERTY_FEED:
                # Map feed string to Feed enum
                feed_id = map.get_feed_id(value)
                if feed_id is not None:
                    value = feed_id
                else:
                    logger.error(f"Telescope Manager invalid FEED value at row {i}: {new_item}")
                    continue

            if update and self.dig_connected:
                dig_req = self._construct_req_to_dig(property=property, method=method, value=value, message="")
                action.set_msg_to_remote(dig_req)
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dig_req.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=dig_req))

        return action

    def process_dig_connected(self, event) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Telescope Manager connected to Digitiser: {event.remote_addr}")

        self.dig_connected = True

        action = Action()
        return action

    def process_dig_disconnected(self, event) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Digitiser: {event.remote_addr}")

        self.dig_connected = False
        
    def process_dig_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received digitiser {api_call['msg_type']} message with action code: {api_call['action_code']}")

        action = Action()

        if api_call['msg_type'] == 'rsp':
            dt = api_msg.get("timestamp")
            if dt:
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dt}", timer_action=Action.Timer.TIMER_STOP))
            
        return action

    def process_sdp_connected(self, event) -> Action:
        """ Processes Science Data Processor connected events.
        """
        logger.info(f"Telescope Manager connected to Science Data Processor: {event.remote_addr}")

        self.sdp_connected = True

    def process_sdp_disconnected(self, event) -> Action:
        """ Processes Science Data Processor disconnected events.
        """
        logger.info(f"Telescope Manager disconnected from Science Data Processor: {event.remote_addr}")

        self.sdp_connected = False

    def process_sdp_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Science Data Processor service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Telescope Manager received Science Data Processor {api_call['msg_type']} message with action code: {api_call['action_code']}")

        action = Action()
        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"Telescope Manager timer event: {event}")

        action = Action()

        if event.name.startswith("read_samples"):
            if self.dig_connected:
                dig_req = self._construct_req_to_dig(method="read_samples")
                action.set_msg_to_remote(dig_req)
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dig_req.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=dig_req))
            else:
                action.set_timer_action(Action.Timer(name="read_samples", timer_action=MSG_TIMEOUT))

        elif event.name.startswith("dig_req_timer"):
            logger.warning(f"Telescope Manager timed out waiting for acknowledgement from Digitiser for request {event}")

        return action

    def _construct_req_to_dig(self, property=None, method=None, value=None, message=None) -> APIMessage:
        """ Constructs a request message to the Digitiser.
        """

        dig_req = APIMessage(api_version=self.dig_api.get_api_version())

        # If property is get_auto_gain or read_samples
        if method is not None:
            dig_req.set_json_api_header(
                api_version=self.dig_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_name, 
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
                from_system=self.app_name, 
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

    last_snapshot = None

    try:
        while True:

            # Only proceed if connected to the Digitiser
            if tm.dig_connected:
                
                result = execute_sheets_request(sheet.values().get(
                    spreadsheetId=ALSTON_RADIO_TELESCOPE, 
                    range=DIG_CONFIG,
                    valueRenderOption="UNFORMATTED_VALUE")) # Retrieve values in their original form e.g. int, float, string, date

                if 'error' in result:
                    error = result['error']['details'][0]
                    error_msg = error.get('errorMessage', 'Unknown error')
                    logger.error(f'TM - error getting DIG configuration: {error_msg}')
                else:
                    values = result.get("values", [])

                    if values != last_snapshot:

                        logger.info(f"Telescope Manager configuration values changed:\n" +  \
                            f"Old Values: {last_snapshot}\n" +  \
                            f"New Values: {values}")

                        config = ConfigEvent(old_config=last_snapshot, new_config=values, timestamp=datetime.now(timezone.utc))
                        tm.get_queue().put(config)

                        last_snapshot = values

            # Poll for config changes every 10 seconds
            time.sleep(10) 
    except KeyboardInterrupt:
        pass
    finally:
        tm.stop()

if __name__ == "__main__":
    main()
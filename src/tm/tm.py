import logging
import json
import time
from datetime import datetime, timezone

from env.app import App
from api import tm_dig, tm_sdp
from ipc.message import AppMessage, APIMessage
from ipc.action import Action
from util.xbase import XBase, XStreamUnableToExtract
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.dsh import Feed
from util import log

logger = logging.getLogger(__name__)

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
        return action

    def process_dig_connected(self, event) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Telescope Manager connected to Digitiser: {event.remote_addr}")

        self.dig_connected = True

        action = Action()

        # Send initial configuration request to the Digitiser
        dig_req = self._construct_req_to_dig(property="center_freq", value=1420.40e6, message="")

        action.set_msg_to_remote(dig_req)
        action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dig_req.get_timestamp()}", timer_action=Action.Timer.TIMER_START, echo_data=dig_req))

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

        if api_call['msg_type'] == 'rsp' and api_call['action_code'] == 'set':
            dt = api_msg.get("timestamp")
            if dt:
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dt}", timer_action=Action.Timer.TIMER_STOP))

            # Check if api_call['message'] contains set_center_freq

            if 'set_center_freq' in api_call.get('message', ''):
                dig_req = self._construct_req_to_dig(property="sample_rate", value=2.4e6, message="")
                action.set_msg_to_remote(dig_req)
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dig_req.get_timestamp()}", timer_action=Action.Timer.TIMER_START, echo_data=dig_req))
            elif 'set_sample_rate' in api_call.get('message', ''):
                dig_req = self._construct_req_to_dig(property="freq_correction", value=-140, message="")
                action.set_msg_to_remote(dig_req)
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dig_req.get_timestamp()}", timer_action=Action.Timer.TIMER_START, echo_data=dig_req))
            elif 'set_freq_correction' in api_call.get('message', ''):
                dig_req = self._construct_req_to_dig(property="get_auto_gain", value=2.4e6, message="")
                action.set_msg_to_remote(dig_req)
                action.set_timer_action(Action.Timer(name=f"dig_req_timer:{dig_req.get_timestamp()}", timer_action=Action.Timer.TIMER_START, echo_data=dig_req))
            
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
        return action

    def _construct_req_to_dig(self, property, value, message) -> APIMessage:
        """ Constructs a request message to the Digitiser.
        """

        dig_req = APIMessage(api_version=self.dig_api.get_api_version())

        if property == "get_auto_gain":
            dig_req.set_json_api_header(
                api_version=self.dig_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_name, 
                to_system="dig", 
                api_call={
                    "msg_type": "req", 
                    "action_code": "method", 
                    "method": property, 
                    "params": {"sample_rate": value, "time_in_secs": 1}
            })
        else:
            dig_req.set_json_api_header(
                api_version=self.dig_api.get_api_version(), 
                dt=datetime.now(timezone.utc), 
                from_system=self.app_name, 
                to_system="dig", 
                api_call={
                    "msg_type": "req", 
                    "action_code": "set", 
                    "property": property, 
                    "value": value, 
                    "message": message if message else ""
            })

        return dig_req

def main():
    tm = TelescopeManager()
    tm.start() 

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        tm.stop()

if __name__ == "__main__":
    main()
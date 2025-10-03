import logging
import json
import numpy as np
import datetime
import time
from datetime import timezone
from rtlsdr import RtlSdr

from env.app import App
from ipc.message import AppMessage
from util.xbase import XBase, XStreamUnableToExtract
from sdr.sdr import SDR
from api import tm_dig, sdp_dig
from ipc.message import APIMessage
from ipc.action import Action
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer

class MillisecondFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        t = time.strftime(datefmt, ct)
        s = "%s:%03d" % (t, record.msecs)
        return s

# Configure logging
logging.basicConfig(level=logging.INFO)  # Or DEBUG for more verbosity
for handler in logging.root.handlers:
    handler.setFormatter(MillisecondFormatter(
        '%(asctime)s %(levelname)s: %(message)s',  # log format
        datefmt='%Y-%m-%d %H:%M:%S'               # date format
    ))

logger = logging.getLogger(__name__)

logging.getLogger("ipc.tcp_server").setLevel(logging.INFO)  # Only INFO and above for tcp_server
logging.getLogger("ipc.tcp_client").setLevel(logging.INFO)  # Only INFO and above for tcp_client
logging.getLogger("util.timer").setLevel(logging.INFO)  # Only INFO and above for timer
logging.getLogger("env.processor").setLevel(logging.INFO)  # Only INFO and above for processor
logging.getLogger("env.app_processor").setLevel(logging.INFO)  # Only INFO and above for app processor
logging.getLogger("api.tm_dig").setLevel(logging.INFO)  # Only INFO and above for tm_dig api
logging.getLogger("api.sdp_dig").setLevel(logging.INFO)  # Only INFO and above for sdp_dig api
logging.getLogger("__main__").setLevel(logging.INFO)  # Only INFO and above for digitiser

class Digitiser(App):

    def __init__(self, app_name: str = "dig"):

        super().__init__(app_name=app_name)

        # Telescope Manager interface
        self.tm_system = "tm"
        self.tm_api = tm_dig.TM_DIG()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), port=self.get_args().tm_port)
        self.tm_endpoint.start()
        self.tm_connected = False
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint)

        # Science Data Processor Interface
        self.sdp_system = "sdp"
        self.sdp_api = sdp_dig.SDP_DIG()
        # Science Data Processor TCP Client
        self.sdp_endpoint = TCPClient(description=self.sdp_system, queue=self.get_queue(), host=self.get_args().sdp_host, port=self.get_args().sdp_port)
        self.sdp_endpoint.connect()
        self.sdp_connected = False
        # Register Science Data Processor interface with the App
        self.register_interface(self.sdp_system, self.sdp_api, self.sdp_endpoint)

        # Software Defined Radio (internal) interface
        self.sdr = SDR()

    def add_args(self, arg_parser): 
        """ Specifies the digitiser's command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--tm_port", type=int, required=False, help="TCP port for Telescope Manager commands", default=50000)
        arg_parser.add_argument("--sdp_host", type=str, required=False, help="TCP server host for downstream Science Data Processor transport",default="localhost")
        arg_parser.add_argument("--sdp_port", type=int, required=False, help="TCP server port for downstream Science Data Processor transport", default=60000)

    def process_tm_msg(self, event, api_msg: dict, api_call: dict) -> Action:
        """ Processes api messages received on the Telescope Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.debug(f"Digitiser received Telescope Manager message: {event}")

        if self.sdr is None or not self.sdr.get_connected():
            status, message = tm_dig.STATUS_ERROR, "Digitiser not connected to SDR device"
            value, payload = None, None
            logger.warning("Digitiser cannot process Telescope Manager message, not connected to SDR device.")
        else:

            dispatch = {
                "set": self.handle_field_set,
                "get": self.handle_field_get,
                "method": self.handle_method_call
            }

            result = dispatch.get(api_call['action_code'], lambda x: None)(api_call)
            status, message, value, payload = self._unpack_result(result)

        action = Action()

        # Prepare rsp msg to tm containing result of api call
        tm_rsp = APIMessage(api_msg=api_msg, api_version=self.tm_api.get_api_version())
        tm_rsp.switch_from_to()
        tm_rsp.set_api_call({
            "msg_type": "rsp", 
            "action_code": api_call['action_code'], 
            "status": status, 
            "message": message if message else "",
            "value": value if value else "",
        })

        action.set_msg_to_remote(tm_rsp)

        if api_call['action_code'] == 'method' and api_call['method'] in ("read_samples", "read_bytes") and payload is not None:

            if self.sdp_connected:

                # Prepare adv msg to sdp containing samples
                sdp_adv = self._construct_adv_to_sdp(status, message,payload.tobytes())

                action.set_msg_to_remote(sdp_adv)
                action.set_timer_action(Action.Timer(name="sdp_adv_timer", timer_action=30000, echo_data=sdp_adv))
                # Also set a timer(s) to read samples for {duration} seconds
                if 'duration' in api_call['params']:
                    duration = api_call['params']['duration']
                    for i in range(1,int(duration)): # 1 because we already sent the first batch of samples
                        action.set_timer_action(Action.Timer(name=f"sdr_read_samples_{i}", timer_action=0))
            else:
                logger.warning("Digitiser cannot send samples to Science Data Processor, not connected.")

                tm_adv = self._construct_adv_to_tm(property=tm_dig.PROPERTY_SDP_CONNECTED, value=False, message="Disconnected from SDP")
                action.set_msg_to_remote(tm_adv)
                action.set_timer_action(Action.Timer(name="tm_adv_timer", timer_action=30000, echo_data=tm_adv))
                
        return action

    def process_tm_connected(self, event) -> Action:
        """ Processes Telescope Manager connected events.
        """
        logger.debug(f"Digitiser connected to Telescope Manager: {event.remote_addr}")

        self.tm_connected = True

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.debug(f"Digitiser disconnected from Telescope Manager: {event.remote_addr}")

        self.tm_connected = False

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"Digitiser timer event: {event}")

        action = Action()

        if event.name.startswith("sdr_read_samples"):
            result = self.handle_method_call({"method": "read_samples", "params": {"num_samples": 2400000.0}})
            status, message, value, payload = self._unpack_result(result)

            sdp_adv = self._construct_adv_to_sdp(status, message, payload.tobytes())

            action.set_msg_to_remote(sdp_adv)
            action.set_timer_action(Action.Timer(name="sdp_adv_timer", timer_action=30000, echo_data=sdp_adv))  

        return action

    def process_sdp_msg(self, event) -> Action:
        """ Processes api messages received on the Science Data Processor service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.debug(f"Digitiser received sdp message: {event}")

    def process_sdp_connected(self, event) -> Action:
        """ Processes Science Data Processor connected events.
        """
        logger.debug(f"Digitiser connected to Science Data Processor: {event.remote_addr}")

        self.sdp_connected = True

        action = Action()

        if self.tm_connected:
            tm_adv = self._construct_adv_to_tm(property=tm_dig.PROPERTY_SDP_CONNECTED, value=True, message="Connected to SDP")

            action.set_msg_to_remote(tm_adv)
            action.set_timer_action(Action.Timer(name="tm_adv_timer", timer_action=30000, echo_data=tm_adv))

        return action

    def process_sdp_disconnected(self, event) -> Action:
        """ Processes Science Data Processor disconnected events.
        """
        logger.debug(f"Digitiser disconnected from Science Data Processor: {event.remote_addr}")

        self.sdp_connected = False

        action = Action()

        if self.tm_connected:
            tm_adv = self._construct_adv_to_tm(property=tm_dig.PROPERTY_SDP_CONNECTED, value=False, message="Disconnected from SDP")

            action.set_msg_to_remote(tm_adv)
            action.set_timer_action(Action.Timer(name="tm_adv_timer", timer_action=30000, echo_data=tm_adv))

        return action

    def handle_field_set(self, api_call):
        """ Handles field set api calls.
                : returns: (status, message, value, payload)
        """
        prop_name = 'set_' + api_call['property']
        prop_value = api_call['value']

        if not hasattr(self.sdr, prop_name):
            return tm_dig.STATUS_ERROR, f"Digitiser property {prop_name} not found on SDR", None, None

        setter = getattr(self.sdr, prop_name)
        if callable(setter):
            setter(prop_value)
            return tm_dig.STATUS_SUCCESS, f"Digitiser set property {prop_name} to {prop_value}", prop_value, None
        else:
            return tm_dig.STATUS_ERROR, f"Digitiser property {prop_name} is not callable", None, None

    def handle_field_get(self, api_call):
        """ Handles field get api calls.
                : returns: (status, message, value, payload)
        """
        prop_name = 'get_' + api_call['property']

        if not hasattr(self.sdr, prop_name):
            return tm_dig.STATUS_ERROR, f"Digitiser property {prop_name} not found on SDR", None, None

        getter = getattr(self.sdr, prop_name)
        value = getter() if callable(getter) else getter

        return tm_dig.STATUS_SUCCESS, f"Digitiser {prop_name} value {value}", value, None

    def handle_method_call(self, api_call):
        """ Handles method api calls.
                : returns: (status, message, value, payload)
        """

        method = api_call['method']

        allowed_keys = {"num_samples", "num_bytes"}
        args = {k: v for k, v in api_call.get('params', {}).items() if k in allowed_keys}

        logger.debug(f"Digitiser method call: {method} with params {args}")

        try:
            method = getattr(self.sdr, method)
        except AttributeError:
            return tm_dig.STATUS_ERROR, f"Digitiser method {method} not found on SDR", None, None

        result = method(**args) if args is not None else method() if callable(method) else method

        return tm_dig.STATUS_SUCCESS, f"Digitiser method {method.__name__} invoked on SDR", None, result

    def _construct_adv_to_tm(self, property, value, message) -> APIMessage:
        """ Constructs an advice message to the Telescope Manager.
        """

        tm_adv = APIMessage(api_version=self.tm_api.get_api_version())

        tm_adv.set_json_api_header(
            api_version=self.tm_api.get_api_version(), 
            dt=datetime.datetime.now(timezone.utc), 
            from_system=self.app_name, 
            to_system="tm", 
            api_call={
                "msg_type": "adv", 
                "action_code": "set", 
                "property": property, 
                "value": value, 
                "message": message if message else ""
        })

        return tm_adv

    def _construct_adv_to_sdp(self, status, message,payload: bytes) -> APIMessage:
        """ Constructs an advice message to the Science Data Processor with the given sample payload.
        """

        sdp_adv = APIMessage(api_version=self.sdp_api.get_api_version(), payload=payload)

        sdp_adv.set_json_api_header(
            api_version=self.sdp_api.get_api_version(), 
            dt=datetime.datetime.now(timezone.utc), 
            from_system=self.app_name, 
            to_system="sdp", 
            api_call={}
        )
        
        metadata = [   
            {"property": "center_freq", "value": 1420.40e6},  # Hz
            {"property": "sample_rate", "value": 2.4e6},      # Hz
            {"property": "bandwidth", "value": 2.0e6},        # Hz
            {"property": "gain", "value": 40},                # dB
            {"property": "timestamp", "value": datetime.datetime.now(timezone.utc).isoformat()}
        ]   

        sdp_adv.set_api_call({
            "msg_type": "adv", 
            "action_code": "samples", 
            "status": status if status else "", 
            "message": message if message else "", 
            "metadata": metadata
        })

        return sdp_adv

    def _unpack_result(self, result):
        """ Unpacks the result of a method call.
        """
        if isinstance(result, tuple) and len(result) == 4:
            status, message, value, payload = result
        elif isinstance(result, tuple) and len(result) == 3:
            status, message, value, payload = result, None
        elif isinstance(result, tuple) and len(result) == 2:
            status, message, value, payload = result, None, None
        elif isinstance(result, tuple) and len(result) == 1:
            status, message, value, payload = result, None, None, None
        else:
            status, message, value, payload = tm_dig.STATUS_ERROR, "Invalid result format", None, None

        return status, message, value, payload

def main():
    digitiser = Digitiser()
    digitiser.start() 

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        digitiser.stop()

if __name__ == "__main__":
    main()
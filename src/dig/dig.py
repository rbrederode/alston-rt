import logging
import json
import numpy as np
import time
from datetime import datetime, timezone
from rtlsdr import RtlSdr

from api import tm_dig, sdp_dig
from env.app import App
from ipc.message import APIMessage
from ipc.message import AppMessage
from ipc.action import Action
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.comms import CommunicationStatus
from models.dsh import Feed
from models.health import HealthState
from sdr.sdr import SDR
from util import log
from util.xbase import XBase, XStreamUnableToExtract, XSoftwareFailure

logger = logging.getLogger(__name__)

MSG_TIMEOUT = 10000 # Timeout in milliseconds for messages awaiting acknowledgement

class Digitiser(App):

    def __init__(self, app_name: str = "dig"):

        super().__init__(app_name=app_name)

        # Telescope Manager interface
        self.tm_system = "tm"
        self.tm_api = tm_dig.TM_DIG()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), host=self.get_args().tm_host, port=self.get_args().tm_port)
        self.tm_endpoint.start()
        self.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint)

        # Science Data Processor Interface
        self.sdp_system = "sdp"
        self.sdp_api = sdp_dig.SDP_DIG()
        # Science Data Processor TCP Client
        self.sdp_endpoint = TCPClient(description=self.sdp_system, queue=self.get_queue(), host=self.get_args().sdp_host, port=self.get_args().sdp_port)
        self.sdp_endpoint.connect()
        self.sdp_connected = CommunicationStatus.NOT_ESTABLISHED
        # Register Science Data Processor interface with the App
        self.register_interface(self.sdp_system, self.sdp_api, self.sdp_endpoint)

        # Software Defined Radio (internal) interface
        self.sdr = SDR()
        self.feed = Feed.NONE
        self.stream_samples = False # Flag indicating if we are currently streaming samples (from the SDR)
        self.load_terminated = False # Flag indicating whether a 'load terminator' has been placed in the signal path

    def add_args(self, arg_parser): 
        """ Specifies the digitiser's command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--tm_host", type=str, required=False, help="TCP host to listen on for Telescope Manager commands", default="localhost")
        arg_parser.add_argument("--tm_port", type=int, required=False, help="TCP port to listen on for Telescope Manager commands", default=50000)
        
        arg_parser.add_argument("--sdp_host", type=str, required=False, help="TCP server host to connect to for downstream Science Data Processor transport",default="localhost")
        arg_parser.add_argument("--sdp_port", type=int, required=False, help="TCP server port to connect to for downstream Science Data Processor transport", default=60000)

    def process_init(self) -> Action:
        """ Processes initialisation events.
        """
        logger.debug(f"Digitiser initialisation event")

        action = Action()
        return action

    def process_tm_connected(self, event) -> Action:
        """ Processes Telescope Manager connected events.
        """
        logger.debug(f"Digitiser connected to Telescope Manager: {event.remote_addr}")

        self.tm_connected = CommunicationStatus.ESTABLISHED

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.debug(f"Digitiser disconnected from Telescope Manager: {event.remote_addr}")

        self.tm_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_tm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Telescope Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.debug(f"Digitiser received Telescope Manager message:\n{event}")

        if self.sdr is None or not self.sdr.get_connected() == CommunicationStatus.ESTABLISHED:
            status, message = tm_dig.STATUS_ERROR, "Digitiser not connected to SDR device"
            value, payload = None, None
            logger.warning("Digitiser not connected to SDR device.")
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
            "value": value if value and isinstance(value, (str, float, int)) else "",
        })

        action.set_msg_to_remote(tm_rsp)

        if api_call['action_code'] == 'method' and api_call['method'] in ("read_samples", "read_bytes"):

            self.stream_samples = True

            # Start reading samples immediately (timer_action=0) 
            # Two timers (1,2) run in parallel, reading samples one after the other, blocking only on the SDR
            for i in range(1, 3):
                action.set_timer_action(Action.Timer(name=f"stream_samples_{i}", timer_action=0))

            if self.sdp_connected == CommunicationStatus.ESTABLISHED and payload is not None:
                # Prepare adv msg to send samples to sdp
                sdp_adv = self._construct_adv_to_sdp(status, message, value, payload.tobytes())
                action.set_msg_to_remote(sdp_adv)
                action.set_timer_action(Action.Timer(name=f"sdp_adv_timer:{sdp_adv.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=sdp_adv))

            elif not self.sdp_connected == CommunicationStatus.ESTABLISHED:
                logger.warning("Digitiser cannot send samples to Science Data Processor, not connected.")

                tm_adv = self._construct_adv_to_tm(property=tm_dig.PROPERTY_SDP_CONNECTED, value=False, message="Disconnected from SDP")
                action.set_msg_to_remote(tm_adv)
                action.set_timer_action(Action.Timer(name=f"tm_adv_timer:{tm_adv.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=tm_adv))

            elif payload is None:
                # Wait for stream_samples timer to trigger again
                logger.warning("Digitiser cannot send samples to Science Data Processor, no payload.")
                
        return action

    def process_sdp_connected(self, event) -> Action:
        """ Processes Science Data Processor connected events.
        """
        logger.debug(f"Digitiser connected to Science Data Processor: {event.remote_addr}")

        self.sdp_connected = CommunicationStatus.ESTABLISHED

        action = Action()

        if self.tm_connected == CommunicationStatus.ESTABLISHED:
            # Send SDP connected adv to TM
            tm_adv = self._construct_adv_to_tm(property=tm_dig.PROPERTY_SDP_CONNECTED, value=True, message="Connected to SDP")
            action.set_msg_to_remote(tm_adv)
            action.set_timer_action(Action.Timer(name=f"tm_adv_timer:{tm_adv.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=tm_adv))

        return action

    def process_sdp_disconnected(self, event) -> Action:
        """ Processes Science Data Processor disconnected events.
        """
        logger.debug(f"Digitiser disconnected from Science Data Processor: {event.remote_addr}")

        self.sdp_connected = CommunicationStatus.NOT_ESTABLISHED

        action = Action()

        if not self.tm_connected == CommunicationStatus.ESTABLISHED:
            # Send SDP disconnected adv to TM
            tm_adv = self._construct_adv_to_tm(property=tm_dig.PROPERTY_SDP_CONNECTED, value=False, message="Disconnected from SDP")
            action.set_msg_to_remote(tm_adv)
            action.set_timer_action(Action.Timer(name=f"tm_adv_timer:{tm_adv.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=tm_adv))

        return action

    def process_sdp_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Science Data Processor service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.debug(f"Digitiser received sdp message:\n{event}")

        action = Action()

        # If we are processing a samples acknowledgement from the sdp, stop the associated timer
        if api_call['msg_type'] == 'rsp' and api_call['action_code'] == 'samples':
            dt = api_msg.get("timestamp")
            if dt:
                action.set_timer_action(Action.Timer(name=f"sdp_adv_timer:{dt}", timer_action=Action.Timer.TIMER_STOP, echo_data=api_msg))
            
            if api_call.get('status') == 'error':
                logger.error(f"Digitiser received negative acknowledgement from SDP for a samples advice.\n{api_msg}")

        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"Digitiser timer event: {event}")

        action = Action()

        if event.name.startswith("stream_samples"):
            result = self.handle_method_call({"method": "read_samples", "params": {}})
            status, message, value, payload = self._unpack_result(result)

            # If the digitiser is set to stream samples
            if self.stream_samples:
                # Start the same stream_samples timer immediately e.g. 'stream_samples_2'
                action.set_timer_action(Action.Timer(name=event.name, timer_action=0)) 

            if self.sdp_connected == CommunicationStatus.ESTABLISHED and payload is not None:
                # Prepare adv msg to send samples to sdp
                sdp_adv = self._construct_adv_to_sdp(status, message, value, payload.tobytes())
                action.set_msg_to_remote(sdp_adv)
                action.set_timer_action(Action.Timer(name=f"sdp_adv_timer:{sdp_adv.get_timestamp()}", timer_action=MSG_TIMEOUT, echo_data=sdp_adv))

            elif payload is None:
                # Wait for stream_samples timer to trigger again
                logger.warning("Digitiser cannot send samples to Science Data Processor, no payload.")
            
        elif event.name.startswith("sdp_adv_timer"):

            logger.warning(f"Digitiser timed out waiting for acknowledgement from SDP for samples advice {event}")

            if event.user_ref and isinstance(event.user_ref, APIMessage):
                try:
                    dt = event.user_ref.get_timestamp()
                    logger.debug(f"Digitiser stopping sdp_adv_timer for timestamp {dt}")
                    if dt:
                        action.set_timer_action(Action.Timer(name=f"sdp_adv_timer:{dt}", timer_action=Action.Timer.TIMER_STOP))
                except Exception as e:
                    logger.error(f"Digitiser - Error processing user_ref in event: {e}")

        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.tm_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.sdp_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        else:
            return HealthState.OK

    def set_feed(self, feed_id: int):
        """ Sets the current feed ID.
        """
        self.feed = feed_id
        logger.info(f"Digitiser feed set to {self.feed}")

    def handle_field_set(self, api_call):
        """ Handles field set api calls.
                : returns: (status, message, value, payload)
        """
        prop_name = 'set_' + api_call['property']
        prop_value = api_call['value']

        setter = getattr(self.sdr, prop_name) if hasattr(self.sdr, prop_name) else (getattr(self, prop_name) if hasattr(self, prop_name) else None)

        if setter and callable(setter):

            try:
                setter(prop_value)
            except Exception as e:
                logger.error(f"Digitiser failed to set property {prop_name}: {e}")
                return tm_dig.STATUS_ERROR, f"Digitiser failed to set property {prop_name}: {e}", None, None

            return tm_dig.STATUS_SUCCESS, f"Digitiser set property {prop_name} to {prop_value}", prop_value, None
        else:
            return tm_dig.STATUS_ERROR, f"Digitiser property {prop_name} is not callable", None, None

        return tm_dig.STATUS_ERROR, f"Digitiser property {prop_name} not found", None, None

    def handle_field_get(self, api_call):
        """ Handles field get api calls.
                : returns: (status, message, value, payload)
        """
        prop_name = 'get_' + api_call['property']

        getter = getattr(self.sdr, prop_name) if hasattr(self.sdr, prop_name) else (getattr(self, prop_name) if hasattr(self, prop_name) else None)
        
        if getter:

            try:
                value = getter()
            except XSoftwareFailure as e:
                logger.error(f"Digitiser failed to get property {prop_name}: {e}")
                return tm_dig.STATUS_ERROR, f"Digitiser failed to get property {prop_name}: {e}", None, None

            value = getter() if callable(getter) else getter
            return tm_dig.STATUS_SUCCESS, f"Digitiser {prop_name} value {value}", value, None
        else:
            return tm_dig.STATUS_ERROR, f"Digitiser property {prop_name} not found", None, None

    def handle_method_call(self, api_call):
        """ Handles method api calls.
                : returns: (status, message, value, payload)
        """
        method = api_call.get('method', None)

        allowed_keys = {"sample_rate", "time_in_secs"}
        args = {k: v for k, v in api_call.get('params', {}).items() if k in allowed_keys}

        logger.debug(f"Digitiser method call: {method} with params {args}")

        call = getattr(self.sdr, method) if hasattr(self.sdr, method) else (getattr(self, method) if hasattr(self, method) else None)

        if call:

            try:
                result = call(**args) if args is not None else call() if callable(call) else call
            except XSoftwareFailure as e:
                logger.error(f"Digitiser method {method} failed with exception: {e}")
                return tm_dig.STATUS_ERROR, f"Digitiser method {method} failed with exception: {e}", None, None

            # Check whether result is a tuple of (value, payload) or just a value
            if isinstance(result, tuple):
                return tm_dig.STATUS_SUCCESS, f"Digitiser method {call.__name__} invoked on SDR", result[0], result[1]
            else:
                return tm_dig.STATUS_SUCCESS, f"Digitiser method {call.__name__} invoked on SDR", result, None
        else:
            return tm_dig.STATUS_ERROR, f"Digitiser method {method} not found", None, None

    def _construct_adv_to_tm(self, property, value, message) -> APIMessage:
        """ Constructs an advice message to the Telescope Manager.
        """

        tm_adv = APIMessage(api_version=self.tm_api.get_api_version())

        tm_adv.set_json_api_header(
            api_version=self.tm_api.get_api_version(), 
            dt=datetime.now(timezone.utc), 
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

    def _construct_adv_to_sdp(self, status, message, value, payload: bytes) -> APIMessage:
        """ Constructs an advice message to the Science Data Processor with the given sample payload.
        """

        # Extract sample metadata from the value dictionary
        read_counter = value.get('read_counter', 0)
        num_samples = value.get('num_samples', 0)
        read_start = value.get('read_start', 0)
        read_end = value.get('read_end', 0)

        sdp_adv = APIMessage(api_version=self.sdp_api.get_api_version(), payload=payload)

        sdp_adv.set_json_api_header(
            api_version=self.sdp_api.get_api_version(), 
            dt=datetime.now(timezone.utc), 
            from_system=self.app_name, 
            to_system="sdp", 
            api_call={}
        )
        
        metadata = [   
            {"property": "feed", "value": self.feed},                    # Feed Id
            {"property": "center_freq", "value": self.sdr.center_freq},  # Hz    
            {"property": "sample_rate", "value": self.sdr.sample_rate},  # Hz
            {"property": "bandwidth", "value": self.sdr.bandwidth},      # MHz
            {"property": "gain", "value": self.sdr.gain},                # dB
            {"property": "read_counter", "value": read_counter},
            {"property": "read_start", "value": datetime.fromtimestamp(read_start, timezone.utc).isoformat()},
            {"property": "read_end", "value": datetime.fromtimestamp(read_end, timezone.utc).isoformat()},
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
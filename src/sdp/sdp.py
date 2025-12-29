import logging
import json
from queue import Queue
import numpy as np
from datetime import datetime, timezone
import time
import threading

from api import sdp_dig, tm_sdp
from env.app import App
from ipc.message import APIMessage
from ipc.action import Action
from ipc.message import AppMessage
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.base import BaseModel
from models.comms import CommunicationStatus, InterfaceType
from models.health import HealthState
from models.scan import ScanModel, ScanState
from models.sdp import ScienceDataProcessorModel
from obs.scan import Scan
from signal_display import SignalDisplay
from util import log, util
from util.xbase import XBase, XStreamUnableToExtract

#OUTPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Home'  # Directory to store samples
OUTPUT_DIR = '/Users/r.brederode/samples'  # Directory to store samples

logger = logging.getLogger(__name__)

class SDP(App):

    sdp_model = ScienceDataProcessorModel(sdp_id="sdp001")

    def __init__(self, app_name: str = "sdp"):

        super().__init__(app_name=app_name, app_model = self.sdp_model.app)

        # Telescope Manager interface (TBD)
        self.tm_system = "tm"
        self.tm_api = tm_sdp.TM_SDP()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), host=self.get_args().tm_host, port=self.get_args().tm_port)
        self.tm_endpoint.start()
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint, InterfaceType.APP_APP)
        # Set initial Telescope Manager connection status
        self.sdp_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        
        # Digitiser interface
        self.dig_system = "dig"
        self.dig_api = sdp_dig.SDP_DIG()
        # Digitiser TCP Server
        self.dig_endpoint = TCPServer(description=self.dig_system, queue=self.get_queue(), host=self.get_args().dig_host, port=self.get_args().dig_port)
        self.dig_endpoint.start()
        # Register Digitiser interface with the App
        self.register_interface(self.dig_system, self.dig_api, self.dig_endpoint, InterfaceType.ENTITY_DRIVER)
        # Entity drivers maintain comms status per entity, so no need to initialise comms status here

        # Queue to hold scans to be processed
        self.scan_q = Queue()            # Queue of scans to load and process
        self._rlock = threading.RLock()  # Lock for thread-safe access to shared resources
        self.capture = True              # Flag to enable/disable sample capture

        self.signal_displays = {}        # Dictionary to hold SignalDisplay objects for each digitiser

    def add_args(self, arg_parser): 
        """ Specifies the science data processors command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--tm_host", type=str, required=False, help="TCP host to listen on for Telescope Manager commands", default="localhost")
        arg_parser.add_argument("--tm_port", type=int, required=False, help="TCP port for Telescope Manager commands", default=50001)

        arg_parser.add_argument("--dig_host", type=str, required=False, help="TCP host to listen on for Digitiser commands", default="localhost")
        arg_parser.add_argument("--dig_port", type=int, required=False, help="TCP server port for upstream Digitiser transport", default=60000)

        arg_parser.add_argument("--output_dir", type=str, required=False, help="Directory to store captured samples", default=OUTPUT_DIR)

    def process_init(self) -> Action:
        """ Processes initialisation events.
        """
        logger.debug(f"SDP initialisation event")

        # Load Digitiser configuration from disk
        # Config file is located in ./config/<profile>/<model>.json
        # Config file defines initial list of digitisers to be processed by the SDP
        input_dir = f"./config/{self.get_args().profile}"
        dig_store = self.sdp_model.dig_store.load_from_disk(input_dir=input_dir, filename="DigitiserList.json")

        if dig_store is not None:
            self.sdp_model.dig_store = dig_store
            logger.info(f"Science Data Processor loaded Digitiser configuration from {input_dir}")
        else:
            logger.warning(f"Science Data Processor could not load Digitiser configuration from {input_dir}")

        action = Action()
        return action

    def get_dig_entity(self, event) -> (str, BaseModel):
        """ Determines the digitiser entity ID based on the remote address of a ConnectEvent, DisconnectEvent, or DataEvent.
            Returns a tuple of the entity ID and entity if found, else None, None.
        """
        logger.debug(f"Finding digitiser entity ID for remote address: {event.remote_addr[0]}")

        for digitiser in self.sdp_model.dig_store.dig_list:

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
        logger.info(f"Science Data Processor connected to Digitiser entity on {event.remote_addr}\n{entity}")
        
        digitiser: DigitiserModel = entity
        digitiser.sdp_connected = CommunicationStatus.ESTABLISHED

        action = Action()
        return action

    def process_dig_entity_disconnected(self, event, entity) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Science Data Processor disconnected from Digitiser entity on {event.remote_addr}\n{entity}")

        digitiser: DigitiserModel = entity
        digitiser.sdp_connected = CommunicationStatus.NOT_ESTABLISHED

        action = Action()
        return action
        
    def process_dig_entity_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray, entity: BaseModel) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Science Data Processor received digitiser {api_call['msg_type']} msg with action code: {api_call['action_code']} on entity: {api_msg['entity']}")

        digitiser: DigitiserModel = entity

        status, message = sdp_dig.STATUS_SUCCESS, "Acknowledged"

        action = Action()

        # If we are receiving samples from the digitiser, load them into the current scan
        if api_call['action_code'] == sdp_dig.ACTION_CODE_SAMPLES:

            # Extract metadata from the api call
            metadata = api_call.get('metadata', [])

            # Default values
            center_freq = gain = sample_rate = read_counter = read_start = read_end = loaddig_id = None
            
            # Extract sample metadata fields
            for item in metadata or []:
                prop = item.get("property")
                value = item.get("value")
                if prop == sdp_dig.PROPERTY_CENTER_FREQ:
                    center_freq = value
                elif prop == sdp_dig.PROPERTY_SAMPLE_RATE:
                    sample_rate = value
                elif prop == sdp_dig.PROPERTY_READ_COUNTER:
                    read_counter = value
                elif prop == sdp_dig.PROPERTY_SDR_GAIN:
                    gain = value
                elif prop == sdp_dig.PROPERTY_READ_START:
                    read_start = datetime.fromisoformat(value)
                elif prop == sdp_dig.PROPERTY_READ_END:
                    read_end = datetime.fromisoformat(value)
                elif prop == sdp_dig.PROPERTY_LOAD:
                    load = value
                elif prop == sdp_dig.PROPERTY_DIG_ID:
                    dig_id = value

            logger.info(f"SDP received digitiser samples message with metadata: digitiser={dig_id}, center_freq={center_freq}, gain={gain}, sample rate={sample_rate}, read counter={read_counter}, load={load}")

            with self._rlock:                    
                # Loop through scans in the queue until we find one that matches the read_counter
                # This handles situations where samples are received out of order
                match = None

                pending_scans = [s for s in list(self.scan_q.queue) if s.get_status() in [ScanState.EMPTY, ScanState.WIP] and s.get_dig_id() == dig_id]
                abort_scans = []

                for scan in pending_scans:
                    start_idx, end_idx = scan.get_start_end_idx()
                    if read_counter >= start_idx and read_counter <= end_idx:

                        # Verify that the digitiser metadata still matches the scan parameters
                        if scan.scan_model.center_freq == center_freq and scan.scan_model.sample_rate == sample_rate and \
                            scan.scan_model.load == load and scan.scan_model.channels == self.sdp_model.channels and \
                            scan.scan_model.duration == self.sdp_model.scan_duration:
                            match = scan
                            break
                        else:
                            logger.warning(f"SDP aborting scan:scan parameters no longer match digitiser sample metadata for scan id {scan.scan_model.scan_id}: "
                                           f"scan(center_freq={scan.scan_model.center_freq}) vs "
                                           f"metadata(center_freq={center_freq})")
                            abort_scans.append(scan)

                    # If the Digitiser read_counter is greater than the scan range by the scan duration or the digitiser has reset itself, abort the scan
                    elif read_counter > end_idx + scan.scan_model.duration or read_counter < start_idx:
                        abort_scans.append(scan)  # add it to the abort list
                        logger.warning(f"SDP aborting scan id: {scan.scan_model.scan_id}, digitiser read_counter {read_counter} has moved beyond this scan index ({end_idx})")

                # If we found scans to abort, do so
                for scan in abort_scans:
                    self._abort_scan(scan)

                # If no matching scan was found, create a new scan
                if match is None:

                    scan_model = ScanModel(
                        dig_id=dig_id if dig_id is not None else "<undefined>",
                        start_idx=read_counter if read_counter is not None else 0,
                        duration=self.sdp_model.scan_duration,
                        sample_rate=sample_rate if sample_rate is not None else 0,
                        channels=self.sdp_model.channels,
                        center_freq=center_freq if center_freq is not None else 0,
                        gain=gain if gain is not None else 0,
                        load=load if load is not None else False)

                    scan = Scan(scan_model=scan_model)
                    self.scan_q.put(scan)
                    self.sdp_model.scans_created += 1
                    match = scan

                    logger.debug(f"SDP created new scan: {scan}")

            if match is not None:              
                logger.debug(f"SDP loading samples into scan: {match}")

                self.sdp_model.add_scan(match.scan_model)

                # Convert payload to complex64 numpy array
                iq_samples = np.frombuffer(payload, dtype=np.complex64)
              
                if match.load_samples(sec=(read_counter - match.get_start_end_idx()[0] + 1), 
                        iq=iq_samples,
                        read_start=read_start,
                        read_end=read_end):
                    status = sdp_dig.STATUS_SUCCESS
                    message = f"SDP loaded samples into scan id: {match.scan_model.scan_id}"
                else:
                    status = sdp_dig.STATUS_ERROR
                    message = f"SDP failed to load samples into scan id: {match.scan_model.scan_id}"

                    if match.scan_model.load_failures >= 3:
                        logger.error(f"SDP aborting scan id: {match.scan_model.scan_id} has exceeded maximum load failures: {match.scan_model.load_failures}")
                        self._abort_scan(match)

                if match.get_status() == ScanState.COMPLETE:
                    logger.info(f"SDP scan is complete: {match}")
                    self._complete_scan(match)
                    
        # Prepare rsp msg to dig acknowledging receipt of the incoming api message
        dig_rsp = APIMessage(api_msg=api_msg, api_version=self.dig_api.get_api_version())
        dig_rsp.switch_from_to()
        dig_rsp.set_api_call({
            "msg_type": "rsp", 
            "action_code": api_call['action_code'], 
            "status": status, 
            "message": message,
        })

        action.set_msg_to_remote(dig_rsp)

        return action

    def process_tm_connected(self, event) -> Action:
        """ Processes Telescope Manager connected events.
        """
        logger.info(f"Science Data Processor connected to Telescope Manager: {event.remote_addr}")

        self.sdp_model.tm_connected = CommunicationStatus.ESTABLISHED
        
        action = Action()
        
        # Send initial status advice message to Telescope Manager
        tm_adv = self._construct_status_adv_to_tm()
        action.set_msg_to_remote(tm_adv)
        return action

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.info(f"Science Data Processor disconnected from Telescope Manager: {event.remote_addr}")

        self.sdp_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_tm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Telescope Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Science Data Processor received Telescope Manager {api_call['msg_type']} message with action code: {api_call['action_code']}")

        action = Action()

         # If api call is a rsp msg, check whether it was successful
        if api_call['msg_type'] == 'rsp':
            # Stop the corresponding timer if applicable
            dt = api_msg.get("timestamp")
            if dt:
                action.set_timer_action(Action.Timer(name=f"tm_adv_timer:{dt}", timer_action=Action.Timer.TIMER_STOP, echo_data=api_msg))
            if api_call.get('status') == 'error':
                logger.error(f"Science Data Processor received negative acknowledgement from TM for api call\n{json.dumps(api_call, indent=2)}")
            return Action()

        # Dispatch the API Call to a handler method
        dispatch = {
            "set": self.handle_field_set,
            "get": self.handle_field_get,
        }

        # Invoke handler method to process the api call
        result = dispatch.get(api_call['action_code'], lambda x: None)(api_call)
        status, message, value, payload = util.unpack_result(result)
              
        # Prepare rsp msg to tm containing result of initial api call
        tm_rsp = APIMessage(api_msg=api_msg, api_version=self.tm_api.get_api_version())
        tm_rsp.switch_from_to()
        tm_rsp_api_call = {
            "msg_type": "rsp", 
            "action_code": api_call['action_code'], 
            "status": status, 
        }
        if api_call.get('property') is not None:
            tm_rsp_api_call["property"] = api_call['property']
        if value is not None:
            tm_rsp_api_call["value"] = value 
        if message is not None:
            tm_rsp_api_call["message"] = message

        tm_rsp.set_api_call(tm_rsp_api_call)       
        action.set_msg_to_remote(tm_rsp)
        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"SDP timer event: {event}")

        action = Action()
        return action

    def process_status_event(self, event) -> Action:
        """ Processes status update events.
        """
        self.get_app_processor_state()

        action = Action()

        # If connected to Telescope Manager, send status advice message
        if self.sdp_model.tm_connected == CommunicationStatus.ESTABLISHED:
            tm_adv = self._construct_status_adv_to_tm()
            action.set_msg_to_remote(tm_adv)

        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.sdp_model.tm_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.FAILED
        elif any(dig.tm_connected != CommunicationStatus.ESTABLISHED for dig in self.sdp_model.dig_store.dig_list):
            return HealthState.DEGRADED
        else:
            return HealthState.OK

    def handle_field_get(self, api_call):
        """ Handles field get api calls.
                : returns: (status, message, value, payload)
        """
        prop_name = api_call['property']

        try:
            if prop_name in self.sdp_model.schema.schema:
                value = getattr(self.sdp_model, prop_name)
                logger.info(f"Science Data Processor {prop_name} get to {value}")
            else:
                logger.error(f"Science Data Processor unknown property {prop_name} get request")
                return tm_sdp.STATUS_ERROR, f"Science Data Processor unknown property {prop_name}", None, None
        
        except XSoftwareFailure as e:
            logger.exception(f"Science Data Processor failed to get property {prop_name}: {e}")
            return tm_sdp.STATUS_ERROR, f"Science Data Processor failed to get property {prop_name}: {e}", None, None

        return tm_sdp.STATUS_SUCCESS, f"Science Data Processor get property {prop_name} value {value}", value, None

    def handle_field_set(self, api_call):
        """ Handles field set api calls.
                : returns: (status, message, value, payload)
        """
        prop_name = api_call['property']
        prop_value = api_call['value']

        try:
            if prop_name in self.sdp_model.schema.schema:
                setattr(self.sdp_model, prop_name, prop_value)
                logger.info(f"Science Data Processor {prop_name} set to {prop_value}")
            else:
                logger.error(f"Science Data Processor unknown property {prop_name} with value {prop_value}")
                return tm_sdp.STATUS_ERROR, f"Science Data Processor unknown property {prop_name}", None, None
        
        except XSoftwareFailure as e:
            logger.exception(f"Science Data Processor failed to set property {prop_name} to {prop_value}: {e}")
            return tm_sdp.STATUS_ERROR, f"Science Data Processor failed to set property {prop_name} to {prop_value}: {e}", None, None

        return tm_sdp.STATUS_SUCCESS, f"Science Data Processor set property {prop_name} to {prop_value}", prop_value, None

    def _construct_status_adv_to_tm(self) -> APIMessage:
        """ Constructs a status advice message for the Telescope Manager.
        """
        tm_adv = APIMessage(api_version=self.tm_api.get_api_version())
        tm_adv.set_json_api_header(
            api_version=self.tm_api.get_api_version(), 
            dt=datetime.now(timezone.utc), 
            from_system=self.sdp_model.app.app_name, 
            to_system="tm", 
            api_call={
                "msg_type": "adv", 
                "action_code": "set", 
                "property": tm_sdp.PROPERTY_STATUS, 
                "value": self.sdp_model.to_dict(), 
                "message": "SDP status update"
            })
        return tm_adv

    def _abort_scan(self, scan: Scan):
        """ Aborts a scan and performs necessary cleanup.
        """
        scan.set_status(ScanState.ABORTED)
        scan.save_to_disk(output_dir=self.get_args().output_dir, include_iq=False)
        scan.del_iq()

        self.sdp_model.scans_aborted += 1

        # Remove this specific scan from the underlying queue 
        try:
            self.scan_q.queue.remove(scan)
            # Balance the unfinished task count for the removed item
            self.scan_q.task_done()
        except ValueError:
            logger.warning(f"Attempted to remove scan {scan} from queue but it was not found")

    def _complete_scan(self, scan: Scan):
        """ Completes a scan and performs necessary cleanup.
        """
        scan.save_to_disk(output_dir=self.get_args().output_dir, include_iq=False)
        scan.del_iq()

        self.sdp_model.scans_completed += 1

        # Remove this specific scan from the underlying queue 
        try:
            self.scan_q.queue.remove(scan)
            # Balance the unfinished task count for the removed item
            self.scan_q.task_done()
        except ValueError:
            logger.warning(f"Attempted to remove scan {scan} from queue but it was not found")

def main():
    sdp = SDP()
    sdp.start()

    try:
        while True:
            # If there is a scan in the queue, process it, else sleep & continue 
            try:
                scan = sdp.scan_q.queue[0]
                if scan.get_status() == ScanState.EMPTY:
                    raise IndexError  # No samples loaded yet, skip processing
            except IndexError:
                time.sleep(0.1)
                continue

            # If there is no signal display for this digitiser, create one
            dig_id = scan.scan_model.dig_id
            if dig_id not in sdp.signal_displays:
                sdp.signal_displays[dig_id] = SignalDisplay(dig_id=dig_id)

            sdp.signal_displays[dig_id].set_scan(scan)

            # Call display while the scan is being loaded 
            # Scan will transition to "Complete" or "Aborted" when done
            # This allows for interactive display of the scan as samples are loaded
            while scan.get_status() == ScanState.WIP:
                sdp.signal_displays[dig_id].display()
                time.sleep(1)  # Update display every second
            
            # Final display call to ensure complete rendering
            sdp.signal_displays[dig_id].display()
            sdp.signal_displays[dig_id].save_scan_figure(output_dir=sdp.get_args().output_dir)
                
    except KeyboardInterrupt:
        pass
    finally:
        sdp.stop()

if __name__ == "__main__":
    main()
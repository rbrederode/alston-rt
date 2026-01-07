import logging
import json
from queue import Queue
import numpy as np
from datetime import datetime, timezone
import re
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
        logger.debug(f"Science Data Processor finding digitiser entity ID for remote address: {event.remote_addr[0]}")

        for digitiser in self.sdp_model.dig_store.dig_list:

            if isinstance(digitiser.app.arguments, dict) and "local_host" in digitiser.app.arguments:

                if digitiser.app.arguments["local_host"] == event.remote_addr[0]:
                    logger.info(f"Science Data Processor found digitiser entity ID: {digitiser.dig_id} for remote address: {event.remote_addr}")
                    return digitiser.dig_id, digitiser
            else:
                logger.warning(f"SDP found {digitiser.dig_id} not configured with valid local_host argument matching against remote address: {event.remote_addr[0]}")

        return None, None

    def process_dig_entity_connected(self, event, entity:BaseModel) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Science Data Processor connected to Digitiser entity on {event.remote_addr}\n{entity}")
        
        digitiser: DigitiserModel = entity
        digitiser.sdp_connected = CommunicationStatus.ESTABLISHED

        action = Action()
        return action

    def process_dig_entity_disconnected(self, event, entity: BaseModel) -> Action:
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

            # Extract metadata from the api call and convert to dictionary for direct access
            metadata = api_call.get('metadata', [])
            meta_dict = {item.get("property"): item.get("value") for item in metadata or []}

            center_freq   = meta_dict.get(sdp_dig.PROPERTY_CENTER_FREQ, 0.0)
            sample_rate   = meta_dict.get(sdp_dig.PROPERTY_SAMPLE_RATE, 0.0)
            read_counter  = meta_dict.get(sdp_dig.PROPERTY_READ_COUNTER, 0)
            gain          = meta_dict.get(sdp_dig.PROPERTY_SDR_GAIN, 0.0)
            read_start    = datetime.fromisoformat(meta_dict.get(sdp_dig.PROPERTY_READ_START))
            read_end      = datetime.fromisoformat(meta_dict.get(sdp_dig.PROPERTY_READ_END))
            load          = meta_dict.get(sdp_dig.PROPERTY_LOAD, False)
            dig_id        = meta_dict.get(sdp_dig.PROPERTY_DIG_ID, "<undefined>")
            channels      = meta_dict.get(sdp_dig.PROPERTY_CHANNELS,1024)               # default to 1024 channels
            scan_duration = meta_dict.get(sdp_dig.PROPERTY_SCAN_DURATION,60)            # default to 60 seconds scan duration
            obs_id        = meta_dict.get(sdp_dig.PROPERTY_OBS_ID, "<undefined>")       # default to "<undefined>" observation id
            tgt_index     = meta_dict.get(sdp_dig.PROPERTY_TGT_INDEX, -1)               # default to -1 target index (within an observation)
            freq_scan     = meta_dict.get(sdp_dig.PROPERTY_FREQ_SCAN, -1)               # default to -1 frequency scan index (within a target)
            scan_iter     = meta_dict.get(sdp_dig.PROPERTY_SCAN_ITER, -1)               # default to -1 scan iteration index

            logger.info(f"SDP received digitiser samples message with metadata:\n{metadata}")

            with self._rlock:                    
                match = None

                # Find pending scans for this digitiser
                pending_scans = [s for s in list(self.scan_q.queue) if s.get_status() in [ScanState.EMPTY, ScanState.WIP] and s.get_dig_id() == dig_id]
                abort_scans = []

                for scan in pending_scans:
                    start_idx, end_idx = scan.get_start_end_idx()
                    if read_counter >= start_idx and read_counter <= end_idx:

                        # Verify that the digitiser metadata still matches the scan parameters
                        if scan.scan_model.center_freq == center_freq and scan.scan_model.sample_rate == sample_rate and scan.scan_model.load == load and \
                                scan.scan_model.channels == channels and scan.scan_model.duration == scan_duration and scan.scan_model.obs_id == obs_id and \
                                scan.scan_model.tgt_index == tgt_index and scan.scan_model.freq_scan == freq_scan and scan.scan_model.scan_iter == scan_iter:
                            match = scan
                            break
                        else:
                            logger.warning(f"SDP aborting scan id {scan.scan_model.scan_id}: scan parameters no longer match digitiser metadata {metadata} vs scan:\n" + \
                                f"{scan}")
                            abort_scans.append(scan)

                    # If the Digitiser read_counter is greater than the scan range by the scan duration or the digitiser has reset itself, abort the scan
                    elif read_counter > end_idx + scan.scan_model.duration or read_counter < start_idx:
                        abort_scans.append(scan)  # add it to the abort list
                        logger.warning(f"SDP aborting scan id: {scan.scan_model.scan_id}, dig read_counter {read_counter} not consistent with scan indexes {start_idx}-{end_idx}")

                # If we found scans to abort, do so
                for scan in abort_scans:
                    self._abort_scan(scan)

                # If no matching scan was found
                if match is None:

                    # Create a new scan model based on the digitiser metadata
                    scan_model = ScanModel(
                        dig_id=dig_id,
                        obs_id=obs_id,
                        tgt_index=tgt_index,
                        freq_scan=freq_scan,
                        scan_iter=scan_iter,
                        start_idx=read_counter,
                        duration=scan_duration,
                        sample_rate=sample_rate,
                        channels=channels,
                        center_freq=center_freq,
                        gain=gain,
                        load=load)

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

                    if self.sdp_model.tm_connected == CommunicationStatus.ESTABLISHED:

                        # Send scan complete advice to Telescope Manager
                        tm_adv = self._construct_scan_complete_adv_to_tm(match)
                        action.set_msg_to_remote(tm_adv)
                        action.set_timer_action(Action.Timer(name=f"tm_adv_timer_retry:{tm_adv.get_timestamp()}", timer_action=self.sdp_model.app.msg_timeout_ms, echo_data=tm_adv))
                    
        dig_rsp = self._construct_rsp_to_dig(status, message, api_msg, api_call)
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

         # If api call is a rsp msg from the TM
        if api_call['msg_type'] == 'rsp':

            # Stop the corresponding adv timer if applicable
            dt = api_msg.get("timestamp")
            if dt:
                action.set_timer_action(Action.Timer(name=f"tm_adv_timer_retry:{dt}", timer_action=Action.Timer.TIMER_STOP))
                action.set_timer_action(Action.Timer(name=f"tm_adv_timer_final:{dt}", timer_action=Action.Timer.TIMER_STOP))
            
            if api_call.get('status') == tm_sdp.STATUS_ERROR:
                logger.error(f"Science Data Processor received negative acknowledgement from TM for api call\n{json.dumps(api_call, indent=2)}")

            return action

        # Else if api call is a req or adv msg from the TM
        elif api_call['msg_type'] in ['req', 'adv']:

            # Dispatch the API Call to a handler method
            dispatch = {
                "set": self.handle_field_set,
                "get": self.handle_field_get,
            }

            # Invoke set or get handler to process the api call
            result = dispatch.get(api_call['action_code'], lambda x: None)(api_call)
            status, message, value, payload = util.unpack_result(result)
              
        tm_rsp = self._construct_rsp_to_tm(status, message, value, api_msg, api_call)
        action.set_msg_to_remote(tm_rsp)

        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"SDP timer event: {event}")

        action = Action()

        # Handle an initial request msg timer retry e.g. sdp_adv_timer_retry:<timestamp> 
        if "adv_timer_retry" in event.name:
            
            logger.warning(f"Science Data Processor timed out waiting for response msg {event.name}, retrying advice msg")

            # Resend the API advice if the timer user_ref is set (containing the original advice message)
            if event.user_ref is not None:

                adv_msg: APIMessage = event.user_ref
                final_timer = re.sub(r':.*$', f':{adv_msg.get_timestamp()}', event.name.replace("retry", "final"))

                action.set_msg_to_remote(adv_msg)
                action.set_timer_action(Action.Timer(
                    name=final_timer, 
                    timer_action=self.sdp_model.app.msg_timeout_ms,
                    echo_data=adv_msg))

        # Handle a final request msg timer e.g. dig002_req_timer_final:<timestamp> or sdp002_req_timer_final:<timestamp>
        elif "adv_timer_final" in event.name:
            
            logger.warning(f"Science Data Processor timed out waiting for response msg after final retry, aborting retries for {event.name}")

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
            return HealthState.DEGRADED
        elif any(dig.tm_connected != CommunicationStatus.ESTABLISHED for dig in self.sdp_model.dig_store.dig_list):
            return HealthState.DEGRADED
        else:
            return HealthState.OK

    def set_signal_display(self, value:dict) -> bool:
        """ Sets the signal display state for a digitiser.
            value is a dictionary with keys 'dig_id' and 'active' (boolean)
        """
        dig_id = value.get('dig_id')
        active = value.get('active', False)

        if dig_id is None:
            logger.error(f"Science Data Processor signal display request missing dig_id in value: {value}")
            return False

        if dig_id not in self.signal_displays:
            self.signal_displays[dig_id] = SignalDisplay(dig_id=dig_id)

        signal_display = self.signal_displays[dig_id]
        signal_display.set_is_active(active)
        logger.info(f"Science Data Processor set signal display for digitiser {dig_id} to active={active}")

        return True

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
            # Else if the SDP has a callable attribute matching the property name
            elif hasattr(self, "set_" + prop_name) and callable(getattr(self, "set_" + prop_name)):
                method = getattr(self, "set_" + prop_name)
                result = method(prop_value) if prop_value is not None else method()
                if result is True:
                    return tm_sdp.STATUS_SUCCESS, f"Science Data Processor set property {prop_name} to {prop_value}", prop_value, None
                else:
                    return tm_sdp.STATUS_ERROR, f"Science Data Processor failed to set property {prop_name} to {prop_value}", None, None
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

    def _construct_scan_complete_adv_to_tm(self, match) -> APIMessage:
        """ Constructs a scan complete advice message for the Telescope Manager.
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
                "property": tm_sdp.PROPERTY_SCAN_COMPLETE, 
                "value": match.scan_model.to_dict(), 
                "message": "SDP scan complete"
            })
        return tm_adv

    def _construct_rsp_to_dig(self, status: int, message: str, api_msg: dict, api_call: dict) -> APIMessage:
        """ Constructs a Digitiser response APIMessage.
        """
        # Prepare rsp msg to dig acknowledging receipt of the incoming api message
        dig_rsp = APIMessage(api_msg=api_msg, api_version=self.dig_api.get_api_version())
        dig_rsp.switch_from_to()
        dig_rsp.set_api_call({
            "msg_type": "rsp", 
            "action_code": api_call['action_code'], 
            "status": status, 
            "message": message,
        })       
        return dig_rsp

    def _construct_rsp_to_tm(self, status: int, message: str, value: any, api_msg: dict, api_call: dict) -> APIMessage:
        """ Constructs a Telescope Manager response APIMessage.
        """
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
        return tm_rsp

    def _abort_scan(self, scan: Scan):
        """ Aborts a scan and performs necessary cleanup.
        """
        scan.set_status(ScanState.ABORTED)
        scan.del_iq()

        self.sdp_model.scans_aborted += 1

        # Remove this specific scan from the underlying queue 
        try:
            self.scan_q.queue.remove(scan)
            # Balance the unfinished task count for the removed item
            self.scan_q.task_done()
        except ValueError:
            logger.warning(f"Science Data Processor while aborting attempted to remove scan {scan} from queue but it was not found")

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
            logger.warning(f"Science Data Processor while completing attempted to remove scan {scan} from queue but it was not found")

def main():
    sdp = SDP()
    sdp.start()

    try:
        while True:

            # If there are no scans to process, sleep and continue
            if sdp.scan_q.qsize() == 0:
                time.sleep(0.1)
                continue

            # For each scan in the queue, update its signal display if applicable
            for i in range(sdp.scan_q.qsize()):

                try:
                    scan = sdp.scan_q.queue[i]
                except IndexError:
                    continue  # Scan index not valid, continue to next scan

                if scan.get_status() == ScanState.EMPTY:
                    continue  # Scan is empty, nothing to display, continue to next scan

                dig_id = scan.get_dig_id()

                if scan.scan_model.scan_iter == 1:
                    sdp.set_signal_display({'dig_id': 'dig001', 'active': False})

                # If there is no signal display for this digitiser, create one
                if dig_id not in sdp.signal_displays:
                    sdp.signal_displays[dig_id] = SignalDisplay(dig_id=dig_id)

                if sdp.signal_displays[dig_id] is None or sdp.signal_displays[dig_id].is_active is False:
                    continue  # No signal display or not active, continue to next scan
                
                # If the signal display is not set to the current scan
                if sdp.signal_displays[dig_id].get_scan() != scan:

                    current_scan = sdp.signal_displays[dig_id].get_scan()

                    # If the previously displayed scan completed, display and save its figure for posterities sake
                    if current_scan and current_scan.get_status() == ScanState.COMPLETE:
                        sdp.signal_displays[dig_id].display()
                        sdp.signal_displays[dig_id].save_scan_figure(output_dir=sdp.get_args().output_dir)

                    sdp.signal_displays[dig_id].set_scan(scan)
                
                # Update the signal display for this scan
                sdp.signal_displays[dig_id].display()

            time.sleep(1)  # Update displays every second                
                
    except KeyboardInterrupt:
        pass
    finally:
        sdp.stop()

if __name__ == "__main__":
    main()
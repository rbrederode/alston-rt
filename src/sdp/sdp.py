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
from models.comms import CommunicationStatus, InterfaceType
from models.dsh import Feed
from models.health import HealthState
from models.scan import ScanModel, ScanState
from models.sdp import ScienceDataProcessorModel
from obs.scan import Scan
from signal_display import SignalDisplay
from util import log
from util.xbase import XBase, XStreamUnableToExtract

#OUTPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Home'  # Directory to store samples
OUTPUT_DIR = '/Users/r.brederode/samples'  # Directory to store samples

logger = logging.getLogger(__name__)

class SDP(App):

    sdp_model = ScienceDataProcessorModel(id="sdp001")

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
        # Set initial Digitiser connection status
        self.sdp_model.dig_connected = CommunicationStatus.NOT_ESTABLISHED

        # Queue to hold scans to be processed
        self.scan_q = Queue()            # Queue of scans to load and process
        self._rlock = threading.RLock()  # Lock for thread-safe access to shared resources
        self.capture = True              # Flag to enable/disable sample capture

        self.signal_display = SignalDisplay()  # Signal display object (optional, for interactive display)

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

        action = Action()
        return action

    def process_dig_connected(self, event) -> Action:
        """ Processes Digitiser connected events.
        """
        logger.info(f"Science Data Processor connected to Digitiser: {event.remote_addr}")

        self.sdp_model.dig_connected = CommunicationStatus.ESTABLISHED

        action = Action()

        if self.sdp_model.tm_connected == CommunicationStatus.ESTABLISHED:
            # Send status advice message to Telescope Manager
            tm_adv = self._construct_status_adv_to_tm()
            action.set_msg_to_remote(tm_adv)

        return action

    def process_dig_disconnected(self, event) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Science Data Processor disconnected from Digitiser: {event.remote_addr}")

        self.sdp_model.dig_connected = CommunicationStatus.NOT_ESTABLISHED

        action = Action()

        if self.sdp_model.tm_connected == CommunicationStatus.ESTABLISHED:
            # Send status advice message to Telescope Manager
            tm_adv = self._construct_status_adv_to_tm()
            action.set_msg_to_remote(tm_adv)

        return action
        
    def process_dig_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Digitiser service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Science Data Processor received digitiser {api_call['msg_type']} message with action code: {api_call['action_code']}")

        status, message = sdp_dig.STATUS_SUCCESS, "Acknowledged"

        action = Action()

        # If we are receiving samples from the digitiser, load them into the current scan
        if api_call['action_code'] == sdp_dig.ACTION_CODE_SAMPLES:

            # Extract metadata from the api call
            metadata = api_call.get('metadata', [])

            # Default values
            center_freq = gain = sample_rate = read_counter = read_start = read_end = feed = dig_id = None
            
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
                elif prop == sdp_dig.PROPERTY_FEED:
                    # Match Feed enum by name (value is a string like 'LOAD')
                    try:
                        feed = Feed[value]
                    except KeyError:
                        logger.warning(f"Unknown feed value: {value}, defaulting to NONE")
                        feed = Feed.NONE
                elif prop == sdp_dig.PROPERTY_DIG_ID:
                    dig_id = value

            logger.info(f"SDP received digitiser samples message with metadata: digitiser={dig_id}, feed={feed}, center_freq={center_freq}, gain={gain}, sample rate={sample_rate}, read counter={read_counter}")

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
                        if scan.scan_model.center_freq == center_freq and scan.scan_model.feed == feed:
                            match = scan
                            break
                        else:
                            logger.warning(f"SDP aborting scan:scan parameters no longer match digitiser sample metadata for scan id {scan.scan_model.scan_id}: "
                                           f"scan(center_freq={scan.scan_model.center_freq}, feed={scan.scan_model.feed}) vs "
                                           f"metadata(center_freq={center_freq}, feed={feed})")
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
                        feed=feed if feed is not None else Feed.NONE)

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
        if self.sdp_model.dig_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.sdp_model.tm_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        else:
            return HealthState.OK

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

            sdp.signal_display.set_scan(scan)

            # Call display while the scan is being loaded 
            # Scan will transition to "Complete" or "Aborted" when done
            # This allows for interactive display of the scan as samples are loaded
            while scan.get_status() == ScanState.WIP:
                sdp.signal_display.display()
                time.sleep(1)  # Update display every second
            
            # Final display call to ensure complete rendering
            sdp.signal_display.display()
            sdp.signal_display.save_scan_figure(output_dir=sdp.get_args().output_dir)
                
    except KeyboardInterrupt:
        pass
    finally:
        sdp.stop()

if __name__ == "__main__":
    main()
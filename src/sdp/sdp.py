import logging
import json
from queue import Queue
import numpy as np
from datetime import datetime, timezone
import time
import threading

from env.app import App
from ipc.message import AppMessage
from util.xbase import XBase, XStreamUnableToExtract
from api import sdp_dig, tm_sdp
from ipc.message import APIMessage
from ipc.action import Action
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.health import HealthState
from models.comms import CommunicationStatus
from util import log
from obs.scan import Scan
from signal_display import SignalDisplay

CHANNELS = 1024 # Size of FFT to compute for each block of samples
SCAN_DURATION = 60 # Duration of each scan in seconds
#OUTPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Home'  # Directory to store samples
OUTPUT_DIR = '/Users/r.brederode/samples'  # Directory to store samples

logger = logging.getLogger(__name__)

class SDP(App):

    def __init__(self, app_name: str = "sdp"):

        super().__init__(app_name=app_name)

        # Telescope Manager interface (TBD)
        self.tm_system = "tm"
        self.tm_api = tm_sdp.TM_SDP()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), host=self.get_args().tm_host, port=self.get_args().tm_port)
        self.tm_endpoint.start()
        self.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint)

        # Digitiser interface
        self.dig_system = "dig"
        self.dig_api = sdp_dig.SDP_DIG()
        # Digitiser TCP Server
        self.dig_endpoint = TCPServer(description=self.dig_system, queue=self.get_queue(), host=self.get_args().dig_host, port=self.get_args().dig_port)
        self.dig_endpoint.start()
        self.dig_connected = CommunicationStatus.NOT_ESTABLISHED
        # Register Digitiser interface with the App
        self.register_interface(self.dig_system, self.dig_api, self.dig_endpoint)

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

        self.dig_connected = CommunicationStatus.ESTABLISHED

    def process_dig_disconnected(self, event) -> Action:
        """ Processes Digitiser disconnected events.
        """
        logger.info(f"Science Data Processor disconnected from Digitiser: {event.remote_addr}")

        self.dig_connected = CommunicationStatus.NOT_ESTABLISHED

        # Flush any pending scans
        while not self.scan_q.empty():
            flushed_scan = self.scan_q.get()
            logger.info(f"SDP flushed scan due to digitiser disconnect: {flushed_scan}")
        
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
            center_freq = gain = sample_rate = read_counter = read_start = read_end = feed = None
            
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
                    feed = value

            logger.debug(f"SDP received digitiser samples message with metadata: feed={feed}, center_freq={center_freq}, gain={gain}, sample rate={sample_rate}, read counter={read_counter}")

            with self._rlock:                    
                # Loop through scans in the queue until we find one that matches the read_counter
                # This handles situations where samples are received out of order
                match = None

                pending_scans = [s for s in list(self.scan_q.queue) if s.get_status() == "Empty" or s.get_status() == "WIP"]
                abort_scans = []

                for scan in pending_scans:
                    start_idx, end_idx = scan.get_start_end_idx()
                    if read_counter >= start_idx and read_counter <= end_idx:

                        # Verify that the digitiser metadata still matches the scan parameters
                        if scan.center_freq == center_freq and scan.feed == feed:
                            match = scan
                            break
                        else:
                            logger.warning(f"SDP scan parameters no longer match digitiser sample metadata for scan id {scan.id}: "
                                           f"scan(center_freq={scan.center_freq}, sample_rate={scan.sample_rate}, gain={scan.gain}, feed={scan.feed}) vs "
                                           f"metadata(center_freq={center_freq}, sample_rate={sample_rate}, gain={gain}, feed={feed})"
                            )
                            abort_scans.append(scan)

                    # If the Digitiser read_counter has moved passed this scan by more than its duration
                    elif read_counter > end_idx + scan.duration: 
                        abort_scans.append(scan)  # add it to the abort list

                # If we found scans to abort, do so
                for scan in abort_scans:
                    scan.abort()
                    scan.save_to_disk(output_dir=OUTPUT_DIR, include_iq=False)
                    # Remove this specific scan from the underlying queue 
                    try:
                        self.scan_q.queue.remove(scan)
                        # Balance the unfinished task count for the removed item
                        self.scan_q.task_done()
                    except ValueError:
                        logger.warning(f"Attempted to remove scan {scan} from queue but it was not found")

                # If no matching scan was found, create a new scan
                if match is None:

                    scan = Scan(
                        start_idx=read_counter, 
                        duration=SCAN_DURATION, 
                        sample_rate=sample_rate, 
                        channels=CHANNELS,
                        center_freq=center_freq,
                        gain=gain,
                        feed=feed
                    )
                    self.scan_q.put(scan)
                    match = scan
                    logger.debug(f"SDP created new scan: {scan}")

            if match is not None:              
                logger.debug(f"SDP loading samples into scan: {match}")                        
                # Convert payload to complex64 numpy array
                iq_samples = np.frombuffer(payload, dtype=np.complex64)
              
                if match.load_samples(sec=(read_counter - match.get_start_end_idx()[0] + 1), 
                        iq=iq_samples,
                        read_start=read_start,
                        read_end=read_end):
                    status = sdp_dig.STATUS_SUCCESS
                    message = f"SDP loaded samples into scan id: {match.id}"
                else:
                    status = sdp_dig.STATUS_ERROR
                    message = f"SDP failed to load samples into scan id: {match.id}"

                if match.get_status() == "Complete":
                    logger.info(f"SDP scan is complete: {match}")
              
                    match.save_to_disk(output_dir=OUTPUT_DIR, include_iq=False)
                    match.del_iq()
                    # Remove the specific completed scan from the queue safely
                    with self._rlock:
                        try:
                            self.scan_q.queue.remove(match)
                            self.scan_q.task_done()
                        except ValueError:
                            logger.warning(f"Completed scan {match} not found in queue when removing")
                    
                    

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

        self.tm_connected = CommunicationStatus.ESTABLISHED

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.info(f"Science Data Processor disconnected from Telescope Manager: {event.remote_addr}")

        self.tm_connected = CommunicationStatus.NOT_ESTABLISHED

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

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.dig_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        elif self.tm_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        else:
            return HealthState.OK

def main():
    sdp = SDP()
    sdp.start()

    try:
        while True:
            # If there is a scan in the queue, process it, else sleep & continue 
            try:
                scan = sdp.scan_q.queue[0]
                if scan.get_status() == "Empty":
                    raise IndexError  # No samples loaded yet, skip processing
            except IndexError:
                time.sleep(0.1)
                continue

            sdp.signal_display.set_scan(scan)

            # Call display while the scan is being loaded 
            # Scan will transition to "Complete" or "Aborted" when done
            # This allows for interactive display of the scan as samples are loaded
            while scan.get_status() == "WIP":
                sdp.signal_display.display()
                time.sleep(1)  # Update display every second
            
            # Final display call to ensure complete rendering
            sdp.signal_display.display()
            sdp.signal_display.save_scan_figure(output_dir=OUTPUT_DIR)
                
    except KeyboardInterrupt:
        pass
    finally:
        sdp.stop()

if __name__ == "__main__":
    main()
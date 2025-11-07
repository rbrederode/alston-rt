import numpy as np
import threading
import logging
import time
import json
import os
from datetime import datetime, timezone

from models.dsh import Feed
from models.scan import ScanModel, ScanState
from util import gen_file_prefix
from util.xbase import XSoftwareFailure

logger = logging.getLogger(__name__)

class Scan:

    _id_lock = threading.Lock()
    _next_id = 1

    def __init__(self, scan_model: ScanModel):
        """ Initialize a scan with the given parameters.
            A scan holds raw IQ samples, power spectrum, summed power spectrum and baseline data arrays.
            The data arrays are initialized to zero and incrementally loaded as samples arrive.

            Parameters
                start_idx: Starting index of the digitiser read counter for this scan
                duration: Duration of the scan in seconds
                sample_rate: Sample rate in Hz
                channels: Number of channels (FFT size) for the analysis
                center_freq: Center frequency of the samples in Hz (optional)
                gain: Gain in dB (optional)
                feed: Feed Id (optional)
        """

        with Scan._id_lock:
            self.id = Scan._next_id                     # Unique scan identifier
            Scan._next_id += 1                          # Increment for next scan

        self._rlock = threading.RLock()  # Lock for thread-safe access to shared resources

        with self._rlock:

            self.scan_model = scan_model
            self.scan_model.scan_id = str(self.id)
            self.scan_model.created = datetime.now(timezone.utc)     # Timestamp when the scan was created
            self.scan_model.status = ScanState.EMPTY                 # Status of the scan: EMPTY, WIP, COMPLETE

            self.loaded_secs = self.scan_model.duration * [False]    # List of seconds for which samples have been loaded
            self.prev_read_end = None                                # Timestamp of the previous read end

            # Data arrays that hold data for a given scan of {duration} seconds
            self.raw = None  # Raw IQ samples for the duration of the scan
            self.pwr = None  # Power spectrum for the duration of the scan
            self.spr = None  # Summed power spectrum for each second in the duration of the scan
            self.bsl = None  # Baseline power spectrum over duration of the scan

            self.mean_real = 0.0  # Mean of real value of the raw samples (I)
            self.mean_imag = 0.0  # Mean of imaginary value of the raw samples (Q)

            # Initialize data arrays for the scan
            self.init_data_arrays()

    def __str__(self):

        created = self.scan_model.created.isoformat()
        total_time = (self.scan_model.read_end - self.scan_model.read_start).total_seconds() if self.scan_model.read_start is not None and self.scan_model.read_end is not None else None

        return f"Scan(id={self.scan_model.scan_id}, created={created}, scan model {self.scan_model}, total_time={total_time})\n"

    def get_start_end_idx(self) -> (int, int):
        """ Get the starting and ending index of the digitiser read counter for this scan.
            :returns: The starting and ending index as a tuple of integers
            Example: If start_idx=1000 and duration=60, then this function returns (1000, 1059)
            where 1000 is the starting index and 1059 is the ending index (inclusive) for a scan of 60 seconds
        """
        return self.scan_model.start_idx, self.scan_model.start_idx + self.scan_model.duration - 1

    def init_data_arrays(self):
        """
        Initialize data arrays for raw samples, power spectrum, summed power & baseline
        The data arrays are flushed every time a new scan is started.
            :param sample_rate: Sample rate in Hz
            :param duration: Duration of the scan in seconds
            :param channels: Number of channels (FFT size) for the analysis
        """    
        with self._rlock:

            # Calculate the number of rows in the spectrogram based on duration and sample rate
            num_rows = int(np.ceil(self.scan_model.duration * self.scan_model.sample_rate / self.scan_model.channels))      # number of rows in the spectrogram

            self.raw = np.zeros((num_rows, self.scan_model.channels), dtype=np.complex64)   # complex64 for raw IQ samples i.e. 8 bytes per sample (4 bytes for real and 4 bytes for imaginary parts)
            self.pwr = np.zeros((num_rows, self.scan_model.channels), dtype=np.float64)     # float64 for power spectrum data
            self.spr = np.zeros((self.scan_model.duration, self.scan_model.channels), dtype=np.float64)     # float64 for summed pwr for each second in duration
            self.bsl = np.ones((self.scan_model.channels,), dtype=np.float64)               # float64 for baseline power spectrum over duration

    def get_dig_id(self) -> str:
        """
        Get the digitiser ID associated with this scan.
            :returns: The digitiser ID as a string
        """
        return self.scan_model.dig_id
    
    def get_status(self) -> str:
        """
        Get the current status of the scan.
            :returns: A string representation of the scan status
        """
        with self._rlock:
            return self.scan_model.status

    def set_status(self, status: ScanState):
        """
        Set the status of the scan.
            :param status: The new status to set
        """
        with self._rlock:
            self.scan_model.status = status

    def get_loaded_seconds(self) -> int:
        """
        Get the number of seconds for which samples have been loaded in this scan.
            :returns: Number of seconds with loaded samples
        """
        return np.count_nonzero(self.loaded_secs)

    def load_samples(self, sec: int, iq: np.ndarray, read_start: datetime, read_end: datetime) -> bool:
        """
        Load raw IQ samples into the scan's data array and calculate the power spectrum.
            :param sec: Second within the scan to load samples (1 <= sec <= scan duration)
            :param iq: A numpy array of complex64 IQ samples to load
            :param read_start: Timestamp when the samples were read (UTC)
            :param read_end: Timestamp when the samples were read (UTC)
            :returns: True if samples were loaded successfully, False otherwise
        Example: load_samples(1, 10, iq) will load samples for seconds 1 to 10 (inclusive) of the scan
        """

        if sec < 1 or sec > self.scan_model.duration:
            logger.warning(f"Scan {self.scan_model.scan_id} - Invalid second ({sec}) for scan duration {self.scan_model.duration}")
            self.scan_model.load_failures += 1
            return False

        if iq is None or len(iq) < self.scan_model.sample_rate:
            logger.warning(f"Scan {self.scan_model.scan_id} - Not enough samples provided. Expected {self.scan_model.sample_rate}, got {len(iq) if iq is not None else 0}. Skipping samples...")
            self.scan_model.load_failures += 1
            return False

        if read_start is None or read_end is None or read_start >= read_end:
            logger.warning(f"Scan {self.scan_model.scan_id} - Invalid read start/end timestamps provided. Skipping samples...")
            self.scan_model.load_failures += 1
            return False

        logger.debug(f"Scan {self.scan_model.scan_id} - Loading {iq.shape} samples for second {sec} into scan.")

        # Reshape the samples to fit into a number of rows, each of channels columns and convert to complex64 (if needed) for better efficiency
        iq = iq[:int(self.scan_model.sample_rate - (self.scan_model.sample_rate % self.scan_model.channels))].astype(np.complex64)  # Discard excess samples that don't fit into the channels
        iq = iq.reshape(-1, self.scan_model.channels) # Reshape to have rows each of size channels columns

        row_start = int((sec - 1) * self.scan_model.sample_rate / self.scan_model.channels)   # Calculate the starting row index (zero based) using sec
        row_end = int(sec * self.scan_model.sample_rate / self.scan_model.channels)           # Calculate the ending row index (zero based) using sec

        pwr = np.zeros((iq.shape[0], self.scan_model.channels), dtype=np.float64)  # Temporary array to hold power spectrum for the loaded samples
        # For each row i.e. 'shape[0]' in the reshaped sample set, calculate and record the power spectrum
        for j in range(iq.shape[0]):
            pwr[j,:] = np.abs(np.fft.fftshift(np.fft.fft(iq[j,:])))**2  # The power spectrum is the absolute value of the signal squared

        spr = np.sum(pwr, axis=0)  # Sum power across all rows for this second
        remove_dc_spike(self.scan_model.channels, spr)  # Remove DC spike if present

        # Store the raw, power and summed spectrum data in the appropriate rows of the scan data arrays
        #with self._rlock:
        self.raw[row_start:row_start + iq.shape[0],:] = iq
        self.pwr[row_start:row_start + iq.shape[0],:] = pwr
        self.spr[sec - 1,:] = spr  # sec is 1-based index, so adjust for 0-based array index
        self.loaded_secs[sec - 1] = True  # Mark this second as loaded

        indices = np.linspace(row_start, row_end - 1, int(self.raw.shape[0]*0.01), dtype=int)

        self.mean_real = np.mean(np.abs(self.raw[row_start:row_end, ].real))*100  # Find the mean real value in the raw samples (I)
        self.mean_imag = np.mean(np.abs(self.raw[row_start:row_end, ].imag))*100  # Find the mean imaginary value in the raw samples (Q)

        # Count how many rows have self.loaded_secs marked as True
        actual_rows = np.count_nonzero(self.loaded_secs)
        expected_rows = self.scan_model.duration

        self.scan_model.read_start = read_start if self.scan_model.read_start is None else min(self.scan_model.read_start, read_start)  # Update read start time
        self.scan_model.read_end = read_end if self.scan_model.read_end is None else max(self.scan_model.read_end, read_end)  # Update read end time
        self.scan_model.gap = (read_start - self.prev_read_end).total_seconds() if self.prev_read_end is not None else None
        if self.scan_model.gap is not None:
            logger.info(f"Scan {self.scan_model.scan_id} - Gap of {self.scan_model.gap:.3f} seconds detected between last read end {self.prev_read_end} and current read start {read_start}.")
        self.prev_read_end = read_end  # Update last read end time

        # Update scan status based on loaded rows
        if actual_rows == 0:
            self.set_status(ScanState.EMPTY)
        elif actual_rows > 0 and actual_rows < expected_rows:
            self.set_status(ScanState.WIP)
        elif actual_rows >= expected_rows:
            self.set_status(ScanState.COMPLETE)

        return True

    def save_to_disk(self, output_dir, include_iq: bool = False) -> bool:
        """
        Flush the IQ sample data of the scan to a file on disk.
            :param output_dir: Directory where the IQ data file will be saved
            :param include_iq: Whether to flush the IQ data or not (default is False)
            :returns: True if the data was saved successfully, False otherwise
        """
        
        if self.scan_model.feed is None:
            logger.warning(f"Scan {self} - Feed Id is not set. Cannot save scan to disk.")
            return False

        if self.scan_model.status != ScanState.COMPLETE:
            logger.warning(f"Scan - Saving an incomplete scan: {self}.")

        if output_dir is None or output_dir == '':
            output_dir = "./"

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        prefix = gen_file_prefix(
            dt=self.scan_model.read_start, feed=self.scan_model.feed, gain=self.scan_model.gain, 
            duration=self.scan_model.duration, sample_rate=self.scan_model.sample_rate, center_freq=self.scan_model.center_freq, 
            channels=self.scan_model.channels, entity_id=self.scan_model.scan_id
        )

        try:
            if include_iq:
                filename = prefix + "-raw" + ".iq"
                with open(f"{output_dir}/{filename}", 'wb') as f:
                    self.raw.tofile(f)

            filename = prefix + "-meta" + ".json"
            with open(f"{output_dir}/{filename}", 'w') as f:
                json.dump(self.get_scan_meta(), f, indent=4)  

            filename = prefix + "-load" + ".csv" if self.scan_model.feed == Feed.LOAD else prefix + "-spr" + ".csv"
            with open(f"{output_dir}/{filename}", 'w') as f:
                np.savetxt(f, self.spr, delimiter=",", fmt="%.6f")
        
        except Exception as e:
            logger.error(f"Scan {self} - Failed to save to {output_dir}/{filename}: {e}")
            return False

        logger.info(f"Scan {self} - Saved to {output_dir}/{prefix}-*")
        return True

    def load_from_disk(self, read_start: str, input_dir: str, include_iq: bool = False) -> bool:
        """
        Load the IQ sample data of the scan from a file on disk.
            :param read_start: The start time of the read operation formatted as "YYYYMMDDTHHMMSS"
            :param input_dir: Directory where the IQ data file is located
            :param include_iq: Whether to load the IQ data or not (default is False)
            :returns: True if the data was loaded successfully, False otherwise
        """

        if read_start is None or read_start == '':
            logger.warning("Scan - read_start parameter is required to load scan from disk.")
            return False

        try:
            read_start = datetime.strptime(read_start, "%Y-%m-%dT%H%M%S")
        except ValueError:
            logger.warning(f"Scan - Invalid scan start date/time provided while loading scan from disk. Expected 'YYYY-MM-DDTHHMMSS', got {read_start}.")
            return False

        if input_dir is None or input_dir == '':
            input_dir = "./"

        logger.info(f"Scan - Looking for scan files in dir {input_dir} matching scan start date/time {read_start.strftime('%Y-%m-%dT%H%M%S')}")
        read_files = [f for f in os.listdir(input_dir) if read_start.strftime("%Y-%m-%dT%H%M%S") in f and f.endswith('meta.json')]

        if read_files is None or len(read_files) == 0:
            logger.warning(f"Scan - No scan files found in dir {input_dir} matching scan start date/time {read_start}")
            return False

        read_file = sorted(read_files)[-1]  # Identify the most recent file and use that one
        logger.info(f"Scan - Reading scan data from {input_dir}/{read_file}")

        try:
            with open(f"{input_dir}/{read_file}", 'r') as f:
                meta = json.load(f)
                self.scan_model.from_dict(meta)

        except Exception as e:
            logger.error(f"Scan - Failed to read metadata from {input_dir}/{read_file}: {e}")
            return False

        if self.scan_model.status != ScanState.COMPLETE:
            logger.warning(f"Scan - Loading an incomplete scan from disk: {self}.")

        try:
            self.init_data_arrays()

            prefix = gen_file_prefix(dt=self.scan_model.read_start, feed=self.scan_model.feed, gain=self.scan_model.gain, 
                duration=self.scan_model.duration, sample_rate=self.scan_model.sample_rate, center_freq=self.scan_model.center_freq, 
                channels=self.scan_model.channels, entity_id=self.scan_model.scan_id)

            if include_iq:
                # Load raw IQ samples 
                filename = prefix + "-raw" + ".iq"
                with open(f"{input_dir}/{filename}", 'rb') as f:
                    self.raw = np.fromfile(f, dtype=np.complex64)
                    self.raw = self.raw.reshape(-1, self.scan_model.channels)

                # Recalculate power spectrum (self.pwr)
                num_rows = self.raw.shape[0]
                for row in range(num_rows):
                    self.pwr[row,:] = np.abs(np.fft.fftshift(np.fft.fft(self.raw[row,:])))**2 # The power spectrum is the absolute value of the signal squared

                # Recalculate the summed power spectrum (self.spr)
                for sec in range(self.scan_model.duration):
                    row_start = sec * (num_rows // self.scan_model.duration)
                    row_end = (sec + 1) * (num_rows // self.scan_model.duration) if sec < self.scan_model.duration - 1 else num_rows  # Ensure we cover all rows

                    # Calculate the sum of the power spectrum for each frequency bin in a given second
                    self.spr[sec,:] = np.sum(self.pwr[row_start:row_end,:], axis=0)  # Sum the power spectrum in a given sec for each frequency bin (in columns)
                    remove_dc_spike(self.scan_model.channels, self.spr[sec,:])

                self.loaded_secs = [True] * self.scan_model.duration
            else:
                # Load summed power spectrum only
                filename = prefix + "-load" + ".csv" if self.scan_model.feed == Feed.NONE else prefix + "-spr" + ".csv"
                with open(f"{input_dir}/{filename}", 'r') as f:
                    self.spr = np.loadtxt(f, delimiter=",")
                    self.spr = self.spr.reshape(-1, self.scan_model.channels)

                self.scan_model.loaded_secs = [True] * self.spr.shape[0]

        except Exception as e:
            logger.error(f"Scan - Failed to load data from {input_dir}: {e}")
            return False

        logger.info(f"Scan - Loaded scan from {input_dir} with start date/time {self.scan_model.read_start}: {self}")
        return True

    def del_iq(self):
        """ Flush the iq data to the bin """
        with self._rlock:
            if hasattr(self, 'raw') and self.raw is not None:
                logger.info(f"Scan {self.scan_model.scan_id} - Deleting raw IQ data from memory.")
                del self.raw

    def get_scan_meta(self) -> dict:
        """
        Get metadata about the scan as a dictionary.
            :returns: A dictionary containing metadata about the scan
        """
        with self._rlock:
            return self.scan_model.to_dict()

def remove_dc_spike(channels, arr):

    """ Ref: https://pysdr.org/content/sampling.html#dc-spike-and-offset-tuning
    Identify and remove the DC spike (if present) at the center frequency """

    # Review the bins either side the centre of channels
    # We expect the DC spike to occur in the central bin
    start = channels//2-1 # Zero indexed array
    end =  channels//2+2 # DC spike is in the middle

    # Calculate the mean and std deviation of the reviewed samples
    mean = np.mean(arr[start:end])
    std = np.std(arr[start:end])

    # Create a mask for values above one standard deviation from the mean
    mask = arr[start:end] > (mean + std)
    # Calculate the mean of the reviewed samples excluding the values in the mask
    mean_no_dc = np.mean(arr[start:end][~mask])

    #print(f"Considered samples from {start} to {end}: {arr[start:end]}")
    #print(f"Mean {mean} Std {std} Mask {mask} Mean Excluding DC {np.mean(arr[start:end][~mask])}")

    # Replace values above one standard deviation with the mean of the samples surrounding the DC spike
    arr[start:end][mask] = np.mean(arr[start:end][~mask])

if __name__ == "__main__":

    # Setup logging configuration
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
        handlers=[
            logging.StreamHandler(),                     # Log to console
            logging.FileHandler("client.log", mode="a")  # Log to a file
            ]
    )

    INPUT_DIR = '~/.alston/samples'  # Directory to store samples
    INPUT_DIR = os.path.expanduser(INPUT_DIR)

    scan_model = ScanModel(
        scan_id="tm001",
        created=datetime.now(timezone.utc),
        read_start=datetime.now(timezone.utc),
        read_end=datetime.now(timezone.utc),
        prev_read_end=datetime.now(timezone.utc),
        start_idx=100,
        duration=60,
        sample_rate=24e5,
        channels=1024,
        center_freq=1420400000,
        gain=12,
        feed=Feed.H3T_1420,
        status="WIP",
        load_failures=0,
        last_update=datetime.now(timezone.utc)
    )

    scan = Scan(scan_model=scan_model)
    print(scan)
    scan.load_from_disk(read_start="2025-06-24T130440", input_dir=INPUT_DIR, include_iq=True)
    print(scan)

    from sdp.signal_display import SignalDisplay

    display = SignalDisplay()
    display.set_scan(scan)
    display.display()

    # press a key to continue
    input("Press Enter to continue...")

    scan.save_to_disk(output_dir=INPUT_DIR, include_iq=False)
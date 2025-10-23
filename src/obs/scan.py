import numpy as np
import threading
import logging
import time
import json
import os
from datetime import datetime, timezone

from models.dsh import Feed
from util import gen_file_prefix
from util.xbase import XSoftwareFailure

logger = logging.getLogger(__name__)

SCAN_STATUS_EMPTY = "Empty"
SCAN_STATUS_WIP = "WIP"
SCAN_STATUS_COMPLETE = "Complete"

class Scan:

    _id_lock = threading.Lock()
    _next_id = 1

    def __init__(self, start_idx: int = 0, duration: int = 60, sample_rate: float = 2.4e6, channels: int = 1024, center_freq: float = 1.42e9, gain: float = 0, feed: Feed = Feed.F1420_H3T):
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

            self.scan_created = time.time()             # Timestamp when the scan was created
            self.start_idx = start_idx                  # Starting index of the digitiser read counter for this scan
            
            self.scan_status = SCAN_STATUS_EMPTY        # Status of the scan: EMPTY, WIP, COMPLETE
            self.loaded_sec = duration * [False]        # List of seconds for which samples have been loaded

            self.duration = duration                    # Duration of the scan in seconds
            self.sample_rate = sample_rate              # Sample rate in Hz
            self.channels = channels                    # Number of channels (FFT bins)
            self.center_freq = center_freq              # Center frequency in Hz
            self.gain = gain                            # Gain in dB (optional)
            self.feed = feed                            # Feed Id

            self.read_start = None                      # Read start time
            self.read_end = None                        # Read end time
            self.last_read_end = None                   # Previous read end time

            # Data arrays that hold data for a given scan of {duration} seconds
            self.raw = None  # Raw IQ samples for the duration of the scan
            self.pwr = None  # Power spectrum for the duration of the scan
            self.spr = None  # Summed power spectrum for each second in the duration of the scan
            self.bsl = None  # Baseline power spectrum over duration of the scan

            self.mean_real = 0.0  # Mean of real value of the raw samples (I)
            self.mean_imag = 0.0  # Mean of imaginary value of the raw samples (Q)

            # Initialize data arrays for the scan
            self.init_data_arrays(sample_rate=self.sample_rate, duration=self.duration, channels=self.channels)

    def __str__(self):

        created = datetime.fromtimestamp(self.scan_created, tz=timezone.utc).isoformat() if self.scan_created is not None else None
        total_time = (self.read_end - self.read_start).total_seconds() if self.read_start is not None and self.read_end is not None else None

        return f"Scan(id={self.id}, created={created}, start_idx={self.start_idx}, status={self.scan_status}, " + \
               f"duration={self.duration}, sample_rate={self.sample_rate}, channels={self.channels}, center_freq={self.center_freq}, " + \
               f"gain={self.gain}, feed={self.feed}, read_start={self.read_start}, read_end={self.read_end}, total_time={total_time})\n"

    def get_start_end_idx(self) -> (int, int):
        """ Get the starting and ending index of the digitiser read counter for this scan.
            :returns: The starting and ending index as a tuple of integers
            Example: If start_idx=1000 and duration=60, then this function returns (1000, 1059)
            where 1000 is the starting index and 1059 is the ending index (inclusive) for a scan of 60 seconds
        """
        return self.start_idx, self.start_idx + self.duration - 1

    def init_data_arrays(self, sample_rate, duration, channels):
        """
        Initialize data arrays for raw samples, power spectrum, summed power & baseline
        The data arrays are flushed every time a new scan is started.
            :param sample_rate: Sample rate in Hz
            :param duration: Duration of the scan in seconds
            :param channels: Number of channels (FFT size) for the analysis
        """    
        with self._rlock:

            # Calculate the number of rows in the spectrogram based on duration and sample rate
            num_rows = int(np.ceil(duration * sample_rate / channels))      # number of rows in the spectrogram

            self.raw = np.zeros((num_rows, channels), dtype=np.complex64)   # complex64 for raw IQ samples i.e. 8 bytes per sample (4 bytes for real and 4 bytes for imaginary parts)
            self.pwr = np.zeros((num_rows, channels), dtype=np.float64)     # float64 for power spectrum data
            self.spr = np.zeros((duration, channels), dtype=np.float64)     # float64 for summed pwr for each second in duration
            self.bsl = np.ones((channels,), dtype=np.float64)               # float64 for baseline power spectrum over duration

    def is_complete(self) -> bool:
        """
        Check if the scan has been completed i.e. all expected samples have been received.
            :returns: True if the scan is complete, False otherwise
        """
        return self.scan_status == SCAN_STATUS_COMPLETE

    def get_loaded_seconds(self) -> int:
        """
        Get the number of seconds for which samples have been loaded in this scan.
            :returns: Number of seconds with loaded samples
        """
        return np.count_nonzero(self.loaded_sec)

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

        if sec < 1 or sec > self.duration:
            logger.warning(f"Scan - Invalid second ({sec}) for scan duration {self.duration}")
            return False

        if iq is None or len(iq) < self.sample_rate:
            logger.warning(f"Scan - Not enough samples provided. Expected {self.sample_rate}, got {len(iq) if iq is not None else 0}. Skipping samples...")
            return False

        if read_start is None or read_end is None or read_start >= read_end:
            logger.warning("Scan - Invalid read start/end timestamps provided. Skipping samples...")
            return False

        logger.debug(f"Loading {iq.shape} samples for second {sec} into scan {self.id}")

        # Reshape the samples to fit into a number of rows, each of channels columns and convert to complex64 (if needed) for better efficiency
        iq = iq[:int(self.sample_rate - (self.sample_rate % self.channels))].astype(np.complex64)  # Discard excess samples that don't fit into the channels
        iq = iq.reshape(-1, self.channels) # Reshape to have rows each of size channels columns

        row_start = int((sec - 1) * self.sample_rate / self.channels)   # Calculate the starting row index (zero based) using sec
        row_end = int(sec * self.sample_rate / self.channels)           # Calculate the ending row index (zero based) using sec

        pwr = np.zeros((iq.shape[0], self.channels), dtype=np.float64)  # Temporary array to hold power spectrum for the loaded samples
        # For each row i.e. 'shape[0]' in the reshaped sample set, calculate and record the power spectrum
        for j in range(iq.shape[0]):
            pwr[j,:] = np.abs(np.fft.fftshift(np.fft.fft(iq[j,:])))**2  # The power spectrum is the absolute value of the signal squared

        spr = np.sum(pwr, axis=0)  # Sum power across all rows for this second
        remove_dc_spike(self.channels, spr)  # Remove DC spike if present

        # Store the raw, power and summed spectrum data in the appropriate rows of the scan data arrays
        #with self._rlock:
        self.raw[row_start:row_start + iq.shape[0],:] = iq
        self.pwr[row_start:row_start + iq.shape[0],:] = pwr
        self.spr[sec - 1,:] = spr  # sec is 1-based index, so adjust for 0-based array index
        self.loaded_sec[sec - 1] = True  # Mark this second as loaded

        indices = np.linspace(row_start, row_end - 1, int(self.raw.shape[0]*0.01), dtype=int)

        self.mean_real = np.mean(np.abs(self.raw[row_start:row_end, ].real))*100  # Find the mean real value in the raw samples (I)
        self.mean_imag = np.mean(np.abs(self.raw[row_start:row_end, ].imag))*100  # Find the mean imaginary value in the raw samples (Q)

        # Count how many rows have self.loaded_sec marked as True
        actual_rows = np.count_nonzero(self.loaded_sec)
        expected_rows = self.duration
        
        # Update scan status based on loaded rows
        if actual_rows == 0:
            self.scan_status = SCAN_STATUS_EMPTY
        elif actual_rows > 0 and actual_rows < expected_rows:
            self.scan_status = SCAN_STATUS_WIP
        elif actual_rows >= expected_rows:
            self.scan_status = SCAN_STATUS_COMPLETE

        self.read_start = read_start if self.read_start is None else min(self.read_start, read_start)  # Update read start time
        self.read_end = read_end if self.read_end is None else max(self.read_end, read_end)  # Update read end time
        gap = (read_start - self.last_read_end).total_seconds() if self.last_read_end is not None else None
        if gap is not None:
            logger.info(f"Scan {self.id} - Gap of {gap:.3f} seconds detected between last read end {self.last_read_end} and current read start {read_start}.")
        self.last_read_end = read_end  # Update last read end time

        return True

    def save_to_disk(self, output_dir, include_iq: bool = False) -> bool:
        """
        Flush the IQ sample data of the scan to a file on disk.
            :param output_dir: Directory where the IQ data file will be saved
            :param include_iq: Whether to flush the IQ data or not (default is False)
            :returns: True if the data was saved successfully, False otherwise
        """
        
        if self.feed is None:
            logger.warning(f"Scan {self} - Feed Id is not set. Cannot save scan to disk.")
            return False
        
        if self.scan_status != SCAN_STATUS_COMPLETE:
            logger.warning(f"Scan - Saving an incomplete scan: {self}.")

        if output_dir is None or output_dir == '':
            output_dir = "./"

        prefix = gen_file_prefix(dt=self.read_start, feed=self.feed, gain=self.gain, duration=self.duration, 
            sample_rate=self.sample_rate, center_freq=self.center_freq, channels=self.channels, entity_id=self.id)

        try:
            if include_iq:
                filename = prefix + "-raw" + ".iq"
                with open(f"{output_dir}/{filename}", 'wb') as f:
                    self.raw.tofile(f)

            filename = prefix + "-meta" + ".json"
            with open(f"{output_dir}/{filename}", 'w') as f:
                json.dump(self.get_scan_meta(), f, indent=4)  

            filename = prefix + "-load" + ".csv" if self.feed == Feed.NONE else prefix + "-spr" + ".csv"
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
            read_start = datetime.strptime(read_start, "%Y%m%dT%H%M%S")
        except ValueError:
            logger.warning(f"Scan - Invalid scan start date/time provided while loading scan from disk. Expected 'YYYYMMDDTHHMMSS', got {read_start}.")
            return False

        if input_dir is None or input_dir == '':
            input_dir = "./"

        logger.info(f"Scan - Looking for scan files in dir {input_dir} matching scan start date/time {read_start}")
        read_files = [f for f in os.listdir(input_dir) if read_start.strftime("%Y%m%dT%H%M%S") in f and f.endswith('meta.json')]

        if read_files is None or len(read_files) == 0:
            logger.warning(f"Scan - No scan files found in dir {input_dir} matching scan start date/time {read_start}")
            return False

        read_file = sorted(read_files)[-1]  # Identify the most recent file and use that one
        logger.info(f"Scan - Reading scan data from {input_dir}/{read_file}")

        try:
            with open(f"{input_dir}/{read_file}", 'r') as f:
                meta = json.load(f)

                self.id = meta["id"]
                self.start_idx = meta["start_idx"]
                self.scan_status = meta["scan_status"]
                self.duration = meta["duration"]
                self.sample_rate = meta["sample_rate"]
                self.channels = meta["channels"]
                self.center_freq = meta["center_freq"]
                self.gain = meta["gain"]
                self.feed = meta["feed"]
                self.scan_created = datetime.fromisoformat(meta["scan_created"]).timestamp() if meta["scan_created"] is not None else None
                self.read_start = datetime.fromisoformat(meta["read_start"]) if meta["read_start"] is not None else None
                self.read_end = datetime.fromisoformat(meta["read_end"]) if meta["read_end"] is not None else None
                
        except Exception as e:
            logger.error(f"Scan - Failed to read metadata from {input_dir}/{read_file}: {e}")
            return False

        if self.scan_status != SCAN_STATUS_COMPLETE:
            logger.warning(f"Scan - Loading an incomplete scan from disk: {self}.")

        try:
            self.init_data_arrays(sample_rate=self.sample_rate, duration=self.duration, channels=self.channels)

            prefix = gen_file_prefix(dt=self.read_start, feed=self.feed, gain=self.gain, duration=self.duration, 
                sample_rate=self.sample_rate, center_freq=self.center_freq, channels=self.channels, entity_id=self.id)

            if include_iq:
                # Load raw IQ samples 
                filename = prefix + "-raw" + ".iq"
                with open(f"{input_dir}/{filename}", 'rb') as f:
                    self.raw = np.fromfile(f, dtype=np.complex64)
                    self.raw = self.raw.reshape(-1, self.channels)

                # Recalculate power spectrum (self.pwr)
                num_rows = self.raw.shape[0]
                for row in range(num_rows):
                    self.pwr[row,:] = np.abs(np.fft.fftshift(np.fft.fft(self.raw[row,:])))**2 # The power spectrum is the absolute value of the signal squared

                # Recalculate the summed power spectrum (self.spr)
                for sec in range(self.duration):
                    row_start = sec * (num_rows // self.duration)
                    row_end = (sec + 1) * (num_rows // self.duration) if sec < self.duration - 1 else num_rows  # Ensure we cover all rows

                    # Calculate the sum of the power spectrum for each frequency bin in a given second
                    self.spr[sec,:] = np.sum(self.pwr[row_start:row_end,:], axis=0)  # Sum the power spectrum in a given sec for each frequency bin (in columns)
                    remove_dc_spike(self.channels, self.spr[sec,:])

                self.loaded_sec = [True] * self.duration
            else:
                # Load summed power spectrum only
                filename = prefix + "-load" + ".csv" if self.feed == Feed.NONE else prefix + "-spr" + ".csv"
                with open(f"{input_dir}/{filename}", 'r') as f:
                    self.spr = np.loadtxt(f, delimiter=",")
                    self.spr = self.spr.reshape(-1, self.channels)

                self.loaded_sec = [True] * self.spr.shape[0]

        except Exception as e:
            logger.error(f"Scan - Failed to load data from {input_dir}: {e}")
            return False

        logger.info(f"Scan - Loaded scan from {input_dir} with start date/time {self.read_start}: {self}")
        return True

    def del_iq(self):
        """ Flush the iq data to the bin """
        del self.raw

    def get_scan_meta(self) -> dict:
        """
        Get metadata about the scan as a dictionary.
            :returns: A dictionary containing metadata about the scan
        """
        with self._rlock:
            meta = {
                "id": self.id,
                "start_idx": self.start_idx,
                "scan_status": self.scan_status,
                "duration": self.duration,
                "sample_rate": self.sample_rate,
                "bandwidth": self.sample_rate,
                "channels": self.channels,
                "center_freq": self.center_freq,
                "gain": self.gain,
                "feed": self.feed,
                "scan_created": datetime.fromtimestamp(self.scan_created, tz=timezone.utc).isoformat(),
                "read_start": self.read_start.isoformat() if self.read_start is not None else None,
                "read_end": self.read_end.isoformat() if self.read_end is not None else None
            }
        return meta

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

    INPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Home'  # Directory to store samples

    scan = Scan(start_idx=1000, duration=10, sample_rate=2.4e6, channels=1024, center_freq=1.42e9, gain=0)
    print(scan)
    scan.load_from_disk(read_start="19700114T200322", input_dir=INPUT_DIR, include_iq=True)
    print(scan)

    from sdp.signal_display import SignalDisplay

    display = SignalDisplay()
    display.set_scan(scan)
    display.display()

    # press a key to continue
    input("Press Enter to continue...")

    scan.save_to_disk(output_dir=INPUT_DIR, include_iq=False)
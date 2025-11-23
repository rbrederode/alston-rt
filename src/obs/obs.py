
import threading
import uuid
import os
import json
import numpy as np
from datetime import datetime, timezone

from util.xbase import XSchedulerFailure, XSoftwareFailure
from models.obs import ObsState, ObsModel
from models.target import TargetModel, TargetType

import logging
logger = logging.getLogger(__name__)

USABLE_BANDWIDTH = 0.65  # Percentage of usable bandwidth for a scan i.e. full bandpass cannot be used due to roll-off at edges
MAX_SCAN_DURATION = 60  # Maximum duration of a single scan in seconds, to keep memory and processing manageable
SCAN_OVERHEAD_FACTOR = 1.2  # Factor to account for overhead in sampling i.e. 60 seconds of samples takes ~70 seconds to sample

#INPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Home'  
INPUT_DIR = '/Users/r.brederode/samples'  # Default input directory for loading calibration data


def generate_obs_id() -> str:
    """Generate a random observation id.

    Uses UUID4 which is random-based and suitable for non-cryptographic unique ids.
    Returns the canonical hex string representation with dashes (e.g. "550e8400-e29b-41d4-a716-446655440000").
    """
    return str(uuid.uuid4())

class Observation:

    def __init__(self,
            obs_id: str | None = None, 
            desc:str=None, 
            target: TargetModel=None, 
            center_freq:float=None, 
            bandwidth:float=None, 
            sample_rate:float=None, 
            channels:int=None, 
            duration:float=None
        ):
        """ 
        Create an Observation.
        If obs_id is not provided, a new UUID4 string will be generated.
        Parameters:
            obs_id (str | None): Unique identifier for the observation.
            desc (str): Description of the observation.
            target (Target): Target object representing the sky / terrestrial target.
            center_freq (float): Center frequency in Hz e.g. 1420e6 or 1.42e9 for 1420 MHz.
            bandwidth (float): Bandwidth in Hz e.g. 1.0e6 for 1.0 MHz.
            sample_rate (float): Sample rate in Hz e.g. 2.4e6 for 2.4 MHz.
            channels (int): Number of channels (FFT size) for the analysis.
            duration (float): Total time on target in seconds.
        """
        self._rlock = threading.RLock()  # Lock for thread-safe access to shared resources

        with self._rlock:

            self.obs_model = ObsModel()

            self.obs_model.obs_id = obs_id or generate_obs_id()
            self.obs_model.state = ObsState.EMPTY  # Initial status of the observation

            self.obs_model.short_desc = desc     # Short description of the observation
            self.obs_model.long_desc = desc      # Long description of the observation

            self.obs_model.targets.append(target)             # Target object
            self.obs_model.target_durations.append(duration)  # Duration on target (seconds)

            self.obs_model.center_freq = center_freq  # Center frequency (Hz)
            self.obs_model.bandwidth = bandwidth      # Bandwidth (Hz)
            self.obs_model.sample_rate = sample_rate  # Sample rate (Hz)
            self.obs_model.channels = channels        # Number of channels (fft size)
            self.obs_model.duration = duration        # Total time on target (seconds)

            self.sample_count = self.obs_model.sample_rate * self.obs_model.duration  # Total number of samples to be collected

            # Calculate and populate above scan parameters
            self.determine_scans(center_freq=self.obs_model.center_freq, 
                                 bandwidth=self.obs_model.bandwidth, 
                                 sample_rate=self.obs_model.sample_rate, 
                                 duration=self.obs_model.duration)

            self.tsys = None        # Numpy array to hold system temperature calibration data
            self.gain = None        # Numpy array to hold gain calibration data
            self.ibsl = None        # Numpy array to hold integrated baseline (load) calibration data

            self.ispr = None        # Numpy array to hold integrated signal power data
            self.itpw = None        # Numpy array to hold integrated total power sky timeline data

            self.scans = []         # List of Scan objects associated with this observation

            # Initialize data arrays for the scan
            self._init_data_arrays(freq_scans=self.obs_model.freq_scans, 
                                   scan_iters=self.obs_model.scan_iterations, 
                                   scan_duration=self.obs_model.scan_duration, 
                                   channels=self.obs_model.channels)

    def _init_data_arrays(self, freq_scans:int, scan_iters:int, scan_duration:int, channels:int):
        """
        Initialize data arrays for tsys, gain, baseline and scan data.
            :param sample_rate: Sample rate in Hz
            :param duration: Duration of the scan in seconds
            :param channels: Number of channels (FFT size) for the analysis
        """

        if freq_scans is None or channels is None:
            raise XSoftwareFailure(f"Observation {self.id}- Frequency scans {freq_scans} and channels {channels} must be specified to initialise observation data arrays.")
 
        with self._rlock:
            # Initialise integrated (over scans) numpy arrays to hold calibration data
            self.gain = np.ones((freq_scans, channels), dtype=np.float64)  # Initialise (scan X fft_size) array for gain calibration
            self.tsys = np.zeros((freq_scans, channels), dtype=np.float64) # Initialise (scan X fft_size) array for Tsys calibration
            self.ibsl = np.ones((freq_scans, channels), dtype=np.float64)  # Initialise (scan X fft_size) array for baseline power spectrum
            self.ispr = np.zeros((freq_scans, channels), dtype=np.float64)  # Initialise (scan X fft_size) array for integrated signal power data
            self.itpw = np.zeros(int(np.ceil(freq_scans * scan_duration * scan_iters)), dtype=np.float64)  # Initialise (scans * duration * iterations) array for total power sky timeline
    
    def determine_scans(self, center_freq:int=None, bandwidth:int=None, sample_rate:int=None, duration:int=None):
        """
        Calculate the number of frequency scans needed to cover the frequency range from start_freq to end_freq
        and the overlap in the frequency domain. NOTE: The overlap is different to the non-usable bandwidth !!
        
        Calculate the number of scan iterations and scan duration to keep the scan duration within 0-60 seconds
        i.e. manageable from a performance perspective.

        E.g. We may need 10 scans of 1 minute each to cover the bandwidth from start_freq to end_freq,
        where each scan is iterated 5 times to cover the duration of 5 minutes per frequency scan.
        """
        if center_freq is None or bandwidth is None or sample_rate is None or duration is None:
            raise XSoftwareFailure(f"Observation {self.obs_model.obs_id} - Center frequency {center_freq}, bandwidth {bandwidth}, "
                                   f"sample rate {sample_rate}, and duration {duration} must be set to determine scans.")

        with self._rlock:
            self.obs_model.freq_min = center_freq - bandwidth / 2 - sample_rate * (1-USABLE_BANDWIDTH)/2  # Start of frequency scanning
            self.obs_model.freq_max = center_freq + bandwidth / 2 + sample_rate * (1-USABLE_BANDWIDTH)/2  # End of frequency scanning

            self.obs_model.freq_scans = int(-((self.obs_model.freq_max - self.obs_model.freq_min)) // -(sample_rate * USABLE_BANDWIDTH))  # Ceiling division
            
            # Overlap in the frequency domain (Hz) rounded to 4 decimals
            self.obs_model.freq_overlap = round(((sample_rate * self.obs_model.freq_scans - (self.obs_model.freq_max-self.obs_model.freq_min))/(self.obs_model.freq_scans-1) if self.obs_model.freq_scans > 1 else 0),4)
            self.obs_model.freq_duration = round(self.obs_model.duration / self.obs_model.freq_scans,3) if self.obs_model.freq_scans > 0 else self.obs_model.duration
            self.obs_model.scan_iterations = int(np.ceil(self.obs_model.freq_duration / MAX_SCAN_DURATION))  # Number of iterations of a frequency scan, # e.g. 5*60 seconds in a freq scan will be 5 scans of 1 minute each
            self.obs_model.scan_duration = self.obs_model.freq_duration // self.obs_model.scan_iterations if self.obs_model.scan_iterations > 1 else self.obs_model.freq_duration  # Duration of each scan in seconds

    def schedule(self, start: datetime, end: datetime):
        """ Set the start and end times of the observation and check that timedelta is sufficient.
        Parameters:
            start (datetime): Scheduled start time of the observation.
            end (datetime): Scheduled end time of the observation.
        """
        duration = (end - start).total_seconds()

        if duration <= self.obs_modelduration * SCAN_OVERHEAD_FACTOR:
            error = f"Observation - scheduled observation {duration} seconds not enough to achieve target time {self.duration} seconds."
            logger.error(error)
            raise XSchedulerFailure(error)

        with self._rlock:
            self.obs_time.start = start
            self.obs_time.end = end

    @property
    def obs_time(self):
        """ Calculate the total scheduled observation time in seconds. """
        if self.obs_time.start is None or self.obs_time.end is None:
            return None
            
        return (self.obs_time.end - self.obs_time.start).total_seconds()

    def get_metadata(self) -> dict:
        """ Get the observation metadata as a dictionary.
        Returns:
            dict: Observation metadata including id, description, target, frequencies, durations, and scan parameters.
        """
        with self._rlock:

            scans_meta = {}
            scans_meta["scan_freqs"] = []  # Initialize the frequency scans list in the metadata structure

            for i in range(self.obs_model.freq_scans):

                scan_freq_meta = {
                    "scan_freq_num": i,
                    "scan_freq_min": self.obs_model.freq_min + (i * (self.obs_model.sample_rate - self.obs_model.freq_overlap)),
                    "scan_freq_ctr": self.obs_model.freq_min + (i * (self.obs_model.sample_rate - self.obs_model.freq_overlap)) + self.obs_model.sample_rate / 2,
                    "scan_freq_max": self.obs_model.freq_min + (i * (self.obs_model.sample_rate - self.obs_model.freq_overlap)) + self.obs_model.sample_rate,
                    "scan_freq_duration": self.obs_model.scan_duration * self.obs_model.scan_iterations,
                    "scan_iters": []
                }

                for j in range(self.obs_model.scan_iterations):
                    scan_iter_meta = {
                        "scan_iter_num": j,
                        "scan_iter_duration": self.obs_model.scan_duration
                    }
                    scan_freq_meta["scan_iters"].append(scan_iter_meta)  # Append the scan metadata to the scans list

                scans_meta["scan_freqs"].append(scan_freq_meta)  # Append the frequency scan metadata to the scan_freqs list

            obs_meta = self.obs_model.to_dict()
            obs_meta["scans"] = scans_meta["scan_freqs"]

            try:
                return json.dumps(obs_meta, indent=4, default=str)
            except Exception as e:
                logger.error(f"Observation - Error serializing metadata to JSON: {e}")

            return obs_meta
    
    def load_baselines(self, input_dir: str) -> bool:
        """
            Load baseline power spectrum data from CSV files in the input directory.
            A baseline is loaded for each frequency scan.
            If a baseline cannot be loaded for a frequency scan, return False else True
        """
        success = True

        if input_dir is None or input_dir == '':
            input_dir = "./"

        # Get metadata and iterate over frequency scans
        metadata = json.loads(self.get_metadata())
        # enumerate yields (index, element) so unpack as (idx, scan_freq)
        for idx, scan_freq in enumerate(metadata.get("scans", [])):

            # Format center frequency in MHz with two decimal places for filename tokens
            scan_freq_cf = f'-cf{scan_freq.get("scan_freq_ctr", 0)/1e6:.2f}'   # Center frequency token for matching files (MHz)
            scan_freq_du = f'-du{scan_freq.get("scan_freq_duration", 0):.0f}'  # Duration token for matching files
            scan_freq_bw = f'-bw{metadata.get("sample_rate", 0)/1e6:.2f}'      # Bandwidth token for matching files
            scan_freq_ch = f'-ch{metadata.get("channels", 0):.0f}'             # Channels token for matching files

            # Look for files in the input directory ending with 'load.csv' that match the center frequency, bandwidth and duration
            load_files = [f for f in os.listdir(input_dir) if scan_freq_cf in f and scan_freq_bw in f and scan_freq_du in f and scan_freq_ch in f and f.endswith('load.csv')]

            if load_files:
                load_file = sorted(load_files)[-1]  # Identify the most recent load file and use that one
                logger.info(
                    f"Observation {self.obs_model.obs_id} - Loading baseline power spectrum {load_file} with center freq {scan_freq_cf}, "
                    f"duration {scan_freq_du}, bandwidth {scan_freq_bw}, channels {scan_freq_ch}"
                )

                # load into the appropriate row for this frequency scan
                file_path = os.path.join(input_dir, load_file)
                self.ibsl[idx, :] = np.genfromtxt(file_path, delimiter=',')
            else:
                # fallback: fill with ones for this scan row
                self.ibsl[idx, :] = np.ones((self.obs_model.channels,))
                logger.warning(
                    f"Observation {self.obs_model.obs_id} - No load file for frequency scan {idx} in {input_dir}. "
                    f"First generate a load file with a freq scan {scan_freq_cf}, duration {scan_freq_du}, bandwidth {scan_freq_bw}, channels {scan_freq_ch}."
                )
                success = False

        return success

if __name__ == "__main__":

    # Setup logging configuration
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
        handlers=[
            logging.StreamHandler(),                  # Log to console
            logging.FileHandler("obs.log", mode="a")  # Log to a file
            ]
    )

    from astropy.coordinates import SkyCoord, ICRS
    # Example usage of the Observation class
    target_coord = SkyCoord(ra="00h42m44.3s", dec="+41d16m9s", frame=ICRS())  # Andromeda Galaxy
    target = TargetModel(name="Andromeda Galaxy", kind="sidereal", sky_coord=target_coord)
    observation = Observation(desc="Test Observation", target=target, center_freq=1420.4e6, bandwidth=2.4e6, sample_rate=2.4e6, channels=1024, duration=120.0)

    logger.info("-"*40)
    logger.info("Testing Sidereal Target:")
    logger.info("-"*40)

    # Pretty-print observation metadata for easier reading in logs
    print(observation.get_metadata())

    observation.load_baselines(input_dir=INPUT_DIR)
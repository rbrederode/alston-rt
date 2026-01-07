import enum
import logging
import math
import numpy as np
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

import astropy.units as u
from astropy.coordinates import get_body
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
from astropy.time import Time

from models.base import BaseModel
from models.dsh import Feed
from models.scan import ScanModel, ScanState
from util import log

logger = logging.getLogger(__name__)

USABLE_BANDWIDTH = 0.65     # Percentage of usable bandwidth for a scan
MAX_SCAN_DURATION_SEC = 60  # Maximum duration of a single scan in seconds

#=======================================
# Models comprising a Target (TARGET)
#=======================================

class PointingType(enum.IntEnum):
    """Python enumerated type for pointing types."""

    SIDEREAL_TRACK = 0        # Sidereal tracking (fixed RA/Dec) e.g. Andromeda Galaxy
    NON_SIDEREAL_TRACK = 1    # Solar system or satellite tracking e.g. planet, moon or Sun
    DRIFT_SCAN = 2            # Fixed Alt-azimuth target e.g. Zenith
    FIVE_POINT_SCAN = 3       # Center point and 4 offset points e.g. for beam mapping

class TargetModel(BaseModel):
    """A class representing a target model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetModel"),
        "tgt_idx": And(int, lambda v: v >= -1),                                          # Target list index (-1 = not set, 0-based)

        "id": Or(None, And(str, lambda v: isinstance(v, str))),                          # Target identifier e.g. "Sun", "Moon", "Mars", "Vega"
        "pointing": And(PointingType, lambda v: isinstance(v, PointingType)),            # Target type
        "sky_coord": Or(None, lambda v: v is None or isinstance(v, SkyCoord)),           # Sky coordinates (any frame)
        "altaz": Or(None, dict, lambda v: v is None or isinstance(v, (dict, SkyCoord))), # Alt-az coordinates (SkyCoord or AltAz)
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetModel",
            "tgt_idx": -1,                          # Target list index (-1 = not set, 0-based)

            "id": None,                             # Used for solar and lunar (and optionally sidereal) targets e.g. "Sun", "Moon", "Mars", "Vega"
            "pointing": PointingType.DRIFT_SCAN,    # Default to drift scan pointing
            "sky_coord": None,                      # Used for sidereal targets (ra,dec or l,b)
            "altaz": None,                          # Used for non-sidereal targets e.g. solar, terrestrial or satellite targets
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class TargetConfig(BaseModel):
    """A class representing a target configuration."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetConfig"),
        "tgt_idx": And(int, lambda v: v >= -1),                                 # Target list index (-1 = not set, 0-based)

        "feed": And(Feed, lambda v: isinstance(v, Feed)),                       # Feed enum
        "gain": And(Or(int, float), lambda v: v >= 0.0),                        # Gain (dBi)
        "center_freq": And(Or(int, float), lambda v: v >= 0.0),                 # Center frequency (Hz) 
        "bandwidth": And(Or(int, float), lambda v: v >= 0.0),                   # Bandwidth (Hz) 
        "sample_rate": And(Or(int, float), lambda v: v >= 0.0),                 # Sample rate (Hz) 
        "integration_time": And(Or(int, float), lambda v: v >= 0.0),            # Integration time (seconds)
        "spectral_resolution": And(int, lambda v: v >= 0),                      # Spectral resolution (fft size)
      })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetConfig",
            "tgt_idx": -1,                  # Target list index (-1 = not set, 0-based)

            "feed": Feed.NONE,              # Default to None feed
            "gain": 0.0,                    # Gain (dBi)
            "center_freq": 0.0,             # Center frequency (Hz) 
            "bandwidth": 0.0,               # Bandwidth (Hz) 
            "sample_rate": 0.0,             # Sample rate (Hz) 
            "integration_time": 0.0,        # Integration time (seconds)
            "spectral_resolution": 0,       # Spectral resolution (fft size)
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class TargetScanSet(BaseModel):
    """A class representing a set of scans for a particular target."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetScanSet"),
        "tgt_idx": And(int, lambda v: v >= -1),                                      # Target list index (-1 = not set, 0-based)

        # Below parameters are calculated based on the above parameters
        "freq_min": And(Or(None, float, int), lambda v: v is None or v >= 0.0),      # Start of frequency scanning (Hz)
        "freq_max": And(Or(None, float, int), lambda v: v is None or v >= 0.0),      # End of frequency scanning (Hz)
        "freq_scans": And(Or(None, int), lambda v: v is None or v >= 0),             # Number of frequency scans
        "freq_overlap": And(Or(None, float, int), lambda v: v is None or v >= 0.0),  # Overlap between frequency scans (Hz)
        "scan_iterations": And(Or(None, int), lambda v: v is None or v >= 0),        # Number of scan iterations (within a frequency scan)
        "scan_duration": And(Or(None, float, int), lambda v: v is None or v >= 0.0), # Duration of each scan (seconds)

        "scans": And(list, lambda v: isinstance(v, list)),                           # List of scans to be performed for this target
      })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetScanSet",
            "tgt_idx": -1,                  # Target list index (-1 = not set, 0-based)

            "freq_min": None,               # Start of frequency scanning (Hz)
            "freq_max": None,               # End of frequency scanning (Hz)
            "freq_scans": 0,                # Number of frequency scans
            "freq_overlap": None,           # Overlap between frequency scans (Hz)
            "scan_iterations": 0,           # Number of scan iterations (within a frequency scan)
            "scan_duration": None,          # Duration of each scan (seconds)
            "scans": [],                    # List of scans for this target
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

    def get_scan_by_index(self, freq_scan: int, scan_iter: int) -> ScanModel:
        """Retrieve a scan by its frequency scan and scan iteration indices."""

        if freq_scan is None or freq_scan < 0 or freq_scan >= self.freq_scans:
            return None

        if scan_iter is None or scan_iter < 0 or scan_iter >= self.scan_iterations:
            return None

        # Total scans = freq_scans * scan_iterations
        idx = freq_scan * self.scan_iterations + scan_iter
        if idx < 0 or idx >= len(self.scans):
            return None

        return self.scans[idx] 

    def get_scan_by_id(self, scan_id: str) -> ScanModel:
        """Retrieve a scan by its unique scan_id."""
        # Scan_id should be in the form: <obs_id>-<target_index>-<freq_scan>-<scan_iter>
        if scan_id is None or not isinstance(scan_id, str):
            return None

        # Split the scan_id to extract target, freq_scan and scan_iter indices
        try:
            tgt_index = int(scan_id.split("-")[-3])
            freq_scan = int(scan_id.split("-")[-2])
            scan_iter = int(scan_id.split("-")[-1])

            if tgt_index != self.tgt_idx:
                return None # Target index does not match

            return self.get_scan_by_index(freq_scan, scan_iter) 

        except Exception as e:
            # Use brute force method if parsing scan_id fails
            for scan in self.scans:
                if scan.scan_id == scan_id:
                    return scan
        return None

    def determine_scans(self, obs_id: str, tgt_config: TargetConfig):
        """
        Calculate the number of frequency scans needed to cover the target frequency range from start_freq to end_freq
        and the overlap in the frequency domain. NOTE: The overlap is different to the non-usable bandwidth !!
        
        Calculate the number of scan iterations and scan duration to keep the scan duration within MAX_SCAN_DURATION_SEC
        i.e. manageable from a performance perspective.

        E.g. We may need 10 scans of 1 minute each to cover the frequency range from start_freq to end_freq,
        where each scan is iterated 5 times to cover the duration of 5 minutes per frequency scan.

        :param tgt_config: TargetConfig object
        """
        if tgt_config is None or tgt_config.tgt_idx == -1:
            logger.error(f"Target cannot determine scans for obs_id={obs_id}: TargetConfig is not valid {tgt_config}")
            return

        self.tgt_idx = tgt_config.tgt_idx
        self.freq_min = tgt_config.center_freq - tgt_config.bandwidth / 2 - tgt_config.sample_rate * (1-USABLE_BANDWIDTH)/2  # Start of frequency scanning
        self.freq_max = tgt_config.center_freq + tgt_config.bandwidth / 2 + tgt_config.sample_rate * (1-USABLE_BANDWIDTH)/2  # End of frequency scanning

        logger.info(f"Target determining scans for TargetConfig idx={self.tgt_idx} from {self.freq_min/1e6:.2f} MHz to {self.freq_max/1e6:.2f} MHz with Sample Rate: {tgt_config.sample_rate/1e6:.2f} MHz and Duration: {tgt_config.integration_time} sec(s)")

        # Calculate the number of frequency scans to cover the bandwidth (ceiling of bandwidth/sample_rate)
        self.freq_scans = int(-((self.freq_max-self.freq_min)) // -(tgt_config.sample_rate * USABLE_BANDWIDTH))  # Ceiling division
        self.freq_overlap = round((tgt_config.sample_rate * self.freq_scans - (self.freq_max-self.freq_min))/(self.freq_scans-1) if self.freq_scans > 1 else 0,4) # Overlap in the frequency domain (Hz) rounded to 4 decimals
        self.scan_iterations = int(np.ceil(tgt_config.integration_time / MAX_SCAN_DURATION_SEC))  # Number of iterations of a frequency scan, # e.g. 5 minutes of data will be 5 scans of 1 minute each
        self.scan_duration = math.ceil(tgt_config.integration_time / self.scan_iterations) if self.scan_iterations > 1 else tgt_config.integration_time  # Duration of each scan in seconds

        logger.info(f"Target Frequency-Iteration Scans: {self.freq_scans}-{self.scan_iterations} each of Scan Duration: {self.scan_duration} sec(s)")
        logger.info(f"Target Sample Rate: {tgt_config.sample_rate} Hz, Overlap: {self.freq_overlap:.2f} Hz")
        
        # Initialise the scans list
        self.scans = []
  
        for i in range(self.freq_scans * self.scan_iterations):
            freq_scan = i // self.scan_iterations               # Current frequency scan number
            scan_iter = i % self.scan_iterations                # Current iteration within the frequency scan

            # Calculate the start, end and center frequencies for each scan
            scan_start_freq = self.freq_min + (freq_scan * (tgt_config.sample_rate - self.freq_overlap)) 
            scan_end_freq = scan_start_freq + tgt_config.sample_rate
            scan_center_freq = scan_start_freq + tgt_config.sample_rate / 2

            scan = ScanModel(
                obs_id=obs_id if obs_id is not None else '<undefined>',
                tgt_index=self.tgt_idx,
                freq_scan=freq_scan,
                scan_iter=scan_iter,
                dig_id=None,
                duration=self.scan_duration,
                sample_rate=tgt_config.sample_rate,
                channels=tgt_config.spectral_resolution,
                start_freq=scan_start_freq,
                center_freq=scan_center_freq,
                end_freq=scan_end_freq,
                gain=tgt_config.gain,
                status=ScanState.EMPTY,
                last_update=datetime.now(timezone.utc)
            )
            self.scans.append(scan)  # Append the scan to the scans list

if __name__ == "__main__":

    import pprint

    coord = SkyCoord(ra="18h36m56.33635s", dec="+38d47m01.2802s", frame="icrs")
    altaz = {"alt": 45.0*u.deg, "az": 180.0*u.deg}

    target001 = TargetModel(
        id="Vega",
        pointing=PointingType.SIDEREAL_TRACK,
        sky_coord=coord,
        altaz=None
    )
    print('='*40)
    print("Target Model: Sidereal Target")
    print('='*40)
    pprint.pprint(target001.to_dict())

    target002 = TargetModel(
        id="Ground Station Alpha",
        pointing=PointingType.DRIFT_SCAN,
        sky_coord=None,
        altaz=altaz
    )
    print('='*40)
    print("Target Model: Terrestrial Target")
    print('='*40)   
    pprint.pprint(target002.to_dict())

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('='*40)   

    dt = datetime.now(timezone.utc)
    location = EarthLocation(lat=45.67*u.deg, lon=-111.05*u.deg, height=1500*u.m)

    moon_icrs = get_body('moon', Time(dt), location)
    altaz_frame = AltAz(obstime=Time(dt), location=location)
    altaz = moon_icrs.transform_to(altaz_frame)

    print("Computed AltAz for Moon at", dt.isoformat())

    target003 = TargetModel(
        id="Moon",
        pointing=PointingType.NON_SIDEREAL_TRACK,
        sky_coord=None,
        altaz={"alt": altaz.alt, "az": altaz.az}
        )

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('='*40)

    pprint.pprint(target003.to_dict())

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('Tests from_dict method')
    print('='*40)

    target004 = TargetModel()
    target004 = target004.from_dict(target003.to_dict())

    pprint.pprint(target004.to_dict())

    print('='*40)
    print("Target Config: Vega Target")
    print('='*40)
    target_config001 = TargetConfig(
        target=target001,
        feed=Feed.H3T_1420,
        gain=12.0,
        center_freq=1.42e9,
        bandwidth=2e6,
        sample_rate=2.0e6,
        integration_time=300,
        spectral_resolution=1024,
        index=0
    )
    pprint.pprint(target_config001.to_dict())
    target_config001.determine_scans()
    print(f"Determined Scans: freq_scans={target_config001.freq_scans}, freq_overlap={target_config001.freq_overlap} Hz, scan_iterations={target_config001.scan_iterations}, scan_duration={target_config001.scan_duration} sec(s)")
    pprint.pprint(target_config001.to_dict())
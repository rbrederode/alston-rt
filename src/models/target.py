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
        "id": Or(None, And(str, lambda v: isinstance(v, str))),                     # Target identifier
        "pointing": And(PointingType, lambda v: isinstance(v, PointingType)),               # Target type
        "sky_coord": Or(None, lambda v: v is None or isinstance(v, SkyCoord)),      # Sky coordinates (any frame)
        "altaz": Or(None, dict, lambda v: v is None or isinstance(v, (dict, SkyCoord))), # Alt-az coordinates (SkyCoord or AltAz)
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetModel",
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
    """A class representing a target and associated configuration."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetConfig"),
        "target": And(TargetModel, lambda v: isinstance(v, TargetModel)),       # Target object
        "feed": And(Feed, lambda v: isinstance(v, Feed)),                       # Feed enum
        "gain": And(Or(int, float), lambda v: v >= 0.0),                        # Gain (dBi)
        "center_freq": And(Or(int, float), lambda v: v >= 0.0),                 # Center frequency (Hz) 
        "bandwidth": And(Or(int, float), lambda v: v >= 0.0),                   # Bandwidth (Hz) 
        "sample_rate": And(Or(int, float), lambda v: v >= 0.0),                 # Sample rate (Hz) 
        "integration_time": And(Or(int, float), lambda v: v >= 0.0),            # Integration time (seconds)
        "spectral_resolution": And(int, lambda v: v >= 0),                      # Spectral resolution (fft size)
        "index": And(int, lambda v: v >= -1),                                   # Target index (-1 = not set, 0 = first etc)

        # Below parameters are calculated based on the above parameters
        "freq_min": And(Or(None, float, int), lambda v: v is None or v >= 0.0),      # Start of frequency scanning (Hz)
        "freq_max": And(Or(None, float, int), lambda v: v is None or v >= 0.0),      # End of frequency scanning (Hz)
        "freq_scans": And(Or(None, int), lambda v: v is None or v >= 0),             # Number of frequency scans
        "freq_overlap": And(Or(None, float, int), lambda v: v is None or v >= 0.0),  # Overlap between frequency scans (Hz)
        "scan_iterations": And(Or(None, int), lambda v: v is None or v >= 0),        # Number of scan iterations (within a frequency scan)
        "scan_duration": And(Or(None, float, int), lambda v: v is None or v >= 0.0), # Duration of each scan (seconds)

        "scans": And(list, lambda v: isinstance(v, list)),                      # List of scans to be performed for this target
      })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetConfig",
            "target": TargetModel(),        # Target object
            "feed": Feed.NONE,              # Default to None feed
            "gain": 0.0,                    # Gain (dBi)
            "center_freq": 0.0,             # Center frequency (Hz) 
            "bandwidth": 0.0,               # Bandwidth (Hz) 
            "sample_rate": 0.0,             # Sample rate (Hz) 
            "integration_time": 0.0,        # Integration time (seconds)
            "spectral_resolution": 0,       # Spectral resolution (fft size)
            "index": -1,                    # Target index (-1 = not set, 0 = first etc)

            "freq_min": None,               # Start of frequency scanning (Hz)
            "freq_max": None,               # End of frequency scanning (Hz)
            "freq_scans": None,             # Number of frequency scans
            "freq_overlap": None,           # Overlap between frequency scans (Hz)
            "scan_iterations": None,        # Number of scan iterations (within a frequency scan)
            "scan_duration": None,          # Duration of each scan (seconds)
            "scans": [],                    # List of scans to be performed for this target
 
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

    def determine_scans(self, obs=None):
        """
        Calculate the number of frequency scans needed to cover the frequency range from start_freq to end_freq
        and the overlap in the frequency domain. NOTE: The overlap is different to the non-usable bandwidth !!
        
        Calculate the number of scan iterations and scan duration to keep the scan duration within MAX_SCAN_DURATION_SEC
        i.e. manageable from a performance perspective.

        E.g. We may need 10 scans of 1 minute each to cover the frequency range from start_freq to end_freq,
        where each scan is iterated 5 times to cover the duration of 5 minutes per frequency scan.

        :param obs: Observation object
        """

        self.freq_min = self.center_freq - self.bandwidth / 2 - self.sample_rate * (1-USABLE_BANDWIDTH)/2  # Start of frequency scanning
        self.freq_max = self.center_freq + self.bandwidth / 2 + self.sample_rate * (1-USABLE_BANDWIDTH)/2  # End of frequency scanning

        logger.info(f"Determining scans for TargetConfig id={self.target.id} from {self.freq_min/1e6:.2f} MHz to {self.freq_max/1e6:.2f} MHz with Sample Rate: {self.sample_rate/1e6:.2f} MHz and Duration: {self.integration_time} sec(s)")

        # Calculate the number of frequency scans to cover the bandwidth (ceiling of bandwidth/sample_rate)
        self.freq_scans = int(-((self.freq_max-self.freq_min)) // -(self.sample_rate * USABLE_BANDWIDTH))  # Ceiling division
        self.freq_overlap = round((self.sample_rate * self.freq_scans - (self.freq_max-self.freq_min))/(self.freq_scans-1) if self.freq_scans > 1 else 0,4) # Overlap in the frequency domain (Hz) rounded to 4 decimals
        self.scan_iterations = int(np.ceil(self.integration_time / MAX_SCAN_DURATION_SEC))  # Number of iterations of a frequency scan, # e.g. 5 minutes of data will be 5 scans of 1 minute each
        self.scan_duration = math.ceil(self.integration_time / self.scan_iterations) if self.scan_iterations > 1 else self.integration_time  # Duration of each scan in seconds

        logger.info(f"Frequency Scans-Iterations: {self.freq_scans}-{self.scan_iterations} each of Scan Duration: {self.scan_duration} sec(s)")
        logger.info(f"Sample Rate: {self.sample_rate} Hz, Overlap: {self.freq_overlap:.2f} Hz")
        
        # Create the scans list
        self.scans = []
  
        for i in range(self.freq_scans * self.scan_iterations):
            freq_scan = i // self.scan_iterations        # Current frequency scan number
            scan_iter = i % self.scan_iterations        # Current iteration within the frequency scan
            # Calculate the start, end and center frequencies for each scan
            scan_start_freq = self.freq_min + (freq_scan * (self.sample_rate - self.freq_overlap)) 
            scan_end_freq = scan_start_freq + self.sample_rate
            scan_center_freq = scan_start_freq + self.sample_rate / 2

            scan = ScanModel(
                tgt_index=self.index,
                freq_scan=freq_scan,
                scan_iter=scan_iter,
                obs_id=obs.obs_id if obs is not None else None,
                dig_id=None,
                duration=self.scan_duration,
                sample_rate=self.sample_rate,
                channels=self.spectral_resolution,
                start_freq=scan_start_freq,
                center_freq=scan_center_freq,
                end_freq=scan_end_freq,
                gain=self.gain,
                status=ScanState.EMPTY,
                last_update=datetime.now(timezone.utc)
            )
            self.scans.append(scan)  # Append the scan metadata to the scans list

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
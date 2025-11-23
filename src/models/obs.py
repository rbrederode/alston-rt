import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel
from models.target import TargetModel, TargetType

#=======================================
# Models comprising an Observation (OBS)
#=======================================

class ObsState(enum.IntEnum):
    """Python enumerated type for observing state."""

    EMPTY = 0
    """The sub-array has no resources allocated and is unconfigured."""

    RESOURCING = 1
    """
    Resources are being allocated to, or deallocated from, the subarray.

    In normal science operations these will be the resources required
    for the upcoming SBI execution.

    This may be a complete de/allocation, or it may be incremental. In
    both cases it is a transient state; when the resourcing operation
    completes, the subarray will automatically transition to EMPTY or
    IDLE, according to whether the subarray ended up having resources or
    not.

    For some subsystems this may be a very brief state if resourcing is
    a quick activity.
    """

    IDLE = 2
    """The subarray has resources allocated but is unconfigured."""

    CONFIGURING = 3
    """
    The subarray is being configured for an observation.

    This is a transient state; the subarray will automatically
    transition to READY when configuring completes normally.
    """

    READY = 4
    """
    The subarray is fully prepared to scan, but is not scanning.

    It may be tracked, but it is not moving in the observed coordinate
    system, nor is it taking data.
    """

    SCANNING = 5
    """
    The subarray is scanning.

    It is taking data and, if needed, all components are synchronously
    moving in the observed coordinate system.

    Any changes to the sub-systems are happening automatically (this
    allows for a scan to cover the case where the phase centre is moved
    in a pre-defined pattern).
    """

    ABORTING = 6
    """The subarray has been interrupted and is aborting what it was doing."""

    ABORTED = 7
    """The subarray is in an aborted state."""

    RESETTING = 8
    """The subarray device is resetting to a base (EMPTY or IDLE) state."""

    FAULT = 9
    """The subarray has detected an error in its observing state."""

    RESTARTING = 10
    """
    The subarray device is restarting.

    After restarting, the subarray will return to EMPTY state, with no
    allocated resources and no configuration defined.
    """

    COMPLETED = 11
    """The subarray has completed the observation successfully."""

class ObsModel(BaseModel):
    """A class representing a model of an observation"""

    schema = Schema({
        "_type": And(str, lambda v: v == "ObsModel"),
        "obs_id": And(str, lambda v: isinstance(v, str)),                       # Unique identifier
        "short_desc": And(str, lambda v: isinstance(v, str)),                   # Short description (255 chars) 
        "long_desc": And(str, lambda v: isinstance(v, str)),                    # Long description (no strict upper limit)
        "state": And(ObsState, lambda v: isinstance(v, ObsState)),

        # Array of target models and durations corresponding to each target
        "targets": And(list, lambda v: all(isinstance(item, TargetModel) for item in v)),    # List of target models
        "target_durations": And(list, lambda v: all(isinstance(item, float) for item in v)), # List of target durations (seconds)

        "dsh_id": And(str, lambda v: isinstance(v, str)),                       # Dish identifier e.g. "dish001"

        "center_freq": And(float, lambda v: v >= 0.0),                          # Center frequency (Hz) 
        "bandwidth": And(float, lambda v: v >= 0.0),                            # Bandwidth (Hz) 
        "sample_rate": And(float, lambda v: v >= 0.0),                          # Sample rate (Hz) 
        "channels": And(int, lambda v: v >= 0),                                 # Number of channels (fourier transform fft_size) 
        
        "freq_min": And(Or(None, float), lambda v: v is None or v >= 0.0),      # Start of frequency scanning (Hz)
        "freq_max": And(Or(None, float), lambda v: v is None or v >= 0.0),      # End of frequency scanning (Hz)
        "freq_scans": And(Or(None, int), lambda v: v is None or v >= 0),        # Number of frequency scans
        "freq_overlap": And(Or(None, float), lambda v: v is None or v >= 0.0),  # Overlap between frequency scans (Hz)
        "freq_duration": And(Or(None, float), lambda v: v is None or v >= 0.0), # Duration of each frequency scan (seconds)
        "scan_iterations": And(Or(None, int), lambda v: v is None or v >= 0),   # Number of scan iterations (within a frequency scan)
        "scan_duration": And(Or(None, float), lambda v: v is None or v >= 0.0), # Duration of each scan (seconds)

        "scans": And(list, lambda v: isinstance(v, list)),                      # List of scans to be performed for this observation

        "start_dt": And(datetime, lambda v: isinstance(v, datetime)),           # Start datetime (UTC) of the observation 
        "end_dt": And(datetime, lambda v: isinstance(v, datetime)),             # End datetime (UTC) of the observation

        # Calibration files to be used by this observation
        "tsys_calibrators": And(list, lambda v: isinstance(v, list)),           # List of *tsys.csv (system temperature calibration) filenames
        "gain_calibrators": And(list, lambda v: isinstance(v, list)),           # List of *gain.csv (gain calibration) filenames
        "load_calibrators": And(list, lambda v: isinstance(v, list)),           # List of *load.csv (terminated signal chain) filenames

        # Summed power scan files generated during this observation
        "spr_scans": And(list, lambda v: isinstance(v, list)),                  # List of *spr.csv (summed power) filenames

        "last_update": And(datetime, lambda v: isinstance(v, datetime)),        # Last update datetime (UTC) of the observation
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "ObsModel",
            "obs_id": "<undefined>",
            "short_desc": "",
            "long_desc": "",
            "state": ObsState.EMPTY,
            "targets": [],
            "target_durations": [],
            "dsh_id": "<undefined>",
            "center_freq": 0.0,
            "bandwidth": 0.0,
            "sample_rate": 0.0,
            "channels": 0,
            "freq_min": None,
            "freq_max": None,
            "freq_scans": None,
            "freq_overlap": None,
            "freq_duration": None,
            "scan_iterations": None,
            "scan_duration": None,
            "scans": [],
            "start_dt": datetime.now(timezone.utc),
            "end_dt": datetime.now(timezone.utc),
            "tsys_calibrators": [],
            "gain_calibrators": [],
            "load_calibrators": [],
            "spr_scans": [],
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":

    from astropy.coordinates import SkyCoord
    import astropy.units as u
    
    obs001 = ObsModel(
        obs_id="obs001",
        short_desc="Test Observation",
        long_desc="This is a test observation of a celestial target.",
        state=ObsState.EMPTY,
        targets=[
            TargetModel(
                name="Test Target",
                type=TargetType.SIDEREAL,
                sky_coord=SkyCoord(ra=180.0*u.deg, dec=-45.0*u.deg, frame='icrs'),
            )
        ],
        target_durations=[120.0],
        dsh_id="dish001",
        center_freq=1420000000.0,
        bandwidth=20000000.0,
        sample_rate=4000000.0,
        channels=1024,
        start_dt=datetime.now(timezone.utc),
        end_dt=datetime.now(timezone.utc),
        last_update=datetime.now(timezone.utc)
    )

    import pprint
    print("="*40)
    print("Observation Model Test")
    print("="*40)
    pprint.pprint(obs001.to_dict())

    obs002 = ObsModel()
    print("="*40)
    print("Observation Model with Defaults Test")
    print("="*40)
    pprint.pprint(obs002.to_dict())
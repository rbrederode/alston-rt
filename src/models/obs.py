import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel
from models.target import TargetModel

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

class ObsModel(BaseModel):
    """A class representing a model of an observation"""

    schema = Schema({
        "_type": And(str, lambda v: v == "ObsModel"),
        "obs_id": And(str, lambda v: isinstance(v, str)),                   # Unique identifier
        "short_desc": And(str, lambda v: isinstance(v, str)),               # Short description (255 chars) 
        "long_desc": And(str, lambda v: isinstance(v, str)),                # Long description (no strict upper limit)
        "state": And(ObsState, lambda v: isinstance(v, ObsState)),

        "target": And(TargetModel, lambda v: isinstance(v, TargetModel)),   # Target model

        "center_freq": And(int, lambda v: v >= 0),                          # Center frequency (Hz) 
        "bandwidth": And(int, lambda v: v >= 0),                            # Bandwidth (Hz) 
        "sample_rate": And(int, lambda v: v >= 0),                          # Sample rate (Hz) 
        "channels": And(int, lambda v: v >= 0),                             # Number of channels (fourier transform fft_size) 
        "duration": And(int, lambda v: v >= 0),                             # Duration (s) of the observation
        "start_dt": And(datetime, lambda v: isinstance(v, datetime)),       # Start datetime (UTC) of the observation 
        "end_dt": And(datetime, lambda v: isinstance(v, datetime)),         # End datetime (UTC) of the observation

        # Calibration files to be used by this observation
        "tsys_calibrators": And(list, lambda v: isinstance(v, list)),       # List of *tsys.csv (system temperature calibration) filenames
        "gain_calibrators": And(list, lambda v: isinstance(v, list)),       # List of *gain.csv (gain calibration) filenames
        "load_calibrators": And(list, lambda v: isinstance(v, list)),       # List of *load.csv (terminated signal chain) filenames

        # Summed power scan files generated during this observation
        "spr_scans": And(list, lambda v: isinstance(v, list)),              # List of *spr.csv (summed power) filenames

        "last_update": And(datetime, lambda v: isinstance(v, datetime)),    # Last update datetime (UTC) of the observation
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "ObsModel",
            "spr_files": [],
            "load_files": [],
            "tsys_files": [],
            "gain_files": [],
            "meta_files": [],
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)
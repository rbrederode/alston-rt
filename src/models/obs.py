import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel
from models.target import TargetModel, PointingType
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

# A scheduling block is the minimum time allocation of resources to an observation
# For example, if an observation requires 90 minutes, and the scheduling block size is 60 minutes,
# then the observation will be allocated 2 scheduling blocks (120 minutes) to ensure sufficient time
# for the observation. Dishes (and other resources) will be booked in increments of the scheduling block size.
SCHEDULING_BLOCK_SIZE = 60  # in minutes

#=======================================
# Models comprising an Observation (OBS)
#=======================================

class ObsState(enum.IntEnum):
    """Python enumerated type for observing state."""

    EMPTY = 0
    """The observation is not resourced."""

    IDLE = 2
    """The observation is sitting idle, resources are allocated or deallocated in this state.
    """

    CONFIGURING = 3
    """
    The resources allocated to an observation are being configured.

    This is a transient state; the observation will automatically
    transition to READY when configuring completes normally.
    """

    READY = 4
    """
    The observation is fully prepared to scan, but is not scanning.
    It may be tracking but is not taking data.
    """

    SCANNING = 5
    """
    The observation is scanning.

    It is taking data and, if needed, all subsystems are synchronously
    moving in the observed coordinate system.

    Any changes to the subsystems are happening automatically (this
    allows for a scan to cover the case where the phase centre is moved
    in a pre-defined pattern).
    """

    ABORTED = 6
    """The observation is in an aborted state. It will need to be reset."""

    FAULT = 9
    """The observation has encountered an error."""

class ObsTransition (enum.IntEnum):
    """Python enumerated type for observation workflow transitions."""

    START = 1
    ASSIGN_RESOURCES = 2
    RELEASE_RESOURCES = 3
    CONFIGURE_RESOURCES = 4
    CONFIGURE_ABORTED = 5
    READY = 6
    SCAN_STARTED = 7
    SCAN_COMPLETED = 8
    SCAN_ENDED = 9
    ABORT = 10
    FAULT_OCCURRED = 11
    RESET = 12

class Observation(BaseModel):
    """A class representing a model of an observation"""

    schema = Schema({
        "_type": And(str, lambda v: v == "Observation"),
        "obs_id": And(Or(None, str), lambda v: v is None or isinstance(v, str)),# Unique identifier
        "title": And(str, lambda v: isinstance(v, str)),                        # Short description (255 chars) 
        "description": And(str, lambda v: isinstance(v, str)),                  # Description (no strict upper limit)
        "obs_state": And(ObsState, lambda v: isinstance(v, ObsState)),

        "target_configs": And(list, lambda v: isinstance(v, list)),             # List of targets and associated configurations
        "next_tgt_index": And(int, lambda v: isinstance(v, int)),               # Index of the next target config to be observed (0-based)
        "next_tgt_scan": And(int, lambda v: isinstance(v, int)),                # Index of the next scan (in the target config) to be observed (0-based)

        "dish_id": And(Or(None, str), lambda v: v is None or isinstance(v, str)),# Dish identifier e.g. "dish001"
        "capabilities": And(str, lambda v: isinstance(v, str)),                 # Dish capabilities e.g. "Drift Scan over Zenith"
        "diameter": And(Or(int, float), lambda v: v >= 0.0),                    # Dish diameter (meters)
        "f/d_ratio": And(Or(int, float), lambda v: v >= 0.0),                   # Dish focal length to diameter ratio
        "latitude": And(Or(int, float), lambda v: -90.0 <= v <= 90.0),          # Dish latitude (degrees)
        "longitude": And(Or(int, float), lambda v: -180.0 <= v <= 180.0),       # Dish longitude (degrees)

        "total_integration_time": And(Or(int, float), lambda v: v >= 0.0),      # Total integration time (seconds)
        "estimated_slewing_time": And(Or(int, float), lambda v: v >= 0.0),      # Estimated slewing time (seconds)
        "estimated_observation_duration": And(str, lambda v: isinstance(v, str)),   # Estimated observation duration (HH:MM:SS)
        "scheduling_block_start": And(Or(None, datetime), lambda v: v is None or isinstance(v, datetime)), # Scheduling block start datetime (UTC)
        "scheduling_block_end": And(Or(None, datetime), lambda v: v is None or isinstance(v, datetime)),   # Scheduling block end datetime (UTC)

        "created": And(Or(None, datetime), lambda v: v is None or isinstance(v, datetime)),  # Creation datetime (UTC)
        "user_email": And(str, lambda v: isinstance(v, str)),                   # User email that created the observation

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

    allowed_transitions = {
        "obs_state": {
            ObsState.EMPTY: {ObsState.EMPTY, ObsState.IDLE, ObsState.FAULT, ObsState.ABORTED},
            ObsState.IDLE: {ObsState.IDLE,ObsState.CONFIGURING, ObsState.FAULT, ObsState.ABORTED},
            ObsState.CONFIGURING: {ObsState.CONFIGURING,ObsState.READY, ObsState.FAULT, ObsState.ABORTED},
            ObsState.READY: {ObsState.CONFIGURING, ObsState.READY, ObsState.SCANNING, ObsState.IDLE, ObsState.FAULT, ObsState.ABORTED},
            ObsState.SCANNING: {ObsState.READY, ObsState.FAULT, ObsState.ABORTED},
            ObsState.ABORTED: {ObsState.ABORTED, ObsState.IDLE},
        }
    }

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "Observation",
            "obs_id": None,
            "title": "",
            "description": "",
            "obs_state": ObsState.EMPTY,
            "target_configs": [],
            "next_tgt_index": 0,
            "next_tgt_scan": 0,
            "dish_id": None,
            "capabilities": "",
            "diameter": 0.0,
            "f/d_ratio": 0.0,
            "latitude": 0.0,
            "longitude": 0.0,
            "total_integration_time": 0.0,
            "estimated_slewing_time": 0.0,
            "estimated_observation_duration": "00:00:00",
            "scheduling_block_start": None,
            "scheduling_block_end": None,
            "created": None,
            "user_email": "",

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

    def save_to_disk(self, output_dir) -> bool:
        """
        Flush the observation to a file on disk.
            :param output_dir: Directory where the file will be saved
            :returns: True if the data was saved successfully, False otherwise
        """
        filename = f"{self.obs_id}-obs.json"

        try:
            super().save_to_disk(output_dir, filename)
            return True
        except XAPIValidationFailed as e:
            raise XSoftwareFailure(f"Failed to save Observation {self.obs_id} to disk due to validation error: {e}")
        except Exception as e:
            raise XSoftwareFailure(f"Failed to save Observation {self.obs_id} to disk due to unexpected error: {e}")

    def __str__(self):
        return f"Observation(obs_id={self.obs_id}, obs_state={self.obs_state.name})"

                
if __name__ == "__main__":

    from astropy.coordinates import SkyCoord
    import astropy.units as u
    import pprint

    obs_dict = {'_type': 'Observation', 'dish_id': 'Dish002', 'capabilities': 'Drift Scan over Zenith', 'diameter': 3, 'f/d_ratio': 1.3, 'latitude': 53.2421, 'longitude': -2.3067, 'total_integration_time': 60, 'estimated_slewing_time': 30, 'estimated_observation_duration': '00:01:30', 'scheduling_block_start': {'_type': 'datetime', 'value': '2025-12-07T19:00:00.000Z'}, 'scheduling_block_end': {'_type': 'datetime', 'value': '2025-12-07T20:00:00.000Z'}, 'obs_id': '2025-12-07T19:00Z-Dish002', 'obs_state': {'_type': 'enum.IntEnum', 'instance': 'ObsState', 'value': 'EMPTY'}, 'target_configs': [{'_type': 'TargetConfig', 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'H3T_1420'}, 'gain': 12, 'center_freq': 1420400000, 'bandwidth': 1000000, 'sample_rate': 2400000, 'target': {'_type': 'TargetModel', 'sky_coord': {'_type': 'SkyCoord', 'frame': 'icrs', 'ra': 204.2538, 'dec': -29.8658}, 'id': 'M83', 'type': {'_type': 'enum.IntEnum', 'instance': 'PointingType', 'value': 'SIDEREAL_TRACK'}}, 'integration_time': 60, 'spectral_resolution': 128, 'target_id': 1}], 'user_email': 'ray.brederode@skao.int', 'created': {'_type': 'datetime', 'value': '2025-12-07T18:19:37.503Z'}}
    obs000 = Observation().from_dict(obs_dict)
    print("="*40)
    print("Observation Model from Dict Test")
    print("="*40)
    pprint.pprint(obs000.to_dict())

    print("="*40)
    print("ObsState valid transition test")
    print("="*40)

    obsx = Observation()
    obsx.obs_state = ObsState.EMPTY
    print(f"Initial obs_state: {obsx.obs_state.name}")
    try:
        obsx.obs_state = ObsState.RESOURCING
        print(f"Updated obs_state: {obsx.obs_state.name}")
        obsx.obs_state = ObsState.IDLE
        print(f"Updated obs_state: {obsx.obs_state.name}")
        obsx.obs_state = ObsState.CONFIGURING
        print(f"Updated obs_state: {obsx.obs_state.name}")
        obsx.obs_state = ObsState.READY
        print(f"Updated obs_state: {obsx.obs_state.name}")
        obsx.obs_state = ObsState.SCANNING
        print(f"Updated obs_state: {obsx.obs_state.name}")
    except XInvalidTransition as e:
        print(f"Caught expected exception on invalid transition: {e}")

    print("="*40)
    print("ObsState invalid transition test")
    print("="*40)
    
    print(f"Current obs_state: {obsx.obs_state.name}")
    try:
        obsx.obs_state = ObsState.IDLE  # Invalid transition from SCANNING to IDLE
        print(f"Updated obs_state: {obsx.obs_state.name}")
    except XInvalidTransition as e:
        print(f"Caught expected exception on invalid transition: {e}")

    obs002 = Observation()
    print("="*40)
    print("Observation Model with Defaults Test")
    print("="*40)
    pprint.pprint(obs002.to_dict())

    obs001 = Observation().from_dict(obs_dict)

    print("="*40)
    print("Observation Model Test")
    print("="*40)
    pprint.pprint(obs001.to_dict())

  
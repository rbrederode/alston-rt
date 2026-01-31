# -*- coding: utf-8 -*-

import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from astropy.coordinates import EarthLocation, AltAz
import astropy.units as u
from astropy.time import Time

from models.app import AppModel
from models.base import BaseModel
from models.comms import CommunicationStatus
from models.health import HealthState
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

# Definition: Mode
# A mode dictates the behaviour of the system. For example when in the OPERATE mode, a dish shall capture
# and transmit signal data and execute pointing commands received from Dish LMC.

# Definition: State
# A state is a fixed setting. For example in the LOW power state, all sub-elements go into a low power
# state to power only the essential equipment.

# If I am in Exercising Mode there a certain types of exercise I could be doing and this would be the state
# I am in such as running or riding a bike or lifting weights.

# If I am in Resting Mode there are a different set of states I would be allowed to be in such
# as sitting or lying down.


# Pointing States are only relevant when the dish is in OPERATE_FULL or OPERATE_DEGRADED capability states
class PointingState(enum.IntEnum):
    READY = 0
    SLEW = 1
    TRACK = 2
    SCAN = 3
    UNKNOWN = 4

class DishMode(enum.IntEnum):
    STARTUP = 0             # Transitional: Reported when power is restored to the dish, perform initial checks and generally transition to STANDBY
    SHUTDOWN = 1            # Non-transitional: To ensure dish is safe before power loss (for a planned outage or UPS trigger), power on should set to STARTUP
    STANDBY_LP = 2          # Non-transitional: Dish is paritally powered e.g. running on a UPS, generally transition to SHUTDOWN or STANDBY_FP from here
    STANDBY_FP = 3          # Non-transitional: Dish is fully powered and can prepare for an observation, generally transition to CONFIG from here
    MAINTENANCE = 4         # Non-transitional: Stow the dish to make it safe for maintenance activities, remain in maintenance until explicitly changed to another mode
    STOW = 5                # Non-transitional: Stow the dish to a safe position, generally transition to STANDBY after stowing
    CONFIG = 6              # Configure the dish before observations e.g. switching a feed, generally transition to OPERATE (by TM)
    OPERATE = 7             # Transitional: Actively observe targets as directed by TM, generally transition to STANDBY after observations
    UNKNOWN = 8

class CapabilityState(enum.IntEnum):
    UNAVAILABLE = 0         # Dish is unavailable due to functional error or components are not fitted, or during STARTUP 
    STANDBY = 1             # Dish is fully functional and ready to operate, but not currently marked as operational
    CONFIGURING = 2         # Dish is in the process of configuring to become ready for operation
    OPERATE_DEGRADED = 3    # Dish is operating but with degraded performance or partial functionality
    OPERATE_FULL = 4        # Dish is operating at full performance and functionality
    UNKNOWN = 5             # Dish capability state is unknown

class Feed(enum.IntEnum):
    NONE = 0
    H3T_1420 = 1    # 3 Turn Helical Feed 1420 MHz 
    H7T_1420 = 2    # 7 Turn Helical Feed 1420 MHz
    LF_400 = 3      # Loop Feed 400 MHz
    LOAD = 4        # Load for calibration

class DriverType(enum.IntEnum):
    MD01 = 1            # RF Hamdesigns MD-01
    MD02 = 2            # RF Hamdesigns MD-02
    MD03 = 3            # RF Hamdesigns MD-03
    LOSMANDY_G11 = 4    # Losmandy G-11
    ASCOM = 5           # ASCOM Standard Driver
    INDI = 6            # INDI Standard Driver  
    UNKNOWN = 7

class DishModel(BaseModel):
    """A class representing the dish model."""

    schema = Schema({      
        "_type": And(str, lambda v: v == "DishModel"),                                                                     
        "dsh_id": And(str, lambda v: isinstance(v, str)),                                         # Dish identifer e.g. "dish001" 
        "short_desc": Or(None, And(str, lambda v: isinstance(v, str))),                           # Short description of the dish
        "diameter": And(Or(int, float), lambda v: v >= 0.0),                                      # Dish diameter (meters)
        "fd_ratio": And(Or(int, float), lambda v: v >= 0.0),                                      # Dish focal length to diameter ratio
        "latitude": And(Or(int, float), lambda v: -90.0 <= v <= 90.0),                            # Dish latitude (degrees)
        "longitude": And(Or(int, float), lambda v: -180.0 <= v <= 180.0),                         # Dish longitude (degrees)
        "height": And(Or(int, float), lambda v: v >= 0.0),                                        # Dish height (meters) above sea level
        "feed": And(Feed, lambda v: isinstance(v, Feed)),                                         # Current feed installed on the dish
        "dig_id": Or(None, And(str, lambda v: isinstance(v, str))),                               # Current digitiser id assigned to the dish
        "mode": And(DishMode, lambda v: isinstance(v, DishMode)),
        "pointing_state": And(PointingState, lambda v: isinstance(v, PointingState)),
        "tgt_id": Or(None, And(str, lambda v: isinstance(v, str))),                               # Current target id assigned to the dish in the form {obs_id}_{obs.tgt_idx}
        "target": Or(None, lambda v: v is None or isinstance(v, BaseModel)),                      # Current target model assigned to the dish
        "desired_altaz": Or(None, dict, lambda v: v is None or isinstance(v, (dict, SkyCoord))),  # Desired alt-az position of dish
        "pointing_altaz": Or(None, dict, lambda v: v is None or isinstance(v, (dict, SkyCoord))), # Current alt-az pointing direction of dish
        "capability_state": And(CapabilityState, lambda v: isinstance(v, CapabilityState)),
        "driver_type": And(DriverType, lambda v: isinstance(v, DriverType)),                      # Dish driver type e.g. "ASCOM", "INDI", "MD-01", "MD-02"
        "driver_config": Or(None, lambda v: v is None or isinstance(v, BaseModel)),               # Dish driver configuration instance e.g. MD01Config
        "driver_poll_period": Or(None, And(int, lambda v: v > 0)),                                # Dish driver poll period in milliseconds to get altaz updates
        "driver_failures": And(int, lambda v: v >= 0),                                            # Count of consecutive driver call failures
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    # Allow transitions to UNKNOWN (something went wrong), and to itself (for re-affirming state)
    allowed_transitions = {
        "mode": { 
            DishMode.UNKNOWN:   {DishMode.UNKNOWN, DishMode.STARTUP, DishMode.SHUTDOWN, DishMode.STOW, DishMode.MAINTENANCE},
            DishMode.SHUTDOWN:  {DishMode.UNKNOWN, DishMode.SHUTDOWN, DishMode.STARTUP},
            DishMode.STARTUP:   {DishMode.UNKNOWN, DishMode.STARTUP, DishMode.STANDBY_LP, DishMode.STANDBY_FP, DishMode.STOW, DishMode.SHUTDOWN},
            DishMode.STANDBY_LP:{DishMode.UNKNOWN, DishMode.STANDBY_LP, DishMode.STANDBY_FP, DishMode.CONFIG, DishMode.STOW, DishMode.SHUTDOWN},
            DishMode.STANDBY_FP:{DishMode.UNKNOWN, DishMode.STANDBY_FP, DishMode.STANDBY_LP, DishMode.CONFIG, DishMode.STOW, DishMode.SHUTDOWN},
            DishMode.CONFIG:    {DishMode.UNKNOWN, DishMode.CONFIG, DishMode.OPERATE, DishMode.MAINTENANCE, DishMode.STOW, DishMode.SHUTDOWN},
            DishMode.STOW:      {DishMode.UNKNOWN, DishMode.STOW, DishMode.STANDBY_LP, DishMode.STANDBY_FP, DishMode.CONFIG, DishMode.MAINTENANCE, DishMode.SHUTDOWN},
            DishMode.MAINTENANCE: {DishMode.UNKNOWN, DishMode.MAINTENANCE, DishMode.STOW, DishMode.SHUTDOWN},
            DishMode.OPERATE:   {DishMode.UNKNOWN, DishMode.OPERATE, DishMode.STANDBY_FP, DishMode.STANDBY_LP, DishMode.CONFIG, DishMode.STOW, DishMode.SHUTDOWN},
        },
        "pointing_state": { 
            PointingState.UNKNOWN:  {PointingState.UNKNOWN, PointingState.READY},
            PointingState.READY:    {PointingState.UNKNOWN, PointingState.READY, PointingState.SLEW, PointingState.TRACK, PointingState.SCAN},
            PointingState.SLEW:     {PointingState.UNKNOWN, PointingState.SLEW, PointingState.READY},
            PointingState.TRACK:    {PointingState.UNKNOWN, PointingState.TRACK, PointingState.READY},
        },
        "capability_state": {
            CapabilityState.UNKNOWN:          {CapabilityState.UNKNOWN, CapabilityState.UNAVAILABLE, CapabilityState.STANDBY, CapabilityState.CONFIGURING, CapabilityState.OPERATE_DEGRADED, CapabilityState.OPERATE_FULL},
            CapabilityState.UNAVAILABLE:      {CapabilityState.UNKNOWN, CapabilityState.UNAVAILABLE, CapabilityState.STANDBY},
            CapabilityState.STANDBY:          {CapabilityState.UNKNOWN, CapabilityState.STANDBY, CapabilityState.CONFIGURING, CapabilityState.UNAVAILABLE},
            CapabilityState.CONFIGURING:      {CapabilityState.UNKNOWN, CapabilityState.CONFIGURING, CapabilityState.OPERATE_DEGRADED, CapabilityState.OPERATE_FULL, CapabilityState.STANDBY},
            CapabilityState.OPERATE_DEGRADED: {CapabilityState.UNKNOWN, CapabilityState.OPERATE_DEGRADED, CapabilityState.OPERATE_FULL, CapabilityState.STANDBY, CapabilityState.CONFIGURING},
            CapabilityState.OPERATE_FULL:     {CapabilityState.UNKNOWN, CapabilityState.OPERATE_FULL, CapabilityState.OPERATE_DEGRADED, CapabilityState.STANDBY, CapabilityState.CONFIGURING},
        },
    }

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "DishModel",
            "dsh_id": "<undefined>",
            "short_desc": None,
            "diameter": 0.0,
            "fd_ratio": 0.0,
            "latitude": 0.0,
            "longitude": 0.0,
            "height": 0.0,
            "feed": Feed.NONE,
            "dig_id": None,
            "mode": DishMode.UNKNOWN,
            "pointing_state": PointingState.UNKNOWN,
            "tgt_id": None,
            "target": None,
            "desired_altaz": None,
            "pointing_altaz": None,
            "capability_state": CapabilityState.UNKNOWN,
            "driver_type": DriverType.UNKNOWN,
            "driver_config": None,                          # Initialize with None, will be set based on driver_type
            "driver_poll_period": 1000,                     # Default to 1000 ms
            "driver_failures": 0,                           # Initialize failure count to zero     
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

    def increment_failures(self):
        """ Increment the driver failure count by one.
        """
        self.driver_failures += 1
        self.last_update = datetime.now(timezone.utc)

    def reset_failures(self):
        """ Reset the driver failure count to zero.
        """
        self.driver_failures = 0
        self.last_update = datetime.now(timezone.utc)

class DishList(BaseModel):
    """A class representing a list of dishes."""

    schema = Schema({
        "_type": And(str, lambda v: v == "DishList"),
        "list_id": And(str, lambda v: isinstance(v, str)),              # Dish List identifier e.g. "active"   
        "dish_list": And(list, lambda v: isinstance(v, list)),          # List of DishModel objects
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "DishList",
            "list_id": "<undefined>",
            "dish_list": [],
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class DishManagerModel(BaseModel):
    """A class representing the dish manager (application) model."""

    schema = Schema({    
        "_type": And(str, lambda v: v == "DishManagerModel"),     
        "id": And(str, lambda v: isinstance(v, str)),                                    # Dish Manager identifier e.g. "dm001"         
        "dish_store": And(DishList, lambda v: isinstance(v, DishList)),                  # List of DishModel objects                        
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "DishManagerModel",
            "id": "<undefined>",
            "dish_store": DishList(),
            "app": AppModel(
                app_name="dshmgr",
                app_running=False,
                num_processors=0,
                queue_size=0,
                interfaces=[],
                processors=[],
                health=HealthState.UNKNOWN,
                last_update=datetime.now(timezone.utc),
            ),
            "tm_connected": CommunicationStatus.NOT_ESTABLISHED,
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":

    dish001 = DishModel(
        dsh_id="dish001",
        short_desc="70cm Discovery dish",
        latitude=45.67, longitude=-111.05, height=1500.0,
        mode=DishMode.STARTUP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.NONE,
        dig_id="dig001",
        capability_state=CapabilityState.UNKNOWN,
        last_update=datetime.now(timezone.utc)
    )

    dish002 = DishModel(id="dish002")

    print("="*40)
    print("Dish001 Model Initialized")
    print(dish001.to_dict())
    print("="*40)

    # ✅ Valid transition
    dish001.mode = DishMode.STANDBY_LP
    print(f"After valid transition: {dish001.mode.name}")

    # ❌ Invalid transition (will raise ValueError)
    try:
        dish001.mode = DishMode.OPERATE
    except XInvalidTransition as e:
        print(f"Caught expected exception on invalid transition: {e}")

    # ❌ Schema violation (wrong type)
    try:
        dish001.feed = "H3T_1420"
    except XAPIValidationFailed as e:
        print("Schema check failed:", e)

    import pprint
    pprint.pprint(dish001.to_dict())

    print("="*40)
    print("Dish002 Model Initialized")
    pprint.pprint(dish002.to_dict())
    print("="*40)

    print("Dish Manager Model Test")
    dsh_mgr = DishManagerModel(
        id="dm001",
        dish_store=DishList(
            dish_list=[
                DishModel(
                    dsh_id="dish001",
                    short_desc="70cm Discovery dish",
                    latitude=45.67, longitude=-111.05, height=1500.0,
                    mode=DishMode.STARTUP,
                    pointing_state=PointingState.UNKNOWN,
                    feed=Feed.NONE,
                    capability_state=CapabilityState.UNKNOWN,
                    last_update=datetime.now(timezone.utc)
                )
            ],
            last_update=datetime.now(timezone.utc)
        ),
        app=AppModel(
            app_name="dsh",
            app_running=True,
            num_processors=2,
            queue_size=0,
            interfaces=["tm", "dsh"],
            processors=[],
            health=HealthState.UNKNOWN,
            last_update=datetime.now(timezone.utc)
        ),
        tm_connected=CommunicationStatus.ESTABLISHED,
        last_update=datetime.now(timezone.utc)
    )
    pprint.pprint(dsh_mgr.to_dict())

    print("="*40)
    print("Add another Dish to Dish Manager Model")
    print("="*40)
    new_dish = DishModel(
        dsh_id="dish002",
        short_desc="50cm Explorer dish",
        latitude=46.00, longitude=-112.00, height=1200.0,
        mode=DishMode.STARTUP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.NONE,
        dig_id="dig002",
        capability_state=CapabilityState.UNKNOWN,
        last_update=datetime.now(timezone.utc)
    )
    dsh_mgr.dish_store.dish_list.append(new_dish)
    pprint.pprint(dsh_mgr.to_dict())

    print("="*40)
    print("Dish Manager Model Default Test")
    print("="*40)
    dsh_mgr_default = DishManagerModel()
    pprint.pprint(dsh_mgr_default.to_dict())

    print("="*40)
    print("Save Dish List to disk as JSON")
    print("="*40)

    dsh_mgr.dish_store.save_to_disk(filename="dish_store_test.json")

    print("="*40)
    print("Load Dish List from disk as JSON")
    print("="*40)   
    dish_store = DishList().load_from_disk(filename="dish_store_test.json")
    pprint.pprint(dish_store.to_dict())

    print("="*40)
    print("Now prepare default DigitiserList configuration")
    print("="*40)  

    dish001 = DishModel(
        dsh_id="dish001",
        short_desc="70cm Discovery Dish",
        diameter=0.7,
        fd_ratio=0.37,
        latitude=53.187052, longitude=-2.256079, height=94.0,
        mode=DishMode.STANDBY_FP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.H3T_1420,
        dig_id="dig001",
        capability_state=CapabilityState.OPERATE_FULL,
        driver_type=DriverType.LOSMANDY_G11,
        driver_config=None,
        last_update=datetime.now(timezone.utc)
    )

    from dsh.drivers.md01.md01_model import MD01Config

    md01_cfg = MD01Config(
        host="192.168.0.2",
        port=65000,
        stow_alt=90.0,
        stow_az=0.0,
        offset_alt=0.0,
        offset_az=0.0,
        min_alt=0.0,
        max_alt=90.0,
        close_enough=0.1,
        last_update=datetime.now(timezone.utc)
    )

    dish002 = DishModel(
        dsh_id="dish002",
        short_desc="3m Jodrell Dish",
        diameter=3.0,
        fd_ratio=0.43,
        latitude=53.2421, longitude=-2.3067, height=80.0,
        mode=DishMode.STANDBY_FP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.NONE,
        dig_id="dig002",
        capability_state=CapabilityState.OPERATE_FULL,
        driver_type=DriverType.MD01,
        driver_config=md01_cfg,
        last_update=datetime.now(timezone.utc)
    )
    
    default_dshlist = DishList(
        list_id = "default",
        dish_list=[dish001, dish002],
    )

    default_dshlist.save_to_disk(output_dir="./config/default")

    



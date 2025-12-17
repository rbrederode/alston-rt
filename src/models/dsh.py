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

class PointingState(enum.IntEnum):
    READY = 0
    SLEW = 1
    TRACK = 2
    SCAN = 3
    UNKNOWN = 4

class DishMode(enum.IntEnum):
    STARTUP = 0
    SHUTDOWN = 1
    STANDBY_LP = 2
    STANDBY_FP = 3
    MAINTENANCE = 4
    STOW = 5
    CONFIG = 6
    OPERATE = 7
    UNKNOWN = 8

class CapabilityStates(enum.IntEnum):
    UNAVAILABLE = 0
    STANDBY = 1
    CONFIGURING = 2
    OPERATE_DEGRADED = 3
    OPERATE_FULL = 4
    UNKNOWN = 5

class Feed(enum.IntEnum):
    NONE = 0
    H3T_1420 = 1    # 3 Turn Helical Feed 1420 MHz 
    H7T_1420 = 2    # 7 Turn Helical Feed 1420 MHz
    LF_400 = 3      # Loop Feed 400 MHz
    LOAD = 4        # Load for calibration

class DishModel(BaseModel):
    """A class representing the dish model."""

    schema = Schema({      
        "_type": And(str, lambda v: v == "DishModel"),                                                                     
        "dsh_id": And(str, lambda v: isinstance(v, str)),                                       # Dish identifer e.g. "dish001" 
        "short_desc": Or(None, And(str, lambda v: isinstance(v, str))),                         # Short description of the dish
        "diameter": And(Or(int, float), lambda v: v >= 0.0),                                    # Dish diameter (meters)
        "fd_ratio": And(Or(int, float), lambda v: v >= 0.0),                                    # Dish focal length to diameter ratio
        "latitude": And(Or(int, float), lambda v: -90.0 <= v <= 90.0),                          # Dish latitude (degrees)
        "longitude": And(Or(int, float), lambda v: -180.0 <= v <= 180.0),                       # Dish longitude (degrees)
        "height": And(Or(int, float), lambda v: v >= 0.0),                                      # Dish height (meters) above sea level
        "feed": And(Feed, lambda v: isinstance(v, Feed)),                                       # Current feed installed on the dish
        "mode": And(DishMode, lambda v: isinstance(v, DishMode)),
        "pointing_state": And(PointingState, lambda v: isinstance(v, PointingState)),
        "altaz": Or(None, And(AltAz, lambda v: isinstance(v, AltAz))),                          # Current alt-az pointing direction
        "capability_state": And(CapabilityStates, lambda v: isinstance(v, CapabilityStates)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {
        "mode": {
            DishMode.UNKNOWN: {DishMode.STARTUP},
            DishMode.STARTUP: {DishMode.STANDBY_LP, DishMode.STANDBY_FP},
            DishMode.STANDBY_LP: {DishMode.CONFIG, DishMode.SHUTDOWN},
            DishMode.CONFIG: {DishMode.OPERATE, DishMode.MAINTENANCE},
            DishMode.OPERATE: {DishMode.STANDBY_FP, DishMode.STOW},
        },
        "pointing_state": {
            PointingState.UNKNOWN: {PointingState.READY},
            PointingState.READY: {PointingState.SLEW, PointingState.SCAN},
            PointingState.SLEW: {PointingState.TRACK, PointingState.READY},
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
            "mode": DishMode.UNKNOWN,
            "pointing_state": PointingState.UNKNOWN,
            "altaz": None,
            "capability_state": CapabilityStates.UNKNOWN,
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class DishList(BaseModel):
    """A class representing a list of dishes."""

    schema = Schema({
        "_type": And(str, lambda v: v == "DishList"),
        "dish_list": And(list, lambda v: isinstance(v, list)),          # List of DishModel objects
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        dish001 = DishModel(
            dsh_id="dish001",
            short_desc="70cm Discovery Dish",
            diameter=0.7,
            fd_ratio=0.37,
            latitude=53.187052, longitude=-2.256079, height=94.0,
            mode=DishMode.STARTUP,
            pointing_state=PointingState.UNKNOWN,
            feed=Feed.H3T_1420,
            capability_state=CapabilityStates.OPERATE_FULL,
            last_update=datetime.now(timezone.utc)
        )

        dish002 = DishModel(
            dsh_id="dish002",
            short_desc="3m Jodrell Dish",
            diameter=3.0,
            fd_ratio=0.43,
            latitude=53.2421, longitude=-2.3067, height=80.0,
            mode=DishMode.STARTUP,
            pointing_state=PointingState.UNKNOWN,
            feed=Feed.NONE,
            capability_state=CapabilityStates.OPERATE_FULL,
            last_update=datetime.now(timezone.utc)
        )

        # Default values
        defaults = {
            "_type": "DishList",
            "dish_list": [dish001, dish002],
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
        capability_state=CapabilityStates.UNKNOWN,
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
                    capability_state=CapabilityStates.UNKNOWN,
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
    new_dish = DishModel(
        dsh_id="dish002",
        short_desc="50cm Explorer dish",
        latitude=46.00, longitude=-112.00, height=1200.0,
        mode=DishMode.STARTUP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.NONE,
        capability_state=CapabilityStates.UNKNOWN,
        last_update=datetime.now(timezone.utc)
    )
    dsh_mgr.dish_store.dish_list.append(new_dish)
    pprint.pprint(dsh_mgr.to_dict())

    print("="*40)
    print("Dish Manager Model Default Test")
    dsh_mgr_default = DishManagerModel()
    pprint.pprint(dsh_mgr_default.to_dict())



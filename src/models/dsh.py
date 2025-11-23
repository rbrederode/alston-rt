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
        "dsh_id": And(str, lambda v: isinstance(v, str)),                                           # Dish identifer e.g. "dish001" 
        "short_desc": Or(None, And(str, lambda v: isinstance(v, str))),                         # Short description of the dish
        "location": Or(None, And(EarthLocation, lambda v: isinstance(v, EarthLocation))),                           # Physical location (lat, long, alt(m)) 
        "feed": And(Feed, lambda v: isinstance(v, Feed)),                                       # Current feed installed on the dish
        "mode": And(DishMode, lambda v: isinstance(v, DishMode)),
        "pointing_state": And(PointingState, lambda v: isinstance(v, PointingState)),
        "altaz": Or(None, And(AltAz, lambda v: isinstance(v, AltAzM))),                 # Current alt-az pointing direction
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
            "location": None,
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

class DishMgrModel(BaseModel):
    """A class representing the dish Local Monitoring and Control (application) model."""

    schema = Schema({    
        "_type": And(str, lambda v: v == "DishMgrModel"),                                                                 
        "dsh_id": And(str, lambda v: isinstance(v, str)),                  # Dish identifer e.g. "dish001"                          
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "DishMgrModel",
            "dsh_id": "<undefined>",
            "app": AppModel(
                app_name="dsh",
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
        location=EarthLocation(lat=45.67*u.deg, lon=-111.05*u.deg, height=1500*u.m),
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

    print("Dish Mgr Model Test")
    dm001 = DishMgrModel(
        dsh_id="dish001",
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
    pprint.pprint(dm001.to_dict())
    print("="*40)
    print("Dish Manager Model Default Test")
    dm002 = DishMgrModel(dsh_id="dish002")
    pprint.pprint(dm002.to_dict())



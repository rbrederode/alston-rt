# -*- coding: utf-8 -*-

import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

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
        "id": And(str, lambda v: isinstance(v, str)),
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "feed": And(Feed, lambda v: isinstance(v, Feed)),
        "mode": And(DishMode, lambda v: isinstance(v, DishMode)),
        "pointing_state": And(PointingState, lambda v: isinstance(v, PointingState)),
        "capability_state": And(CapabilityStates, lambda v: isinstance(v, CapabilityStates)),
        "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
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
            "id": "dish001",
            "app": AppModel(
                app_name="dish",
                app_running=False,
                num_processors=0,
                queue_size=0,
                interfaces=[],
                processors=[],
                health=HealthState.UNKNOWN,
                last_update=datetime.now(timezone.utc),
            ),
            "feed": Feed.NONE,
            "mode": DishMode.UNKNOWN,
            "pointing_state": PointingState.UNKNOWN,
            "capability_state": CapabilityStates.UNKNOWN,
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
        id="dish001",
        app=AppModel(
            app_name="dish",
            app_running=True,
            num_processors=2,
            queue_size=0,
            interfaces=["tm"],
            processors=[],
            health=HealthState.UNKNOWN,
            last_update=datetime.now(timezone.utc)
        ),
        mode=DishMode.STARTUP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.NONE,
        capability_state=CapabilityStates.UNKNOWN,
        tm_connected=CommunicationStatus.NOT_ESTABLISHED,
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




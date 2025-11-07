import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel
from models.health import HealthState

class AppModel(BaseModel):
    """A class representing an App(lication) model."""

    schema = Schema({
        "app_name": And(str, lambda v: isinstance(v, str)),
        "app_running": And(bool, lambda v: isinstance(v, bool)),
        "health": And(HealthState, lambda v: isinstance(v, HealthState)),
        "num_processors": And(int, lambda v: v >= 0),
        "queue_size": And(int, lambda v: v >= 0),
        "interfaces": And(list, lambda v: isinstance(v, list)),
        "processors": And(list, lambda v: isinstance(v, list)),
        "msg_timeout_ms": And(int, lambda v: v >= 0),
        "arguments": Or(None, And(dict, lambda v: isinstance(v, dict))),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

      # Default values
        defaults = {
            "app_name": "app",
            "app_running": False,
            "health": HealthState.UNKNOWN,
            "num_processors": 0,
            "queue_size": 0,
            "interfaces": [],
            "processors": [],
            "msg_timeout_ms": 10000,
            "arguments": None,
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":

    app001 = AppModel(
        app_name="app001",
        app_running=True,
        num_processors=2,
        queue_size=0,
        interfaces=["tm", "sdp"],
        processors=[],
        health=HealthState.UNKNOWN,
        msg_timeout_ms=10000,
        last_update=datetime.now(timezone.utc)
    )

    app002 = AppModel()

    import pprint
    print("="*40)
    print("App001")
    print("="*40)
    pprint.pprint(app001.to_dict())
    print("="*40)
    print("App002")
    print("="*40)
    pprint.pprint(app002.to_dict()) 
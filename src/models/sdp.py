# -*- coding: utf-8 -*-

import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.app import AppModel
from models.component import ComponentModel
from models.health import HealthState
from models.comms import CommunicationStatus
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

class ScienceDataProcessorModel(ComponentModel):
    """A class representing the science data processor model."""

    schema = Schema({
        "id": And(str, lambda v: isinstance(v, str)),
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "channels": And(int, lambda v: v >= 0),
        "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "dig_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "app": AppModel(
                app_name="sdp",
                app_running=False,
                num_processors=0,
                queue_size=0,
                interfaces=[],
                processors=[],
                health=HealthState.UNKNOWN,
                last_update=datetime.now(timezone.utc),
            ),
            "channels": 1024,
            "tm_connected": CommunicationStatus.NOT_ESTABLISHED,
            "dig_connected": CommunicationStatus.NOT_ESTABLISHED,
            "last_update": datetime.now(timezone.utc)
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)


if __name__ == "__main__":
    
    sdp001 = ScienceDataProcessorModel(
        id="sdp001",
        app=AppModel(
            app_name="sdp",
            app_running=True,
            num_processors=2,
            queue_size=0,
            interfaces=["tm", "dig"],
            processors=[],
            health=HealthState.UNKNOWN,
            last_update=datetime.now(timezone.utc)
        ),
        channels=0,
        tm_connected=CommunicationStatus.NOT_ESTABLISHED,
        dig_connected=CommunicationStatus.NOT_ESTABLISHED,
        last_update=datetime.now(timezone.utc)
    )

    sdp002 = ScienceDataProcessorModel(id="sdp002")

    import pprint
    print("="*40)
    print("sdp001 Model Initialized")
    print("="*40)
    pprint.pprint(sdp001.to_dict())

    print("="*40)
    print("sdp002 Model with Defaults Initialized")
    print("="*40)
    pprint.pprint(sdp002.to_dict())
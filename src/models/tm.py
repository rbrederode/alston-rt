# -*- coding: utf-8 -*-

import enum
from pathlib import Path
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.app import AppModel
from models.comms import CommunicationStatus
from models.base import BaseModel
from models.health import HealthState
from models.proc import ProcessorModel
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

class TelescopeManagerModel(BaseModel):
    """A class representing the telescope manager model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TelescopeManagerModel"),
        "id": And(str, lambda v: isinstance(v, str)),
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "sdp_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "dig_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "oet_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TelescopeManagerModel",
            "app": AppModel(
                app_name="tm",
                app_running=False,
                num_processors=0,
                queue_size=0,
                interfaces=[],
                processors=[],
                health=HealthState.UNKNOWN,
                last_update=datetime.now(timezone.utc),
            ),
            "sdp_connected": CommunicationStatus.NOT_ESTABLISHED,
            "dig_connected": CommunicationStatus.NOT_ESTABLISHED,
            "oet_connected": CommunicationStatus.NOT_ESTABLISHED,
            "last_update": datetime.now(timezone.utc)
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":
    
    tm001 = TelescopeManagerModel(
        id="tm001",
        app=AppModel(
            app_name="dig",
            app_running=True,
            num_processors=2,
            queue_size=0,
            interfaces=["tm", "sdp"],
            processors=[ProcessorModel(
                name="Thread-1",
                current_event="Idle",
                processing_time_ms=0.0,
                last_update=datetime.now()
            )],
            health=HealthState.UNKNOWN,
            last_update=datetime.now(timezone.utc)
        ),
        sdp_connected=CommunicationStatus.NOT_ESTABLISHED,
        dig_connected=CommunicationStatus.NOT_ESTABLISHED,
        oet_connected=CommunicationStatus.NOT_ESTABLISHED,
        last_update=datetime.now(timezone.utc)
    )

    tm002 = TelescopeManagerModel(id="tm002")

    tm001.app.app_name = "tm"

    import pprint
    print("="*40)
    print("tm001 Model Initialized")
    print("="*40)
    pprint.pprint(tm001.to_dict())

    print("="*40)
    print("tm002 Model with Defaults Initialized")
    print("="*40)
    pprint.pprint(tm002.to_dict())

# -*- coding: utf-8 -*-

import enum
import time
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel
from models.comms import CommunicationStatus
from models.dsh import Feed
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

class ScanState(enum.IntEnum):
    EMPTY = 0       # Scan has been created but no data loaded
    WIP = 1         # Scan is has some but not all data loaded
    ABORTED = 2     # Scan has been aborted (not fully loaded)
    COMPLETE = 3    # Scan has been fully loaded

class ScanModel(BaseModel):
    """A class representing the scan model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "ScanModel"),
        "scan_id": And(str, lambda v: isinstance(v, str)),
        "dig_id": And(str, lambda v: isinstance(v, str)),
        "created": And(datetime, lambda v: isinstance(v, datetime)),
        "read_start": Or(None, And(datetime, lambda v: isinstance(v, datetime))),
        "read_end": Or(None, And(datetime, lambda v: isinstance(v, datetime))),
        "gap": Or(None, And(float, lambda v: isinstance(v, float))),
        "start_idx": And(int, lambda v: v >= 0),
        "duration": And(Or(int, float), lambda v: v >= 0),
        "sample_rate": And(Or(int, float), lambda v: v >= 0.0),
        "channels": And(int, lambda v: v >= 0),
        "center_freq": And(Or(int, float), lambda v: v >= 0.0),
        "gain": And(Or(int, float), lambda v: 0 <= v <= 100.0),
        "feed": And(Feed, lambda v: isinstance(v, Feed)),
        "status": And(ScanState, lambda v: isinstance(v, ScanState)),
        "load_failures": And(int, lambda v: v >= 0),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "ScanModel",
            "scan_id": "<undefined>",
            "dig_id": "<undefined>",
            "created": datetime.now(timezone.utc),
            "read_start": None,
            "read_end": None,
            "gap": None,
            "start_idx": 0,
            "duration": 0,
            "sample_rate": 0.0,
            "channels": 0,
            "center_freq": 0.0,
            "gain": 0.0,
            "feed": Feed.NONE,
            "status": ScanState.EMPTY,
            "load_failures": 0,
            "loaded_secs": [],
            "last_update": datetime.now(timezone.utc)
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":
    
    scan001 = ScanModel(
        scan_id="scan001",
        dig_id="dig001",
        created=datetime.now(timezone.utc),
        read_start=datetime.now(timezone.utc),
        read_end=datetime.now(timezone.utc),
        start_idx=100,
        duration=60,
        sample_rate=1024.0,
        channels=1024,
        center_freq=1420405752.0,
        gain=50.0,
        feed=Feed.H3T_1420,
        status=ScanState.WIP,
        load_failures=0,
        last_update=datetime.now(timezone.utc)
    )

    scan002 = ScanModel(scan_id="scan002")

    import pprint
    print("="*40)
    print("scan001 Model Initialized")
    print("="*40)
    pprint.pprint(scan001.to_dict())

    print("="*40)
    print("scan002 Model with Defaults Initialized")
    print("="*40)
    pprint.pprint(scan002.to_dict())
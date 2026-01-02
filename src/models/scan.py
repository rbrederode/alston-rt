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
        "obs_id": Or(None, And(str, lambda v: isinstance(v, str))),
        "tgt_index": Or(None, And(int, lambda v: isinstance(v, int))),
        "freq_scan": Or(None, And(int, lambda v: isinstance(v, int))),
        "scan_iter": Or(None, And(int, lambda v: isinstance(v, int))),
        "dig_id": Or(None, And(str, lambda v: isinstance(v, str))),
        "created": And(datetime, lambda v: isinstance(v, datetime)),
        "read_start": Or(None, And(datetime, lambda v: isinstance(v, datetime))),
        "read_end": Or(None, And(datetime, lambda v: isinstance(v, datetime))),
        "gap": Or(None, And(float, lambda v: isinstance(v, float))),
        "start_idx": And(int, lambda v: v >= 0),
        "duration": And(Or(int, float), lambda v: v >= 0),
        "sample_rate": And(Or(int, float), lambda v: v >= 0.0),
        "channels": And(int, lambda v: v >= 0),
        "start_freq": Or(None, And(Or(int, float), lambda v: v >= 0.0)),
        "center_freq": And(Or(int, float), lambda v: v >= 0.0),
        "end_freq": Or(None, And(Or(int, float), lambda v: v >= 0.0)),
        "gain": And(Or(int, float), lambda v: 0 <= v <= 100.0),
        "load": And(bool, lambda v: isinstance(v, bool)),
        "status": And(ScanState, lambda v: isinstance(v, ScanState)),
        "load_failures": And(int, lambda v: v >= 0),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    # Default values
    _defaults = {
        "_type": "ScanModel",
        "obs_id": datetime.now(timezone.utc).isoformat(),
        "tgt_index": -1,
        "freq_scan": -1,
        "scan_iter": -1,
        "created": datetime.now(timezone.utc),
        "read_start": None,
        "read_end": None,
        "gap": None,
        "start_idx": 0,
        "duration": 0,
        "sample_rate": 0.0,
        "channels": 0,
        "start_freq": 0.0,
        "center_freq": 0.0,
        "end_freq": 0.0,
        "gain": 0.0,
        "load": False,
        "status": ScanState.EMPTY,
        "load_failures": 0,
        "loaded_secs": [],
        "last_update": datetime.now(timezone.utc)
    }

    def __init__(self, **kwargs):
        # Apply defaults if not provided in kwargs
        for key, value in self._defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

    @property
    def scan_id(self):
        return f"{self.obs_id}-{self.tgt_index}-{self.freq_scan}-{self.scan_iter}"

    def update_from_model(self, other_scan_model):
        """Update the current scan model with values from another scan model.
            Only update attributes that are different from the defaults.
        """
        if not isinstance(other_scan_model, ScanModel):
            raise XSoftwareFailure("Provided model is not a ScanModel instance")

        defaults = self.__class__._defaults
        updated = False

        for key in self.schema.schema.keys():
            if hasattr(other_scan_model, key):
                other_value = getattr(other_scan_model, key)
                # Only update if key not in defaults or value differs from default
                if key not in defaults or other_value != defaults[key]:
                    # Only update if value is different from current value
                    if getattr(self, key, None) != other_value:
                        setattr(self, key, other_value)
                        updated = True

        self.last_update = datetime.now(timezone.utc) if updated else self.last_update

    def __str__(self):
        return f"ScanModel(scan_id={self.scan_id}, dig_id={self.dig_id}, status={self.status.name}, " + \
               f"start_idx={self.start_idx}, duration={self.duration}, sample_rate={self.sample_rate}, " + \
               f"channels={self.channels}, center_freq={self.center_freq}, gain={self.gain}, load={self.load}, " + \
               f"created={self.created}, read_start={self.read_start}, read_end={self.read_end}, last_update={self.last_update})"   

if __name__ == "__main__":
    
    scan001 = ScanModel(
        dig_id="dig001",
        obs_id="obs001",
        tgt_index=0,
        freq_scan=1,
        scan_iter=5,
        created=datetime.now(timezone.utc),
        read_start=datetime.now(timezone.utc),
        read_end=datetime.now(timezone.utc),
        start_idx=100,
        duration=60,
        sample_rate=1024.0,
        channels=1024,
        center_freq=1420405752.0,
        gain=50.0,
        load=False,
        status=ScanState.WIP,
        load_failures=0,
        last_update=datetime.now(timezone.utc)
    )

    scan002 = ScanModel(
        obs_id="obs002",
        tgt_index=1,
        freq_scan=0,
        scan_iter=0
    )

    import pprint
    print("="*40)
    print("scan001 Model Initialized")
    print("="*40)
    pprint.pprint(scan001.to_dict())

    print("="*40)
    print("scan002 Model with Defaults Initialized")
    print("="*40)
    pprint.pprint(scan002.to_dict())

    print(scan002)

    scan001.update_from_model(scan002)
    print("="*40)
    print("scan001 after update from scan002")
    print("="*40)
    pprint.pprint(scan001.to_dict())
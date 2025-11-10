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

class ScanStoreModel(BaseModel):
    """A class representing the scan store model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "ScanStoreModel"),
        "spr_files": And(list, lambda v: isinstance(v, list)),
        "load_files": And(list, lambda v: isinstance(v, list)),
        "tsys_files": And(list, lambda v: isinstance(v, list)),
        "gain_files": And(list, lambda v: isinstance(v, list)),
        "meta_files": And(list, lambda v: isinstance(v, list)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "ScanStoreModel",
            "spr_files": [],
            "load_files": [],
            "tsys_files": [],
            "gain_files": [],
            "meta_files": [],
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class TelescopeManagerModel(BaseModel):
    """A class representing the telescope manager model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TelescopeManagerModel"),
        "id": And(str, lambda v: isinstance(v, str)),
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "scan_store": And(ScanStoreModel, lambda v: isinstance(v, ScanStoreModel)),
        "sdp_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "dig_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
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
            "scan_store": ScanStoreModel(
                spr_files=[],
                load_files=[],
                tsys_files=[],
                gain_files=[],
                meta_files=[],
            ),
            "sdp_connected": CommunicationStatus.NOT_ESTABLISHED,
            "dig_connected": CommunicationStatus.NOT_ESTABLISHED,
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

    scan_store_dir = "/Users/r.brederode/samples"

    # Read scan store directory listing 
    scan_files = list(Path(scan_store_dir).glob("*spr.csv"))
    # Sort by modification time, newest first
    scan_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    if scan_files:

        for scan_file in scan_files:
            mod_time = datetime.fromtimestamp(scan_file.stat().st_mtime, timezone.utc)
            tm001.scan_store.spr_files.append(scan_file.name)

    print("="*40)
    print("tm001 Model with Scan Store Files Updated")
    print("="*40)
    pprint.pprint(tm001.to_dict())

    tm002.scan_store = ScanStoreModel()
    tm002.scan_store.from_dict(tm001.scan_store.to_dict())

    print("="*40)
    print("tm002 Model with Scan Store from tm001")
    print("="*40)
    pprint.pprint(tm002.to_dict())
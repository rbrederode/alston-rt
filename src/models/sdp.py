# -*- coding: utf-8 -*-

import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.app import AppModel
from models.base import BaseModel
from models.comms import CommunicationStatus
from models.health import HealthState
from models.scan import ScanModel, ScanState
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

class ScienceDataProcessorModel(BaseModel):
    """A class representing the science data processor model."""

    schema = Schema({
        "id": And(str, lambda v: isinstance(v, str)),
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "channels": And(int, lambda v: v >= 0),
        "scan_duration": And(int, lambda v: v >= 0),
        "scans_created": And(int, lambda v: v >= 0),
        "scans_completed": And(int, lambda v: v >= 0),
        "scans_aborted": And(int, lambda v: v >= 0),
        "processing_scans": And(list, lambda v: all(isinstance(item, ScanModel) for item in v)),
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
            "scan_duration": 60,
            "scans_created": 0,
            "scans_completed": 0,
            "scans_aborted": 0,
            "processing_scans": [],
            "tm_connected": CommunicationStatus.NOT_ESTABLISHED,
            "dig_connected": CommunicationStatus.NOT_ESTABLISHED,
            "last_update": datetime.now(timezone.utc)
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

    def add_scan(self, scan: ScanModel):
        """Add a scan to the processing queue.
            A single scan is maintained in the processing scans list per digitiser id.
            If a scan with the same digitiser id already exists, it will be replaced.

        Args:
            scan (ScanModel): The scan model to add.
        """

        if not isinstance(scan, ScanModel):
            raise XAPIValidationFailed("scan must be an instance of ScanModel")

        # Replace existing scan with the same digitiser id
        for i, existing_scan in enumerate(self.processing_scans):
            if existing_scan.dig_id == scan.dig_id:
                self.processing_scans[i] = scan
                return

        self.processing_scans.append(scan)


if __name__ == "__main__":

    from models.dsh import Feed

    scan001 = ScanModel(
        scan_id="scan001",
        dig_id="dig001",
        duration=120,
        sample_rate=2000000,
        channels=2048,
        center_freq=1420000000,
        gain=30,
        feed=Feed.LF_400,
        status=ScanState.WIP,
    )

    scan002 = ScanModel(
        scan_id="scan002",
        dig_id="dig002",
        duration=120,
        sample_rate=2000000,
        channels=2048,
        center_freq=1420000000,
        gain=30,
        feed=Feed.LF_400,
        status=ScanState.WIP,
    )

    scan003 = ScanModel(
        scan_id="scan003",
        dig_id="dig001",
        duration=60,
        sample_rate=2000000,
        channels=2048,
        center_freq=1420000000,
        gain=30,
        feed=Feed.LF_400,
        status=ScanState.COMPLETE,
    )
    
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
        scan_duration=0,
        processing_scans=[scan001],
        tm_connected=CommunicationStatus.NOT_ESTABLISHED,
        dig_connected=CommunicationStatus.NOT_ESTABLISHED,
        last_update=datetime.now(timezone.utc)
    )

    sdp001.add_scan(scan002)
    sdp001.add_scan(scan003)

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
# -*- coding: utf-8 -*-

import ast
import enum
import json
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.app import AppModel
from models.base import BaseModel
from models.dsh import Feed
from models.health import HealthState
from models.comms import CommunicationStatus
from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

class DigitiserModel(BaseModel):
    """A class representing the digitiser model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "DigitiserModel"),
        "id": And(str, lambda v: isinstance(v, str)),
        "app": And(AppModel, lambda v: isinstance(v, AppModel)),
        "feed": And(Feed, lambda v: isinstance(v, Feed)),
        "gain": And(float, lambda v: 0 <= v <= 100.0),
        "sample_rate": And(float, lambda v: v >= 0.0),
        "bandwidth": And(float, lambda v: v >= 0.0),
        "center_freq": And(float, lambda v: v >= 0.0),
        "freq_correction": And(int, lambda v: -1000 <= v <= 1000),
        "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "sdp_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "sdr_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)),
        "streaming": And(bool, lambda v: isinstance(v, bool)),
        "sdr_eeprom": And(dict, lambda v: isinstance(v, dict)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "DigitiserModel",
            "id": "<undefined>",
            "app": AppModel(
                app_name="dig",
                app_running=False,
                num_processors=0,
                queue_size=0,
                interfaces=[],
                processors=[],
                health=HealthState.UNKNOWN,
                last_update=datetime.now(timezone.utc),
            ),
            "feed": Feed.NONE,
            "gain": 0.0,
            "sample_rate": 0.0,
            "bandwidth": 0.0,
            "center_freq": 0.0,
            "freq_correction": 0,
            "streaming": False,
            "tm_connected": CommunicationStatus.NOT_ESTABLISHED,
            "sdp_connected": CommunicationStatus.NOT_ESTABLISHED,
            "sdr_connected": CommunicationStatus.NOT_ESTABLISHED,
            "sdr_eeprom": {},
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":
    
    dig001 = DigitiserModel(
        id="dig001",
        app=AppModel(
            app_name="dig",
            app_running=True,
            num_processors=2,
            queue_size=0,
            interfaces=["tm", "sdp"],
            processors=[],
            health=HealthState.UNKNOWN,
            last_update=datetime.now()
        ),
        feed=Feed.LOAD,
        gain=0.0,
        sample_rate=2400000.0,
        bandwidth=200000.0,
        center_freq=1420000000.0,
        freq_correction=0,
        streaming=False,
        tm_connected=CommunicationStatus.NOT_ESTABLISHED,
        sdp_connected=CommunicationStatus.NOT_ESTABLISHED,
        sdr_connected=CommunicationStatus.NOT_ESTABLISHED,
        sdr_eeprom={},
        last_update=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    )

    dig002 = DigitiserModel(id="dig002")

    import pprint
    print("="*40)
    print("Digitiser 001")
    print("="*40)
    pprint.pprint(dig001.to_dict())
    print("="*40)
    print("Digitiser 002")
    print("="*40)
    pprint.pprint(dig002.to_dict())

    dig001.from_dict(dig002.to_dict())
    print("="*40)
    print("Digitiser 001 after from_dict with Digitiser 002 data")
    print("="*40)
    pprint.pprint(dig001.to_dict())

    dig_json = """
        {'_type': 'DigitiserModel',
            'app': {'_type': 'AppModel',
                    'app_name': 'dig',
                    'app_running': True,
                    'arguments': None,
                    'health': {'_type': 'enum.IntEnum',
                                'instance': 'HealthState',
                                'value': 'UNKNOWN'},
                    'interfaces': ['tm', 'sdp'],
                    'last_update': {'_type': 'datetime',
                                    'value': '2025-12-16T15:10:34.004551'},
                    'msg_timeout_ms': 10000,
                    'num_processors': 2,
                    'processors': [],
                    'queue_size': 0},
            'bandwidth': 200000.0,
            'center_freq': 1420000000.0,
            'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'LOAD'},
            'freq_correction': 0,
            'gain': 0.0,
            'id': 'dig001',
            'last_update': {'_type': 'datetime', 'value': '2025-11-01T12:00:00+00:00'},
            'sample_rate': 2400000.0,
            'sdp_connected': {'_type': 'enum.IntEnum',
                            'instance': 'CommunicationStatus',
                            'value': 'NOT_ESTABLISHED'},
            'sdr_connected': {'_type': 'enum.IntEnum',
                            'instance': 'CommunicationStatus',
                            'value': 'NOT_ESTABLISHED'},
            'sdr_eeprom': {},
            'streaming': False,
            'tm_connected': {'_type': 'enum.IntEnum',
                            'instance': 'CommunicationStatus',
                            'value': 'NOT_ESTABLISHED'}}
"""
    
    print("="*40)
    print("Digitiser 003 from JSON string")
    print("="*40)
    print(dig_json)
    print("="*40)

    dig003 = DigitiserModel(id="dig003")

    # Convert Python dict literal string to dictionary (strip indentation first)
    dig_json_dict = ast.literal_eval(dig_json.strip())

    dig003 = DigitiserModel.from_dict(dig_json_dict)
    
    print("="*40)
    print("Digitiser 003 after from_dict")
    print("="*40)
    pprint.pprint(dig003.to_dict())

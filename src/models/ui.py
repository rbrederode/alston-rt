import enum
import logging
from pathlib import Path
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel

logger = logging.getLogger(__name__)

class UIDriverType(enum.IntEnum):
    GSHEETS = 1         # Google Sheets
    REST_API = 2        # REST API
    CUSTOM_UI = 3       # Custom UI implementation
    UNKNOWN = 4

class UIDriver(BaseModel):
    """A class representing a UI driver for the telescope manager."""

    schema = Schema({
        "_type": And(str, lambda v: v == "UIDriver"),
        "type": And(UIDriverType, lambda v: isinstance(v, UIDriverType)),
        "short_desc": Or(None, And(str, lambda v: isinstance(v, str))),     # Optional short description of the UI driver
        "config": Or(None, lambda v: v is None or isinstance(v, dict)),     # Configuration dictionary for the UI driver
        "instance": Or(None, lambda v: v is None or isinstance(v, object)), # Optional instance of the UI driver (e.g. GoogleSheetsDriver object)
        "poll_period": Or(None, And(int, lambda v: v > 0)),                 # Poll period in seconds to push updates to the UI 
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "UIDriver",
            "type": UIDriverType.UNKNOWN,
            "short_desc": None,
            "config": None,
            "instance": None,   
            "poll_period": 30,
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

    def to_dict(self):
        """Override to exclude the 'instance' field which holds a non-serializable runtime object."""
        return {k: BaseModel._serialise(v) for k, v in self._data.items() if k != "instance"}

if __name__ == "__main__":

    import pprint

    gsheets_driver = UIDriver(
        type=UIDriverType.GSHEETS,
        config={
            "sheet_id": "1234567890",
            "tm_range": "TM_UI_API++!A2",
            "dig_range": "TM_UI_API++!D2",
            "dsh_range": "TM_UI_API++!G2",
            "sdp_range": "TM_UI_API++!J2",
            "odt_range": "TM_UI_API++!M2",
            "oda_range": "TM_UI_API++!P2",
        },  
        short_desc="Google Sheets UI Driver",
        poll_period=30,
        last_update=datetime.now(timezone.utc)
    )

    print("="*40)
    print("UIDriver object created:")
    print("="*40)
    pprint.pprint(gsheets_driver.to_dict())

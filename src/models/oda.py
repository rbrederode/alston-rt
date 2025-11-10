import enum
from pathlib import Path
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel

# Models comprising the Observation Data Archive (ODA)

class ScanStoreModel(BaseModel):
    """A class representing the scan store model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "ScanStoreModel"),
        "spr_files": And(list, lambda v: isinstance(v, list)),          # List of *spr.csv (summed power) filenames
        "load_files": And(list, lambda v: isinstance(v, list)),         # List of *load.csv (terminated signal chain) filenames    
        "tsys_files": And(list, lambda v: isinstance(v, list)),         # List of *tsys.csv (system temperature calibration) filenames
        "gain_files": And(list, lambda v: isinstance(v, list)),         # List of *gain.csv (gain calibration) filenames
        "meta_files": And(list, lambda v: isinstance(v, list)),         # List of *meta.csv (scan metadata) filenames
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



if __name__ == "__main__":
    
    import pprint

    ss001 = ScanStoreModel()
    ss_dir = "/Users/r.brederode/samples"

    # Read scan store directory listing 
    scan_files = list(Path(ss_dir).glob("*spr.csv"))
    # Sort by modification time, newest first
    scan_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    if scan_files:

        for scan_file in scan_files:
            mod_time = datetime.fromtimestamp(scan_file.stat().st_mtime, timezone.utc)
            ss001.spr_files.append(scan_file.name)

    print("="*40)
    print("Scan Store Model with Scan Store SPR Files")
    print("="*40)
    pprint.pprint(ss001.to_dict())

    print("="*40)
    print("Scan Store Model to_dict / from_dict Consistency Check")
    print("="*40)

    ss002 = ScanStoreModel()
    ss002.from_dict(ss001.to_dict())
    pprint.pprint(ss002.to_dict())
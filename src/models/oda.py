import enum
from pathlib import Path
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

from models.base import BaseModel
from models.obs import ObsModel, ObsState

# Models comprising the Observation Data Archive (ODA)

class ScanStore(BaseModel):
    """A class representing the scan store."""

    schema = Schema({
        "_type": And(str, lambda v: v == "ScanStore"),
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
            "_type": "ScanStore",
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

class ObsList(BaseModel):
    """A class representing a list of observations."""

    schema = Schema({
        "_type": And(str, lambda v: v == "ObsList"),
        "obs_list": And(list, lambda v: isinstance(v, list)),          # List of observations
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "ObsList",
            "obs_list": [],
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class ODAModel(BaseModel):
    """A class representing the observation data archive."""

    schema = Schema({
        "_type": And(str, lambda v: v == "ODAModel"),
        "scan_store": And(ScanStore, lambda v: isinstance(v, ScanStore)),
        "obs_list": And(ObsList, lambda v: isinstance(v, ObsList)),
        "last_update": And(datetime, lambda v: isinstance(v, datetime)),
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "ODAModel",
            "scan_store": ScanStore(),
            "obs_list": ObsList(),
            "last_update": datetime.now(timezone.utc),
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":
    
    import pprint

    ss001 = ScanStore()
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
    print("Scan Store with Scan Store SPR Files")
    print("="*40)
    pprint.pprint(ss001.to_dict())

    print("="*40)
    print("Scan Store to_dict / from_dict Consistency Check")
    print("="*40)

    ss002 = ScanStore()
    ss002.from_dict(ss001.to_dict())
    pprint.pprint(ss002.to_dict())

    obs001 = ObsModel(
        obs_id="obs001",
        short_desc="Test Observation",
        long_desc="This is a test observation of a celestial target.",
        state=ObsState.EMPTY,
        targets=[],
        target_durations=[],
        dsh_id="dish001",
        center_freq=1420405752.0,
        bandwidth=2048000.0,
        sample_rate=1024.0,
        channels=1024,
        scans=[],
        start_dt=datetime.now(timezone.utc),
        end_dt=datetime.now(timezone.utc),
        tsys_calibrators=[],
        gain_calibrators=[],
        load_calibrators=[],
        spr_scans=[],
        last_update=datetime.now(timezone.utc)
    )

    obslist = ObsList()
    obslist.obs_list.append(obs001)
    print("="*40)
    print("Observation List with One Observation")
    print("="*40)
    pprint.pprint(obslist.to_dict())


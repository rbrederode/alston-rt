import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

import astropy.units as u
from astropy.coordinates import get_body
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
from astropy.time import Time

from models.base import BaseModel
from models.dsh import Feed

#=======================================
# Models comprising a Target (TARGET)
#=======================================

class PointingType(enum.IntEnum):
    """Python enumerated type for pointing types."""

    SIDEREAL_TRACK = 0        # Sidereal tracking (fixed RA/Dec) e.g. Andromeda Galaxy
    NON_SIDEREAL_TRACK = 1    # Solar system or satellite tracking e.g. planet, moon or Sun
    DRIFT_SCAN = 2            # Fixed Alt-azimuth target e.g. Zenith
    FIVE_POINT_SCAN = 3       # Center point and 4 offset points e.g. for beam mapping

class TargetModel(BaseModel):
    """A class representing a target model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetModel"),
        "id": Or(None, And(str, lambda v: isinstance(v, str))),                     # Target identifier
        "pointing": And(PointingType, lambda v: isinstance(v, PointingType)),               # Target type
        "sky_coord": Or(None, lambda v: v is None or isinstance(v, SkyCoord)),      # Sky coordinates (any frame)
        "altaz": Or(None, dict, lambda v: v is None or isinstance(v, (dict, SkyCoord))), # Alt-az coordinates (SkyCoord or AltAz)
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetModel",
            "id": None,                             # Used for solar and lunar (and optionally sidereal) targets e.g. "Sun", "Moon", "Mars", "Vega"
            "pointing": PointingType.DRIFT_SCAN,    # Default to drift scan pointing
            "sky_coord": None,                      # Used for sidereal targets (ra,dec or l,b)
            "altaz": None,                          # Used for non-sidereal targets e.g. solar, terrestrial or satellite targets
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

class TargetConfig(BaseModel):
    """A class representing a target and associated configuration."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetConfig"),
        "target": And(TargetModel, lambda v: isinstance(v, TargetModel)),   # Target object
        "feed": And(Feed, lambda v: isinstance(v, Feed)),                   # Feed enum
        "gain": And(Or(int, float), lambda v: v >= 0.0),                    # Gain (dBi)
        "center_freq": And(Or(int, float), lambda v: v >= 0.0),             # Center frequency (Hz) 
        "bandwidth": And(Or(int, float), lambda v: v >= 0.0),               # Bandwidth (Hz) 
        "sample_rate": And(Or(int, float), lambda v: v >= 0.0),             # Sample rate (Hz) 
        "integration_time": And(Or(int, float), lambda v: v >= 0.0),        # Integration time (seconds)
        "spectral_resolution": And(int, lambda v: v >= 0),                  # Spectral resolution (fft size)
        "target_id": And(int, lambda v: v >= -1),                           # Target identifier
      })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetConfig",
            "target": TargetModel(),           # Target object
            "feed": Feed.NONE,                 # Default to None feed
            "gain": 0.0,                       # Gain (dBi)
            "center_freq": 0.0,                # Center frequency (Hz) 
            "bandwidth": 0.0,                  # Bandwidth (Hz) 
            "sample_rate": 0.0,                # Sample rate (Hz) 
            "integration_time": 0.0,           # Integration time (seconds)
            "spectral_resolution": 0,          # Spectral resolution (fft size)
            "target_id": -1,                   # Target identifier
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":

    import pprint

    coord = SkyCoord(ra="18h36m56.33635s", dec="+38d47m01.2802s", frame="icrs")
    altaz = {"alt": 45.0*u.deg, "az": 180.0*u.deg}

    target001 = TargetModel(
        id="Vega",
        pointing=PointingType.SIDEREAL_TRACK,
        sky_coord=coord,
        altaz=None
    )
    print('='*40)
    print("Target Model: Sidereal Target")
    print('='*40)
    pprint.pprint(target001.to_dict())

    target002 = TargetModel(
        id="Ground Station Alpha",
        pointing=PointingType.DRIFT_SCAN,
        sky_coord=None,
        altaz=altaz
    )
    print('='*40)
    print("Target Model: Terrestrial Target")
    print('='*40)   
    pprint.pprint(target002.to_dict())

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('='*40)   

    dt = datetime.now(timezone.utc)
    location = EarthLocation(lat=45.67*u.deg, lon=-111.05*u.deg, height=1500*u.m)

    moon_icrs = get_body('moon', Time(dt), location)
    altaz_frame = AltAz(obstime=Time(dt), location=location)
    altaz = moon_icrs.transform_to(altaz_frame)

    print("Computed AltAz for Moon at", dt.isoformat())

    target003 = TargetModel(
        id="Moon",
        pointing=PointingType.NON_SIDEREAL_TRACK,
        sky_coord=None,
        altaz={"alt": altaz.alt, "az": altaz.az}
        )

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('='*40)

    pprint.pprint(target003.to_dict())

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('Tests from_dict method')
    print('='*40)

    target004 = TargetModel()
    target004 = target004.from_dict(target003.to_dict())

    pprint.pprint(target004.to_dict())
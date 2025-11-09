import enum
from datetime import datetime, timezone
from schema import Schema, And, Or, Use, SchemaError

import astropy.units as u
from astropy.coordinates import get_body
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
from astropy.time import Time

from models.base import BaseModel

#=======================================
# Models comprising a Target (TARGET)
#=======================================

class TargetType(enum.IntEnum):
    """Python enumerated type for target types."""

    SIDEREAL = 0        # Sidereal target (fixed RA/Dec)
    SOLAR = 1           # Solar system target (e.g. planet or Sun)
    LUNAR = 2           # Lunar target (the Moon)
    TERRESTRIAL = 3     # Terrestrial target (e.g. ground station)
    SATELLITE = 4       # Satellite target (e.g. communications satellite)

class TargetModel(BaseModel):
    """A class representing a target model."""

    schema = Schema({
        "_type": And(str, lambda v: v == "TargetModel"),
        "name": Or(None, And(str, lambda v: isinstance(v, str))),                         # Target name
        "type": And(TargetType, lambda v: isinstance(v, TargetType)),                       # Target type
        "sky_coord": Or(None, lambda v: v is None or isinstance(v, SkyCoord)),  # Sky coordinates (any frame)
        "altaz": Or(None, lambda v: v is None or isinstance(v, (SkyCoord, AltAz))),      # Alt-az coordinates (SkyCoord or AltAz)
    })

    allowed_transitions = {}

    def __init__(self, **kwargs):

        # Default values
        defaults = {
            "_type": "TargetModel",
            "name": None,                   # Used for solar and lunar (and optionally sidereal) targets e.g. "Sun", "Moon", "Mars", "Vega"
            "type": TargetType.SIDEREAL,    # Default to sidereal target
            "sky_coord": None,              # Used for sidereal targets (ra,dec or l,b)
            "altaz": None,                  # Used for terrestrial and satellite targets
        }

        # Apply defaults if not provided in kwargs
        for key, value in defaults.items():
            if key not in kwargs:
                kwargs.setdefault(key, value)

        super().__init__(**kwargs)

if __name__ == "__main__":

    import pprint

    coord = SkyCoord(ra="18h36m56.33635s", dec="+38d47m01.2802s", frame="icrs")
    altaz = AltAz(alt=45.0*u.deg, az=180.0*u.deg)

    target001 = TargetModel(
        name="Vega",
        type=TargetType.SIDEREAL,
        sky_coord=coord,
        altaz=None
    )
    print('='*40)
    print("Target Model: Sidereal Target")
    print('='*40)
    pprint.pprint(target001.to_dict())

    target002 = TargetModel(
        name="Ground Station Alpha",
        type=TargetType.TERRESTRIAL,
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
        name="Moon",
        type=TargetType.SOLAR,
        sky_coord=None,
        altaz=altaz
        )

    print('='*40)

    pprint.pprint(target003.to_dict())

    print('='*40)
    print("Target Model: Solar Target (Moon)")
    print('Tests from_dict method')
    print('='*40)

    target004 = TargetModel()
    target004 = target004.from_dict(target003.to_dict())

    pprint.pprint(target004.to_dict())
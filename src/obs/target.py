
from datetime import datetime, timezone

from astropy import units as u
from astropy.coordinates import SkyCoord, AltAz, ICRS, EarthLocation, get_body
from astropy.time import Time

import logging
logger = logging.getLogger(__name__)

class Target:
    """ Class representing a target object in the sky. """
    def __init__(self, name: str, kind: str = "sidereal", coord: SkyCoord | None = None):
        self.name = name    # Name of the target e.g. Polaris Australis, Sun, Moon etc.
        self.kind = kind    # Kind of the target (e.g. sidereal, solar, RFI, calibration etc.)
        self.coord = coord  # SkyCoord object using a reference frame such as ICRS, FK5, Galactic, etc.

    def get_altaz(self, observing_time: datetime | Time, location: EarthLocation) -> AltAz:
        """ Convert the target's coordinates to AltAz frame for a given observing time and location. 
                Parameters:
                    observing_time: The time of observation either as a datetime or Time object.
                    location (EarthLocation): The location of the observer on Earth.
                Returns:
                    AltAz: The target's coordinates in the AltAz frame.
        
        """
        if self.coord is None:
            raise ValueError("Target coordinates are not set.")

        if location is None:
            raise ValueError("Target requires observer location to convert to AltAz.")

        # Convert to Time object if necessary
        if isinstance(observing_time, datetime):
            if observing_time.tzinfo is None:
                observing_time = observing_time.replace(tzinfo=timezone.utc)
            time = Time(observing_time, scale='utc')
        elif isinstance(observing_time, Time):
            time = observing_time

        altaz_frame = AltAz(obstime=time, location=location)
        return self.coord.transform_to(altaz_frame)

    def __repr__(self):
        return f"Target(name={self.name}, kind={self.kind}, coord={self.coord})"


if __name__ == "__main__":

    # Setup logging configuration
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
        handlers=[
            logging.StreamHandler(),                     # Log to console
            logging.FileHandler("target.log", mode="a")  # Log to a file
            ]
    )

    # Example ICRS (International Celestial Reference System) usage
    # ICRS is a stable inertial reference frame with its origin at the solar system barycenter, commonly used in astronomy
    target_coord = SkyCoord(ra=10.684*u.degree, dec=41.269*u.degree, frame='icrs')
    target = Target(name="Andromeda Galaxy", kind="sidereal", coord=target_coord)

    logger.info("-"*40)
    logger.info("Testing Sidereal Target:")
    logger.info("-"*40)
    logger.info(target)

    # produce a timezone-aware datetime using the standard library's timezone.utc
    observing_time = Time("2024-10-01 22:00:00", scale='utc')
    location = EarthLocation(lat=34.05*u.deg, lon=-118.25*u.deg, height=100*u.m)

    altaz = target.get_altaz(observing_time, location)

    logger.info(f"AltAz Coordinates: {altaz}")

    # Example usage with solar and lunar targets
    sun = get_body('sun', observing_time, location)
    moon = get_body('moon', observing_time, location)

    solar_target = Target(name="Sun", kind="solar", coord=sun)
    lunar_target = Target(name="Moon", kind="lunar", coord=moon)

    logger.info("-"*40)
    logger.info("Testing Solar/Lunar Target:")
    logger.info("-"*40)
    logger.info(solar_target)
    logger.info(lunar_target)

    solar_altaz = solar_target.get_altaz(observing_time, location)
    lunar_altaz = lunar_target.get_altaz(observing_time, location)

    logger.info(f"Solar AltAz Coordinates: {solar_altaz}")
    logger.info(f"Lunar AltAz Coordinates: {lunar_altaz}")

    # Example usage with terrestrial target (e.g., a tower or landmark)
    # observer / when
    obs_loc = EarthLocation(lat=34.05*u.deg, lon=-118.25*u.deg, height=100*u.m)
    t = Time("2025-10-19 22:00:00", scale='utc')

    # Alt/Az direction you want (az measured east of north: 0°=North, 90°=East)
    alt_val = 30.0 * u.deg
    az_val  = 120.0 * u.deg

    altaz_frame = AltAz(obstime=t, location=obs_loc)
    # Create SkyCoord in the AltAz frame
    terrestrial_target = Target(name="Tower", kind="terrestrial", coord=SkyCoord(alt=alt_val, az=az_val, frame=altaz_frame))
    
    logger.info("-"*40)
    logger.info("Testing Terrestrial Target:")
    logger.info("-"*40)
    logger.info(terrestrial_target)

    # Transform AltAz frame to ICRS (RA/Dec) for the same instant:
    direction_icrs = terrestrial_target.coord.transform_to("icrs")
    logger.info(f"RA/Dec at that time: {direction_icrs.ra.to_string()}, {direction_icrs.dec.to_string()}")
    logger.info("-"*40)
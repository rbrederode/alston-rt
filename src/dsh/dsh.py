from astropy.coordinates import EarthLocation, AltAz
from models.dsh import PointingState, CapabilityStates, DishMode, Feed
from models.health import HealthState

class Dish:

    def __init__(self, name: str, diameter: float, focal_length: float, location: EarthLocation):

        self.name = name
        self.diameter = diameter # Meters
        self.focal_length = focal_length # Meters
        self.focal_ratio = focal_length / diameter
        self.location = location

        self.configured_feed = Feed.NONE
        self.health_state = HealthState.UNKNOWN
        self.pointing_state = PointingState.UNKNOWN
        self.capability_state = CapabilityStates.UNKNOWN
        self.mode = DishMode.UNKNOWN

    def set_standby_mode(self):
        pass

    def set_operate_mode(self):
        pass

    def set_maintenance_mode(self):
        pass

    def set_full_power_mode(self):
        pass

    def track(self):
        pass

    def track_stop(self):
        pass

    def configure_feed(self, feed: int):
        pass

    def slew(self, altaz: AltAz):
        pass

    def scan(self, start_altaz: AltAz, end_altaz: AltAz, rate_deg_per_sec: float):
        pass

    def end_scan(self):
        pass

if __name__ == "__main__":
    # Example usage
    from astropy.coordinates import EarthLocation
    from astropy import units as u

    # Define dish location
    location = EarthLocation(lat=-30.7215*u.deg, lon=21.4439*u.deg, height=1073*u.m)

    # Create Dish instance
    dish = Dish(name="Alston RT", diameter=3.0, focal_length=1.5, location=location)

    print(f"Dish '{dish.name}' initialized at location {dish.location}.")
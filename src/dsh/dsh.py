from astropy.coordinates import EarthLocation, AltAz
from models.dsh import DishModel, PointingState, CapabilityStates, DishMode, Feed
from models.health import HealthState

class Dish:

    def __init__(self, dsh_model: DishModel=None):
        if dsh_model is None:
            raise ValueError("DishModel cannot be None")

        self.dsh_model = dsh_model

    def set_standby_mode(self):
        pass

    def set_operate_mode(self):
        pass

    def set_maintenance_mode(self):
        pass

    def set_full_power_mode(self):
        pass

    def track(self)->Tuple[str,str]:

        def _is_track_cmd_allowed(self) -> bool:
            
            if self.dsh_model.mode != DishMode.OPERATE:
                return False
            if self.dsh_model.pointing_state != PointingState.READY:
                return False
            return True
        # Check if track command is allowed
        if not _is_track_cmd_allowed(self):
            return ("ERROR", "Track command not allowed in current dish mode or pointing state.")
        # Implement track logic here
        return ("OK", "Track command accepted.")

    def track_stop(self):
        pass

    def configure_feed(self, feed: int):
        pass

    def slew(self, altaz: AltAz)->Tuple[str,str]:

        def _is_slew_cmd_allowed(self) -> bool:

            if self.dsh_model.mode != DishMode.OPERATE:
                return False
            if self.dsh_model.pointing_state != PointingState.READY:
                return False
            return True

        # Check if slew command is allowed
        if not _is_slew_cmd_allowed(self):
            return ("ERROR", "Slew command not allowed in current dish mode or pointing state.")
        # Implement slew logic here
        return ("OK", "Slew command accepted.")

    def scan(self, start_altaz: AltAz, end_altaz: AltAz, rate_deg_per_sec: float):
        pass

    def end_scan(self):
        pass

if __name__ == "__main__":
    # Example usage 

    dish001 = DishModel(
        dsh_id="dish001",
        short_desc="70cm Discovery Dish",
        diameter=0.7,
        fd_ratio=0.37,
        latitude=53.187052, longitude=-2.256079, height=94.0,
        mode=DishMode.STANDBY_FP,
        pointing_state=PointingState.UNKNOWN,
        feed=Feed.H3T_1420,
        dig_id="dig001",
        capability_state=CapabilityState.OPERATE_FULL,
        last_update=datetime.now(timezone.utc)
    )


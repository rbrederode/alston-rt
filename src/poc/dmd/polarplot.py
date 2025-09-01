import numpy as np
import tzlocal
import pytz
import matplotlib.pyplot as plt
import datetime
import time
from astropy.coordinates import EarthLocation, AltAz, SkyCoord
from astropy.coordinates import get_sun, get_body
from astropy.time import Time
import astropy.units as u
from astroplan import Observer, FixedTarget

# Granularity of the polar plot 
# Number of points within 360 deg to calculate altaz coordinates
GRANULARITY = 1000
# Indices into a target entry in the Targets list
FIXEDTARGET = 0
ALTAZ = 1
HMASK = 2

class PolarPlot:

    def __init__(self, location: EarthLocation, dt=None):
       
        # --- Set Observer Location ---
        self.location = location if not location is None else EarthLocation(lat=53.8*u.deg, lon=-2.8*u.deg, height=100*u.m)

        # Get the system's IANA timezone name (e.g., 'Europe/London')
        local_zone_name = tzlocal.get_localzone_name()
        self.observer = Observer(location=location, timezone=local_zone_name)

        # Record the datetime at which we calculate the altaz coordinates, needs refreshing every 24 hrs
        self.init_dt = dt if not dt is None else datetime.datetime.now().astimezone()
        
        # Calculate the timezone difference from utc to local timezone as a timedelta in hours
        self.offset = self.init_dt.utcoffset()
        self.hours_diff = self.offset.total_seconds() / 3600  

        # Generate GRANULARITY points over 24 hrs as a set of indexes over which to calculate altaz coordinates
        delta_hours = np.linspace(0, 24, GRANULARITY) 
        self.times = Time(self.init_dt) + delta_hours * u.hour # np array of date times are in UTC !

        self.start_index = None # observing start index within times array
        self.stop_index = None # observing end index within times array
        self.prev_index = None # index that was previously updated on the plot

        # Initialise variables to track the Sun and Moon
        self.sun_altaz = None 
        self.moon_altaz = None 

        # Initialise Day/Night mask
        self.is_day = np.ones(GRANULARITY, dtype=bool) # default to daytime
        self.is_night = ~self.is_day

        # Keep track of the targets added to the polar plot
        self.targets = [] # array of targets

        # Initialise label indices every few hrs
        label_interval = 2 # hrs
        label_hours = np.arange(0, 24, label_interval)
        self.label_indices = [np.argmin(np.abs((self.times - (Time(self.init_dt) + h*u.hour)).value)) for h in label_hours]

        # Initialise a target info box for the plot
        self.target_info_box = None

    def reset(self):

        # Record the datetime at which we calculate the altaz coordinates, needs refreshing every 24 hrs
        self.init_dt = Time(self.init_dt) + 24 * u.hour

        # Generate GRANULARITY points over 24 hrs as a set of indexes over which to calculate altaz coordinates
        delta_hours = np.linspace(0, 24, GRANULARITY) 
        self.times = Time(self.init_dt) + delta_hours * u.hour # np array of date times are in UTC !

        self.start_index = 0 if not self.start_index is None else None
        self.stop_index = 0 if not self.stop_index is None else None

        if self.sun_altaz:
            self.add_sun()
        if self.moon_altaz:
            self.add_moon()
        
        tmp_targets = []
        for target in self.targets:
            tmp_targets.append(target[FIXEDTARGET])
        
        self.targets = []
        for target in tmp_targets:
            self.add_target(target.coord, target.name)

        # Initialise label indices every few hrs
        label_interval = 2 # hrs
        label_hours = np.arange(0, 24, label_interval)
        self.label_indices = [np.argmin(np.abs((self.times - (Time(self.init_dt) + h*u.hour)).value)) for h in label_hours]
    
    def add_sun(self):

        # Calculate the azimuth and alitude coordinates for the Sun
        self.sun_altaz = self.observer.altaz(self.times, get_sun(self.times))

        # Calculate Day/Night mask based on the Sun being above the horizon
        self.is_day = self.sun_altaz.alt > 0*u.deg
        self.is_night = ~self.is_day

        try:
            self.sunrise_time = self.observer.sun_rise_time(Time(self.init_dt), which='next')
            self.sunset_time = self.observer.sun_set_time(Time(self.init_dt), which='next')
        except ValueError as e:
            print(f"Sun does not rise / set {e}")
            self.sunrise_time = self.sunset_time = None

    def add_moon(self):

        self.moon_altaz = np.empty(GRANULARITY, dtype=object)

        moon_icrs = get_body('moon', self.times, self.location)
        altaz_frame = AltAz(obstime=self.times, location=self.location)
        self.moon_altaz = moon_icrs.transform_to(altaz_frame)

        try:
            self.moonrise_time = self.observer.moon_rise_time(Time(self.init_dt), which='next')
            self.moonset_time = self.observer.moon_set_time(Time(self.init_dt), which='next')
        except ValueError as e:
            print(f"Moon does not rise / set {e}")
            self.moonrise_time = self.moonset_time = None

    def add_target(self, target_coord: SkyCoord, target_name: str=None):

        if target_name is None:
            target_name = f"Target{len(self.targets)}"

        # Calculate target AltAz coordinates for each time in the times array
        # Calculate target horizon mask where it is below the horizon
        tgt = FixedTarget(name=target_name, coord=target_coord)
        tgt_altaz = self.observer.altaz(self.times, tgt)
        tgt_horizon_mask = tgt_altaz.alt < 0*u.deg
        
        # Register the target in the targets list
        self.targets.append([tgt, tgt_altaz, tgt_horizon_mask])    

    def plot(self, figure=None, axes=None):
        
        # Create a figure and axes if none were provided as parameters
        self.fig = plt.figure(figsize=(9, 9)) if figure is None else figure
        self.ax = plt.subplot(111, polar=True) if axes is None else axes

        # Polar formatting
        self.ax.set_theta_zero_location("N")
        self.ax.set_theta_direction(-1)
        self.ax.set_rlim(0, 90)
        self.ax.set_yticks([30, 60, 90])
        self.ax.set_yticklabels(['60°', '30°', '0° Alt'])
        self.ax.set_title(f"Sky Trajectory on {self.init_dt.strftime('%Y/%m/%d')}\n" + \
            f"Observer: LAT {round(self.location.lat,2)}, LON {round(self.location.lon,2)}\n", va='bottom')

        # If the Moon was added to the plot
        if not self.moon_altaz is None: 

            # Check whether the Moon has a rise time, or that it is daytime (if it doesn't rise/set)
            if not self.moonrise_time is None:
        
                # Plot the Moon's trajectory
                az_moon = self.moon_altaz.az.radian
                r_moon = 90 - self.moon_altaz.alt.degree          
                self.ax.plot(az_moon, r_moon, color='darkviolet', lw=1.5, zorder=5, label='Moon')

                # Add local time labels to the Moon's Trajectory
                for i in self.label_indices:
                    if self.moon_altaz.alt[i] > 0*u.deg:
                        # Adjust hour_str for timezone difference between localtime and utc
                        hour_str = f"{int(self.times[i].datetime.hour + self.hours_diff):02d}:00"
                        self.ax.text(az_moon[i], r_moon[i], hour_str, fontsize=8, color='black', ha='center', va='center', zorder=10)

        # If the Sun was added to the plot
        if not self.sun_altaz is None: 

            # Check whether the Sun has a rise time, or that it is daytime (if it doesn't rise/set)
            if not self.sunrise_time is None or self.is_day:
        
                # Plot the Sun's trajectory
                az_sun = self.sun_altaz.az.radian
                r_sun = 90 - self.sun_altaz.alt.degree          
                self.ax.plot(az_sun, r_sun, color='orange', lw=1.5, zorder=5, label='Sun')

                # Add local time labels to the Sun's Trajectory
                for i in self.label_indices:
                    if self.sun_altaz.alt[i] > 0*u.deg:
                        # Adjust hour_str for timezone difference between localtime and utc
                        hour_str = f"{int(self.times[i].datetime.hour + self.hours_diff):02d}:00"
                        self.ax.text(az_sun[i], r_sun[i], hour_str, fontsize=8, color='black', ha='center', va='center', zorder=10)

        # For each target added to the plot
        for idx, tgt in enumerate(self.targets):

            # Plot the target's trajectory
            az_tgt = tgt[ALTAZ].az.radian
            r_tgt = 90 - tgt[ALTAZ].alt.degree

            # Identify parts of the target trajectory below the horizon and during day vs night
            is_below_horizon = tgt[HMASK]
            night_az = az_tgt[self.is_night & ~is_below_horizon]
            night_r = r_tgt[self.is_night & ~is_below_horizon]

            # Nighttime trajectory
            for seg_az, seg_r in split_on_altaz_jumps(night_az, night_r):
                self.ax.plot(seg_az, seg_r, color='blue', lw=2, label="Night" if 'Night' not in [l.get_label() for l in self.ax.lines] else "")
            # Daytime trajectory
            for seg_az, seg_r in split_on_altaz_jumps(az_tgt[self.is_day & ~is_below_horizon], r_tgt[self.is_day & ~is_below_horizon]):
                self.ax.plot(seg_az, seg_r, color='red', lw=2, label="Day" if 'Day' not in [l.get_label() for l in self.ax.lines] else "")

            annotated = None

            # Add local time and target name labels to the Target's Trajectory
            for i in self.label_indices:
                if tgt[ALTAZ].alt[i] > 0*u.deg:
                    # Adjust hour_str for timezone difference between localtime and utc
                    hour_str = f"{int(self.times[i].datetime.hour + self.hours_diff):02d}:00"    
                    self.ax.text(az_tgt[i], r_tgt[i], hour_str, fontsize=8, color='black', ha='center', va='center', zorder=10)
                    if not annotated:
                        annotated = self.ax.annotate(tgt[FIXEDTARGET].name, xy=(az_tgt[i], r_tgt[i]+2), xytext=(az_tgt[i]+idx*0.087, 100),
                            arrowprops={'arrowstyle':'->', 'connectionstyle':'arc3,rad=0.3'}, horizontalalignment='center', zorder=20)

        self.ax.legend(loc='lower right', bbox_to_anchor=(1.25, 0), fontsize='small')
        plt.tight_layout()
        plt.draw()
        plt.pause(0.001)

    def start_observing(self):

        # If observing not started yet, start it
        if self.start_index is None:

            dt = datetime.datetime.now(datetime.timezone.utc)

            # Keep track of the times index when we start observing
            self.start_index = np.argmin(np.abs(self.times - Time(dt))) 
            self.stop_index = None # Reset the stop index
            print(f"Started observing at index {self.start_index}...")

    def stop_observing(self):

        # If observing not stopped yet, stop it
        if self.stop_index is None: 

            dt = datetime.datetime.now(datetime.timezone.utc)

            # Don't record a stop if we had not started
            if self.start_index:
                # Stop observing by keeping track of the times index when we stop observing
                self.stop_index = np.argmin(np.abs(self.times - Time(dt))) 
                self.start_index = None # Reset the start index
                print(f"Stopped observing at index {self.stop_index}...")

    def get_target_altaz(self, target_name: str = None):

        dt = datetime.datetime.now(datetime.timezone.utc)

        # Check if the polar plot has expired (older than 24 hrs) or has not yet been plotted (no axes)
        if dt > Time(self.init_dt) + 24 * u.hour:
            # Reset the polar plot for the next 24 hrs
            self.reset()

        for tgt in self.targets:
            # Check if the target is the one we're interested in, or no target name was specified
            if tgt[FIXEDTARGET].name == target_name or target_name is None:
                tgt_altaz = self.observer.altaz(dt, tgt[FIXEDTARGET])
                return tgt_altaz.alt.degree, tgt_altaz.az.degree

        return None, None

    def update_targetbox(self):

        dt = datetime.datetime.now(datetime.timezone.utc)

        # Check if the polar plot has expired (older than 24 hrs) or has not yet been plotted (no axes)
        if dt > Time(self.init_dt) + 24 * u.hour or not self.ax:
            # Reset the polar plot for the next 24 hrs
            self.reset()
            self.plot()

        # Identify index corresponding to provided UTC time
        index = np.argmin(np.abs(self.times - Time(dt))) 

         # Build info string for all targets
        info_lines = []
        info_lines.append(f"Time (UTC) {dt.strftime('%Y/%m/%d %H:%M:%S')}\n")

        for tgt in self.targets:
            tgt_altaz = self.observer.altaz(dt, tgt[FIXEDTARGET])
            tgt_name = tgt[FIXEDTARGET].name
            info_lines.append(f"{tgt_name}:{' '*(10-len(tgt_name))}Alt {tgt_altaz.alt.degree:.3f}°, Az {tgt_altaz.az.degree:.3f}°")
        
        if self.sun_altaz:
            sun_altaz = self.observer.altaz(dt, get_sun(Time(dt)))
            info_lines.append(f"Sun:{' '*7}Alt {sun_altaz.alt.degree:.3f}°, Az {sun_altaz.az.degree:.3f}°")

        if self.moon_altaz:
            phase_angle = self.observer.moon_phase(Time(dt))
            phase_desc = get_moon_phase(phase_angle)
            moon_icrs = get_body('moon', Time(dt), self.location)
            altaz_frame = AltAz(obstime=Time(dt), location=self.location)
            moon_altaz = moon_icrs.transform_to(altaz_frame)
            info_lines.append(f"Moon:{' '*6}Alt {moon_altaz.alt.degree:.3f}°, Az {moon_altaz.az.degree:.3f}°")

        # Build info string for all targets
        info_text = "\n".join(info_lines)

        # Update the targetbox regardless of whether we are tracking an observation or not
        # Remove previous target box if it exists
        if self.target_info_box is not None:
            self.target_info_box.remove()

        # Add new text box in the bottom left 
        self.target_info_box = self.ax.text(
            -0.4, 0.1, info_text,
            transform=self.ax.transAxes,
            fontsize=8,
            fontname='DejaVu Sans Mono',
            va='top', ha='left',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.7),
            zorder=20
        )

        plt.draw()
        plt.pause(0.001)
        return info_text

    def update_tracks(self):
        """ Updates the polar plot with observation tracks during observations. 
            Assumes updates are atomic i.e. we transition between observation
            states or target/sun rise/setting from one update_track call to the
            next call, rather than within an update_track call. So we are either
            observing or not, rather than trying to deal with the transition.
        """ 

        dt = datetime.datetime.now(datetime.timezone.utc)

        # Check if the polar plot has expired (older than 24 hrs) or has not yet been plotted (no axes)
        if dt > Time(self.init_dt) + 24 * u.hour or not self.ax:
            # Reset the polar plot for the next 24 hrs
            self.reset()
            self.plot()

        # Initialise the begin and end indices of the tracks update
        update_end = np.argmin(np.abs(self.times - Time(dt))) 
        update_begin = self.prev_index if not self.prev_index is None else update_end

        # If we are not currently observing
        if self.start_index is None:
            # No tracks to update without an observation
            self.prev_index = update_end # advancing the prev index is all that's required
            return

        print(f"Update polar plot from {update_begin} to {update_end}")

        # Check day vs night time transitions
        was_day = self.is_day[update_begin]
        was_night = ~was_day
        is_night = self.is_night[update_end]
        is_day = ~is_night

        for tgt in self.targets:

            # Target's Trajectory
            az_tgt = tgt[ALTAZ].az.radian
            r_tgt = 90 - tgt[ALTAZ].alt.degree

             # Check if target transitioned below the horizon 
            was_below_horizon = tgt[HMASK][update_begin]
            is_below_horizon = tgt[HMASK][update_end]

            colour = 'whitesmoke' # default colour to indicate horizon transition

            # Check cases where there is no horizon transition
            if was_below_horizon and is_below_horizon:
                colour = 'lightgrey'    
            elif not was_below_horizon and not is_below_horizon:

                if was_day and is_day:
                    colour = 'salmon'      
                elif was_night and is_night:
                    colour = 'lightskyblue'
                elif (was_night and is_day) or (was_day and is_night):
                    colour = 'violet'

            # If indices have not yet advanced
            if update_begin == update_end:
                if self.prev_index is None:
                    # Draw a line to indicate that observations have started
                    self.ax.plot([az_tgt[update_end], az_tgt[update_end]], [0, r_tgt[update_end]], alpha=0.2, zorder=1, color=colour, lw=2, linestyle='-')
            else: # we need to fill the area between indices

                az_progress = az_tgt[update_begin:update_end]
                r_progress = r_tgt[update_begin:update_end]

                # Fill the progression
                try:
                    az_fill = np.concatenate(([az_progress[0]], az_progress))
                    r_fill = np.concatenate(([0], r_progress))
                    self.ax.fill(az_fill, r_fill, color=colour, alpha=0.2, zorder=1, label='Progression')
                except IndexError as e:
                    print(f"Index error for Target begin index {update_begin} and end index {update_end} updating polar plot target {e}")

        # If the Sun was added to the plot
        if self.sun_altaz: 

            # Check whether the Sun has a rise time, or that it is daytime
            if not self.sunrise_time is None or self.is_day:

                # Sun's trajectory
                az_sun = self.sun_altaz.az.radian
                r_sun = 90 - self.sun_altaz.alt.degree    

                 # Calculate the times array indexes corresponding to sunset and sunrise
                sunset_index = np.argmin(np.abs(self.times - self.sunset_time)) if not self.sunset_time is None else None
                sunrise_index = np.argmin(np.abs(self.times - self.sunrise_time)) if not self.sunrise_time is None else None        

                # If we have transitioned between day and night
                if was_day and is_night:
                    az_progress = az_sun[update_begin:sunset_index]
                    r_progress = r_sun[update_begin:sunset_index]

                elif was_day and is_day: # no transition from daytime took place
                    az_progress = az_sun[update_begin:update_end]
                    r_progress = r_sun[update_begin:update_end]

                elif was_night and is_day: # transition from night to day
                    az_progress = az_sun[sunrise_index:update_end]
                    r_progress = r_sun[sunrise_index:update_end]

                # Only plot if it was not night and still is night time
                if not (was_night and is_night):

                    # If we have not progressed across index boundaries
                    if len(az_progress) == 0:
                        if self.prev_index is None:
                            self.ax.plot([az_sun[update_end], az_sun[update_end]], [0, r_sun[update_end]], alpha=0.2, zorder=1, color='bisque', lw=2, linestyle='-')
                    else: # we need to fill the area between indices
                    
                        az_fill = np.concatenate(([az_progress[0]], az_progress))
                        r_fill = np.concatenate(([0], r_progress))
                        self.ax.fill(az_fill, r_fill, color='bisque', alpha=0.2, zorder=1, label='Progression')

        # If the Moon was added to the plot and will rise today
        if not self.moon_altaz is None and not self.moonrise_time is None: 

            # Check whether the Moon is above the horizon between begin and end updates indices
            if self.moon_altaz.alt[update_begin] > 0 * u.deg and self.moon_altaz.alt[update_end] > 0 * u.deg:

                 # Moon's trajectory
                az_moon = self.moon_altaz.az.radian
                r_moon = 90 - self.moon_altaz.alt.degree   

                # If indices have not yet advanced
                if update_begin == update_end:
                    if self.prev_index is None:
                        # Draw a line to indicate that observations have started
                        self.ax.plot([az_moon[update_end], az_moon[update_end]], [0, r_moon[update_end]], alpha=0.2, zorder=1, color="plum", lw=2, linestyle='-')
                else: # we need to fill the area between indices

                    az_progress = az_moon[update_begin:update_end]
                    r_progress = r_moon[update_begin:update_end]

                    # Fill the progression
                    try:
                        az_fill = np.concatenate(([az_progress[0]], az_progress))
                        r_fill = np.concatenate(([0], r_progress))
                        self.ax.fill(az_fill, r_fill, color="plum", alpha=0.2, zorder=1, label='Progression')
                    except IndexError as e:
                        print(f"Index error for Moon begin index {update_begin} and end index {update_end} updating polar plot target {e}")

       
        plt.draw()
        #plt.pause(0.001)
        self.prev_index = update_end

def split_on_altaz_jumps(az, r, alt_limit=5, az_limit=np.pi/8):
    segments = []
    start = 0
    for i in range(1, len(r)):
        dphi = r[i] - r[i - 1]  
        if np.abs(dphi) > alt_limit or np.abs(az[i] - az[i-1]) > az_limit:
            segments.append((az[start:i], r[start:i]))
            start = i
    segments.append((az[start:], r[start:])) 
    return segments

def get_moon_phase(phase_angle_rad):
    """ Provides a textual description of the moon phase based on the phase angle in radians """

    phase_angle_deg = np.degrees(phase_angle_rad).value
    if 0 <= phase_angle_deg < 22.5:
        return "New Moon"
    elif 22.5 <= phase_angle_deg < 67.5:
        return "Waxing Crescent"
    elif 67.5 <= phase_angle_deg < 112.5:
        return "First Quarter"
    elif 112.5 <= phase_angle_deg < 157.5:
        return "Waxing Gibbous"
    elif 157.5 <= phase_angle_deg < 202.5:
        return "Full Moon"
    elif 202.5 <= phase_angle_deg < 247.5:
        return "Waning Gibbous"
    elif 247.5 <= phase_angle_deg < 292.5:
        return "Last Quarter"
    elif 292.5 <= phase_angle_deg < 337.5:
        return "Waning Crescent"
    else:
        return "New Moon"

def main():

    delta_hours = np.linspace(0, 24, GRANULARITY) 
    times = Time(datetime.datetime.now(datetime.timezone.utc)) + (24+delta_hours) * u.hour # np array of date times are in UTC !

    polar = PolarPlot(EarthLocation(lat=53*u.deg, lon=-2.8*u.deg, height=100*u.m), datetime.datetime.now().astimezone() )
    polar.add_sun()
    polar.add_moon()
    #polar.add_target(SkyCoord('00h42m42s +41d16m00s', frame='icrs'), 'Target1')
    polar.add_target(SkyCoord('17h43m15s -28d52m00s', frame='icrs'), 'Sag. A*')
    polar.add_target(SkyCoord('06h40m58s +09d53m43s', frame='icrs'), 'NGC2264')
    polar.plot()
    #polar.reset()
    polar.start_observing()
    #print(f"Observing started")
    for i in range(10000):
        polar.update_tracks()
        polar.update_targetbox()
        print(f"{i} ")
        time.sleep(1)

    polar.update_tracks(Time(times[i]) + 1*u.hour)
    polar.stop_observing(dt=times[i])
    print(f"Observing stopped")
    for i in range(10):
        polar.update_tracks(dt=times[i])
        polar.update_targetbox(dt=times[i])
        print(f"{i} ")

    polar.start_observing(dt=times[i])
    print(f"Observing started")
    for i in range(10,GRANULARITY):
        polar.update_tracks(dt=times[i])
        polar.update_targetbox(dt=times[i])
        #time.sleep(0.01)
        print(f"{i} ")

    confirm = input("Close the application (press ENTER)").strip().lower()

    plt.close('all')

if __name__ == "__main__":
    main()


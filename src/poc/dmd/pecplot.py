import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import time
import tzlocal
import threading
import numpy as np

from astroplan import Observer, FixedTarget
from astropy.coordinates import EarthLocation, AltAz, SkyCoord, get_sun
from astropy.time import Time
import astropy.units as u

from motion import Motion

MAX_HISTORY = 1000  # Store last X PEC readings
FIG_SIZE = (14, 4)  # Default figure size for plots

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Or DEBUG for more verbosity
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class PECPlot:
    """
    A class to plot and analyze periodic error corrections (PEC) in astronomical observations.

    A target (RA, DEC) must be identified that translates to an alt-az position.
    A motion sensor must be configured to track the telescope's orientation in alt-az.

    PEC is calculated based on the difference between the azimuth and altitude readings from the motion
    sensor and the expected values derived from the target's coordinates and the observer's location.

    """

    def __init__(self, location: EarthLocation, target_coord: SkyCoord, motion: Motion=None, update_rate: float=1.0):
        # --- Set Observer Location ---
        self.location = location if not location is None else EarthLocation(lat=53.8*u.deg, lon=-2.8*u.deg, height=100*u.m)

        # Get the system's IANA timezone name (e.g., 'Europe/London')
        local_zone_name = tzlocal.get_localzone_name()
        self.observer = Observer(location=location, timezone=local_zone_name)

        # Set Target
        self.target = FixedTarget(name="Target", coord=target_coord) if not target_coord is None else None

        # Set Motion Sensor
        self.motion = motion if not motion is None else Motion()
        self.update_rate = update_rate if not update_rate is None else 1.0

        # Initialize Recording Thread
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()  # Lock for thread-safe access to shared resources (pec_history numpy array)

        # Store last MAX_HISTORY PEC readings (timestamp, alt_pec, az_pec)
        self.pec_hist = np.zeros((MAX_HISTORY, 3))  
        self.fig = None

        logging.info(f"Initializing PECPlot with location: {self.location}, target: {self.target}, motion: {self.motion}, update_rate: {self.update_rate}")

    def set_target(self, target_coord: SkyCoord):
        self.target = FixedTarget(name="Target", coord=target_coord) if not target_coord is None else None

    def set_location(self, location: EarthLocation):
        self.location = location if not location is None else EarthLocation(lat=53.8*u.deg, lon=-2.8*u.deg, height=100*u.m)

    def set_motion(self, motion: Motion):
        self.motion = motion if not motion is None else Motion()

    def start_recording(self):
        # Start recording periodic error 
        if self._thread is None or not self._thread.is_alive():
            self._stop_event.clear()
            if self.motion and self.motion.connect():
                self._thread = threading.Thread(target=self._record_loop)
                self._thread.start()
                logging.info("Started recording PEC.")
            else:
                logging.warning("Failed to start recording PEC")
                return False

        return True

    def stop_recording(self):
        # Stop recording periodic error
        if self._thread is not None and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join()
            self._thread = None

            if self.motion and self.motion.get_connected():
                self.motion.disconnect()

            logging.info("Stopped recording PEC.")

        return True

    def _record_loop(self):
        while not self._stop_event.is_set():
            self.calculate_periodic_error()
            time.sleep(self.update_rate)

    def calculate_periodic_error(self):

        # If target is not specified
        if self.target is None:
            logging.warning("PEC calculation failed: Target not specified.")
            return

        if self.motion is None:
            self.motion = Motion()
            self.motion.connect()

        if not self.motion.get_connected():
            logging.warning("PEC calculation failed: Motion sensor connection failed.")
            return

        now = Time(datetime.datetime.now().astimezone()) # Current datetime in UTC
        tgt_altaz = self.observer.altaz(now, self.target)

        # Get current telescope pointing from the motion sensor
        tel_altaz = self.motion.get_altaz()

        if tel_altaz is None or tel_altaz[0] is None or tel_altaz[1] is None:
            logging.warning("Telescope altaz not available.")
            return

        # Calculate the difference between the target and telescope positions
        alt_pec = tgt_altaz.alt.degree - tel_altaz[0]
        az_pec = tgt_altaz.az.degree - tel_altaz[1]

        logging.info(f"PEC for Target alt:{tgt_altaz.alt.degree:.4f} az:{tgt_altaz.az.degree:.4f}, Telescope:alt:{tel_altaz[0]:.4f} az:{tel_altaz[1]:.4f}, PEC alt_pec={alt_pec:.4f}, az_pec={az_pec:.4f}")

        now.format = 'unix'

        # Update PEC history by obtaining the current thread lock first
        # Numpy arrays (pec_hist) are not inherently thread-safe
        with self._lock:
            self.pec_hist = np.roll(self.pec_hist, shift=-1, axis=0)
            self.pec_hist[-1] = (now.value, alt_pec, az_pec)

    def plot(self, figure=None, axes=None):

        # Obtain the current thread lock before accessing shared resources (pec_hist)
        with self._lock:
            pec_hist_copy = self.pec_hist.copy()

        try:
            # Find first valid index where timestamp is not zero
            first_idx = np.where(pec_hist_copy[:,0]!=0)[0][0]
        except IndexError:
            logging.warning("Valid PEC data not available for plotting.")
            return

        # Create a figure and axes if none were provided as parameters
        self.fig = plt.figure(num=600, figsize=FIG_SIZE) if figure is None else figure
        self.ax = plt.subplot(111, polar=False) if axes is None else axes

        # Clear previous plot
        self.ax.cla()

        dates = [datetime.datetime.fromtimestamp(ts) for ts in pec_hist_copy[:, 0] if ts > 0]
        alt_pec = pec_hist_copy[pec_hist_copy[:, 0] > 0, 1]
        az_pec = pec_hist_copy[pec_hist_copy[:, 0] > 0, 2]

        alt_pec_rms = np.sqrt(np.mean(np.square(alt_pec - np.mean(alt_pec)))) if alt_pec.size > 0 else 0
        az_pec_rms = np.sqrt(np.mean(np.square(az_pec - np.mean(az_pec)))) if az_pec.size > 0 else 0

        # Plot the PEC history
        if pec_hist_copy is not None:
            self.ax.fill_between(dates, alt_pec, alpha=0.2, color='tab:blue', label=f'Alt PEC {alt_pec[-1]:.3f}° (RMS: {alt_pec_rms:.3f}°)')
            self.ax.fill_between(dates, az_pec, alpha=0.2, color='tab:red', label=f'Az PEC {az_pec[-1]:.3f}° (RMS: {az_pec_rms:.3f}°)')

        self.ax.set_title("Periodic Error Correction (PEC)")
        self.ax.set_ylabel("PEC [Deg]")
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        self.ax.set_xlabel("Time [HH:MM:SS]")
        self.ax.grid(True)
        self.ax.legend(loc='upper right')

        plt.draw()
        plt.pause(0.001)

    def __del__(self):
        self.stop_recording()
        if self.fig is not None:
            plt.close(self.fig)

        logging.info("PECPlot resources released.")

def main():
    logging.basicConfig(level=logging.INFO)

    location = EarthLocation(lat=53.8*u.deg, lon=-2.8*u.deg, height=100*u.m)
            
    # Get the Sun's position at the current date and time
    current_time = Time.now()
    target_coord = get_sun(current_time)  # Sun's position at current time
    
    # Alternative: Use a fixed target like Andromeda Galaxy
    # target_coord = SkyCoord(ra=10.684*u.deg, dec=41.269*u.deg, frame='icrs')  # Example: Andromeda Galaxy
    motion = Motion(device="auto", baudrate=9600)

    pec_plot = PECPlot(location=location, target_coord=target_coord, motion=motion, update_rate=1.0)

    pec_plot.start_recording()
    
    try:
        while True:
            pec_plot.plot()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt...")
    finally:
        pec_plot.stop_recording()

if __name__ == "__main__":
    main()
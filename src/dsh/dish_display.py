from __future__ import annotations
from typing import TYPE_CHECKING

import datetime
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib as mpl
# Disable automatic window raising (backend-specific)
mpl.rcParams['figure.raise_window'] = False

try:
    from AppKit import NSApplication
    HAS_APPKIT = True
except ImportError:
    HAS_APPKIT = False
    print("AppKit not available. Install pyobjc: pip install pyobjc")

from matplotlib.gridspec import GridSpec

from models.dsh import DishModel

import logging
logger = logging.getLogger(__name__)

FIG_SIZE = (14, 7)  # Default figure size for plots

class DishDisplay:

    # Attribute layout: (x, y, label) — x determines left (0) vs right (5) column
    ATTR_LABELS = [
        
        (0, 0.5, "Target Type"),
        (0, 1.5, "Target"),         
        (0, 2.5, "Lat/Long"),       (5, 2.5, "Height"),
        (0, 3.5, "Feed"),           (5, 3.5, "Digitiser"),
        (0, 4.5, "Pointing State"), (5, 4.5, "Driver Type"),
        (0, 5.5, "Mode"),           (5, 5.5, "Capability"),
        (0, 6.5, "Velocity Az"),    (5, 6.5, "Velocity Alt"),
        (0, 7.5, "Deviation Az"),   (5, 7.5, "Deviation Alt"),
        (0, 8.5, "Pointing Az"),    (5, 8.5, "Pointing Alt"),
        (0, 9.5, "Desired Az"),     (5, 9.5, "Desired Alt"),
    ]

    RECT_LEFT_X, RECT_RIGHT_X = 2.5, 7.5
    RECT_W, RECT_H = 2.4, 0.5
    LABEL_GAP = 0.1  # gap between label text and rectangle edge

    def __init__(self, driver: DishDriver):
        """ Initialize the dish display for a given dish driver
            :param driver: The dish driver to display plots
        """

        #mpl.use('TkAgg')        # Use TkAgg backend for interactive display

        self.is_active = True   # Is this dish display instance active
        self.driver = driver    # Current dish driver being displayed

        logger.info(f"Dish display initialized for dish {self.driver.dsh_model.dsh_id} with feed {self.driver.dsh_model.feed.value} and driver {self.driver.dsh_model.driver_type.name}")

        self.fig = None         # Figure for the dish display
        self.axes = [None] * 5  # Axes for the dish subplots
        self.attr_rects = {}    # Attribute rectangles keyed by label name
        self.attr_texts = {}    # Attribute value texts keyed by label name

        self.gs0 = GridSpec(1, 3, width_ratios=[1, 1, 1], left=0.07, right=0.93, top=0.90, bottom=0.3, wspace=0.2) # dish displays
        self.gs1 = GridSpec(1, 2, width_ratios=[0.32, 0.68], height_ratios=[1], left=0.07, right=0.93, top=0.20, bottom=0.07, wspace=0.2) # pec plot

        self._create_figure()   # Create the figure and axes for the dish display

    def __str__(self):
        return f"DishDisplay(dsh_id={self.driver.dsh_model.dsh_id}, is_active={self.is_active})"

    def _create_figure(self):
        """ Create the figure and axes of the dish displays """

        self.fig = plt.figure(num=f"Dish {self.driver.dsh_model.dsh_id}", figsize=FIG_SIZE)

        self.axes = [None] * 5  # Initialize axes for the subplots
        self.axes[0] = self.fig.add_subplot(self.gs0[0]) # Elevation and azimuth timeline
        self.axes[1] = self.fig.add_subplot(self.gs0[1]) # TBD
        self.axes[2] = self.fig.add_subplot(self.gs0[2]) # TBD

        self.axes[3] = self.fig.add_subplot(self.gs1[0]) # TBD
        self.axes[4] = self.fig.add_subplot(self.gs1[1]) # PEC Plot

        self.fig.subplots_adjust(top=0.78)
        
        self.fig.suptitle(
            f"Dish Id: {self.driver.dsh_model.dsh_id} Diameter: {self.driver.dsh_model.diameter}m FD Ratio: {self.driver.dsh_model.fd_ratio:.2f}",
            fontsize=12, y=0.96)

        self.init_attribute_axes(self.axes[0]) # Initialise axes for attributes such as pointing state and dish mode
        self.init_pec_axes(self.axes[4]) # Initialise the PEC axes

    def _close_figure(self):
        """ Close the figure of the dish displays """
        if self.fig is not None:
            plt.close(num=f"Dish {self.driver.dsh_model.dsh_id}")
            #self.fig = None
            #self.axes = [None] * 5
            #self.pwr_im = None
            #self.extent = None

    def _clear_figure(self):
        """ Clear the figure of the dish displays for reuse 
        """
        if self.fig is not None:
            # Clear existing axes for reuse
            for ax in self.axes:
                if ax is not None:
                    ax.cla()

    def is_visible_figure(self) -> bool:
        """ Return whether this dish display figure is visible or hidden. 
            A figure can be active but not visible if another figure window is in focus.
            The dish display is considered visible if its figure window is the key window in the OS
        """
        if not HAS_APPKIT or self.fig is None:
            logger.warning(
                f"Dish display checking whether figure for {self.driver.dsh_model.dsh_id} is visible but "
                + ("AppKit not available" if not HAS_APPKIT else "figure is None"))
            return None

        key_window = NSApplication.sharedApplication().keyWindow()
        if key_window is None:
             return None

        key_window_title = key_window.title()                   # Get the title of the key window
        fig_title = self.fig.canvas.manager.get_window_title()  # Get the title of the dish display figure
        return (fig_title == key_window_title)

    def get_is_active(self) -> bool:
        """ Return whether this dish display instance is active """
        return self.is_active

    def set_is_active(self, active: bool):
        """ Set whether this dish display instance is active """
        self.is_active = active
   
    def display(self):
        """
        Update the dish display for the current period.
        """

        # If the dish display is not active
        if not self.is_active:
            self._close_figure() # Close the figure if it exists
            return

        # If no figure, then log warning and return
        if self.fig is None:
            logger.warning(f"Dish display for {self.driver.dsh_model.dsh_id} cannot display when figure is None")
            return

        # Check if the dish display figure is still visible
        is_visible_fig = self.is_visible_figure()
        if is_visible_fig == False:
            return

        logger.debug(f"Dish display updating for dish {self.driver.dsh_model.dsh_id}")

        if self.driver.dsh_model is not None:
            m = self.driver.dsh_model
            self.attr_texts["Height"].set_text(f"{m.height:.1f}m")
            self.attr_texts["Lat/Long"].set_text(f"{m.latitude:.1f}°,{m.longitude:.1f}°")
            self.attr_texts["Driver Type"].set_text(m.driver_type.name)
            self.attr_texts["Feed"].set_text(m.feed.name)
            self.attr_texts["Digitiser"].set_text(m.dig_id or "—")
            
            self.attr_texts["Mode"].set_text(m.mode.name)
            self.attr_rects["Mode"].set_color(
                {'OPERATE': 'tab:green', 'STANDBY_FP': 'tab:blue', 'STOW': 'gold', 'SHUTDOWN': 'tab:red', 'STARTUP': 'tab:olive',
                 'MAINTENANCE': 'tab:red', 'UNKNOWN': 'tab:red', 'CONFIG': 'tab:olive'}.get(m.mode.name, 'tab:gray'))

            self.attr_texts["Capability"].set_text('DEGRADED' if m.capability.name == 'OPERATE_DEGRADED' else m.capability.name)
            self.attr_rects["Capability"].set_color(
                {'UNAVAILABLE': 'tab:gray', 'STANDBY': 'tab:blue', 'CONFIGURING': 'tab:olive',
                 'OPERATE_FULL': 'tab:green', 'OPERATE_DEGRADED': 'gold'}.get(m.capability.name, 'tab:gray'))
            
            self.attr_texts["Pointing State"].set_text(m.pointing_state.name)
            self.attr_rects["Pointing State"].set_color(
                {'SLEW': 'gold', 'READY': 'tab:blue', 'UNKNOWN': 'tab:red',
                 'TRACK': 'tab:green', 'SCAN': 'tab:olive'}.get(m.pointing_state.name, 'tab:gray'))

            if m.target is not None:
                self.attr_texts["Target"].set_text(m.target.id or "—")
                self.attr_rects["Target"].set_color('tab:blue')
                self.attr_texts["Target Type"].set_text(m.target.pointing.name if m.target.pointing is not None else "—")
                self.attr_rects["Target Type"].set_color('tab:blue' if m.target.pointing is not None else 'tab:gray')
            else:
                self.attr_texts["Target"].set_text("—")
                self.attr_rects["Target"].set_color('tab:gray')
                self.attr_texts["Target Type"].set_text("—")
                self.attr_rects["Target Type"].set_color('tab:gray')
            
            if m.pointing_altaz is not None and isinstance(m.pointing_altaz, dict):
                self.attr_texts["Pointing Az"].set_text(f"{m.pointing_altaz.get('az', 0):.4f}°")
                self.attr_texts["Pointing Alt"].set_text(f"{m.pointing_altaz.get('alt', 0):.4f}°")
            if m.desired_altaz is not None and isinstance(m.desired_altaz, dict):
                self.attr_texts["Desired Az"].set_text(f"{m.desired_altaz.get('az', 0):.4f}°")
                self.attr_texts["Desired Alt"].set_text(f"{m.desired_altaz.get('alt', 0):.4f}°")
            if m.velocity_altaz is not None and isinstance(m.velocity_altaz, dict):
                self.attr_texts["Velocity Az"].set_text(f"{m.velocity_altaz.get('az', 0):.4f}°/s")
                self.attr_texts["Velocity Alt"].set_text(f"{m.velocity_altaz.get('alt', 0):.4f}°/s")
            
            pec_alt, pec_az = self.driver.get_current_pec()
            if pec_alt is not None and pec_az is not None:
                self.attr_texts["Deviation Az"].set_text(f"{pec_az:.4f}°")
                self.attr_texts["Deviation Alt"].set_text(f"{pec_alt:.4f}°")
                self.attr_rects["Deviation Az"].set_color('tab:red' if abs(pec_az) > 10 else 'gold' if abs(pec_az) > 1 else 'tab:green')
                self.attr_rects["Deviation Alt"].set_color('tab:red' if abs(pec_alt) > 10 else 'gold' if abs(pec_alt) > 1 else 'tab:green')
            else:
                self.attr_texts["Deviation Az"].set_text("—")
                self.attr_texts["Deviation Alt"].set_text("—")
                self.attr_rects["Deviation Az"].set_color('tab:gray')
                self.attr_rects["Deviation Alt"].set_color('tab:gray')

        if self.driver.pointing_altaz_hist is not None:

            pointing_hist_copy = self.driver.pointing_altaz_hist.copy()

            dates = [datetime.datetime.fromtimestamp(ts) for ts in pointing_hist_copy[:, 0] if ts > 0]
            alt_pointing = pointing_hist_copy[pointing_hist_copy[:, 0] > 0, 1]
            az_pointing = pointing_hist_copy[pointing_hist_copy[:, 0] > 0, 2]

            if len(dates) > 0 and len(alt_pointing) > 0 and len(az_pointing) > 0:
                self.axes[1].cla() # Clear the pointing axes for reuse
                self.axes[1].plot(dates, alt_pointing, label=f'Pointing Alt {alt_pointing[-1]:.2f}°', color='tab:blue')
                self.axes[1].plot(dates, az_pointing, label=f'Pointing Az {az_pointing[-1]:.2f}°', color='tab:red')

                # Draw min/max altitude limit lines
                min_alt, max_alt = self.driver.get_min_max_alt()
                self.axes[1].axhline(y=min_alt, color='tab:purple', linestyle='dashed', linewidth=1.5, label=f'Min Alt {min_alt:.1f}°')
                self.axes[1].axhline(y=max_alt, color='tab:purple', linestyle='dashed', linewidth=1.5, label=f'Max Alt {max_alt:.1f}°')

                self.axes[1].set_title("Pointing Altitude and Azimuth")
                self.axes[1].set_ylabel("Pointing AltAz [Deg]")
                self.axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%M:%S'))
                self.axes[1].tick_params(axis='x', labelsize=7)
                self.axes[1].set_xlabel("Time [MM:SS]")
                self.axes[1].grid(True)
                self.axes[1].legend(loc='upper left')

        if self.driver.desired_altaz_hist is not None:
            
            desired_hist_copy = self.driver.desired_altaz_hist.copy()

            dates_desired = [datetime.datetime.fromtimestamp(ts) for ts in desired_hist_copy[:, 0] if ts > 0]
            alt_desired = desired_hist_copy[desired_hist_copy[:, 0] > 0, 1]
            az_desired = desired_hist_copy[desired_hist_copy[:, 0] > 0, 2]

            if len(dates_desired) > 0 and len(alt_desired) > 0 and len(az_desired) > 0:
                self.axes[2].cla() # Clear the desired pointing axes for reuse
                self.axes[2].plot(dates_desired, alt_desired, label=f'Desired Alt {alt_desired[-1]:.2f}°', color='tab:green')
                self.axes[2].plot(dates_desired, az_desired, label=f'Desired Az {az_desired[-1]:.2f}°', color='tab:orange')
                self.axes[2].set_title("Desired Altitude and Azimuth")
                self.axes[2].set_ylabel("Desired AltAz [Deg]")
                self.axes[2].xaxis.set_major_formatter(mdates.DateFormatter('%M:%S'))
                self.axes[2].tick_params(axis='x', labelsize=7)
                self.axes[2].set_xlabel("Time [MM:SS]")
                self.axes[2].grid(True)
                self.axes[2].legend(loc='upper left')

        if self.driver.pec_hist is not None:

            pec_hist_copy = self.driver.pec_hist.copy()

            dates = [datetime.datetime.fromtimestamp(ts) for ts in pec_hist_copy[:, 0] if ts > 0]
            alt_pec = pec_hist_copy[pec_hist_copy[:, 0] > 0, 1]
            az_pec = pec_hist_copy[pec_hist_copy[:, 0] > 0, 2]

            alt_pec_rms, az_pec_rms = self.driver.get_rms_pec()

            if len(alt_pec) > 0 and len(az_pec) > 0:
                self.axes[4].cla() # Clear the PEC axes for reuse
                self.axes[4].fill_between(dates, alt_pec, alpha=0.2, color='tab:blue', label=f'Alt PEC {alt_pec[-1]:.3f}° (RMS: {alt_pec_rms:.3f}°)')
                self.axes[4].fill_between(dates, az_pec, alpha=0.2, color='tab:red', label=f'Az PEC {az_pec[-1]:.3f}° (RMS: {az_pec_rms:.3f}°)')

            self.init_pec_axes(self.axes[4])

        # If we cannot determine which figure is active, draw all figures
        if is_visible_fig is None:
            plt.draw()                      # A figure will become active when plt.draw() is called
            plt.pause(0.0001)
        elif is_visible_fig == True:
            self.fig.canvas.draw()          # Draw only this figure
            self.fig.canvas.flush_events()  # Process events for this figure only
        else:
            self.fig.canvas.draw_idle()     # Schedules a redraw without forcing a window focus change
            self.fig.canvas.flush_events()  # Process events for this figure only

    def save_dsh_figure(self, output_dir: str) -> bool:
        """ Save the current dish figure to disk
            :param output_dir: The directory to save the figure in
        """
        if self.fig is None:
            return False

        if output_dir is None or output_dir == "":
            output_dir = "."

        filename = f"{output_dir}/" + self.driver.dsh_model.dsh_id + ".png"

        # Expand user (~) and ensure the directory exists before saving
        filepath = Path(filename).expanduser()
        filepath.parent.mkdir(parents=True, exist_ok=True)

        self.fig.savefig(str(filepath))
        logger.info(f"Dish display for dish {self.driver.dsh_model.dsh_id} figure saved to {filepath}")
        return True

    def init_pec_axes(self, axes):
        """ Initialise periodic error correction (pec) plot axes """

        axes.set_title("Periodic Error Correction (PEC)")
        axes.set_ylabel("PEC [Deg]")
        axes.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        axes.set_xlabel("Time [HH:MM:SS]")
        axes.grid(True)
        axes.legend(loc='upper right')

    def init_attribute_axes(self, axes):
        """ Initialise attribute plot axes """

        axes.set_title("Dish Attributes")
        axes.grid(True)

        axes.set_xlim(0,10)
        axes.set_ylim(0,10)
        axes.axis('off')

        for x, y, label in self.ATTR_LABELS:
            rect_x = self.RECT_LEFT_X if x < 5 else self.RECT_RIGHT_X
            rect_w = self.RECT_W * 3.1 if label in ["Target", "Target Type"] else self.RECT_W
            axes.text(rect_x - self.LABEL_GAP, y, label, ha='right', va='center', fontsize=9)
            rect = plt.Rectangle((rect_x, y - 0.2), rect_w, self.RECT_H, color='tab:gray', alpha=0.5)
            axes.add_patch(rect)
            self.attr_rects[label] = rect
            # Value text centered inside the rectangle
            txt = axes.text(rect_x + rect_w / 2, y, "", ha='center', va='center', fontsize=8)
            self.attr_texts[label] = txt

    def clear_axes_data(self, ax):
        """Clear plot data from axes without removing titles and labels."""
        # Remove all lines
        for line in ax.get_lines():
            line.remove()
        
        # Remove all collections (e.g., scatter plots, bar plots)
        for coll in ax.collections:
            coll.remove()
        
        # Remove all patches (e.g., bars, rectangles)
        for patch in list(ax.patches):
            patch.remove()
        
        # Remove all images (e.g., imshow)
        for img in ax.images:
            img.remove()
        
        # Remove all texts (except title and axis labels)
        for txt in ax.texts:
            txt.remove()

        # Remove legend if present
        legend = ax.get_legend()
        if legend is not None:
            legend.remove()


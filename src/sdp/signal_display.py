from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdp.scan import Scan
    
import numpy as np
import matplotlib.pyplot as plt
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
from queue import Queue
from util import gen_file_prefix
from pathlib import Path

import logging
logger = logging.getLogger(__name__)

FIG_SIZE = (14, 7)  # Default figure size for plots

class SignalDisplay:

    def __init__(self, dig_id: str):
        """ Initialize the signal display for a given digitiser ID
            :param dig_id: The digitiser ID to display signals for
        """

        #mpl.use('TkAgg')        # Use TkAgg backend for interactive display

        self.is_active = True  # Is this signal display instance active
        self.dig_id = dig_id    # Current digitiser signal being displayed
        
        self.scan = None        # Current digitiser scan being displayed
        self.sec = None         # Current scan second being displayed

        self.fig = None         # Figure for the digitiser signal display
        self.sig = [None] * 5   # Axes for the signal subplots
        self.pwr_im = None      # Image for the power spectrum
        self.extent = None      # Extent for the imshow plot

        self.gs0 = GridSpec(1, 3, width_ratios=[1, 1, 1], left=0.07, right=0.93, top=0.90, bottom=0.3, wspace=0.2) # signal displays
        self.gs1 = GridSpec(1, 2, width_ratios=[0.32, 0.68], height_ratios=[1], left=0.07, right=0.93, top=0.20, bottom=0.07, wspace=0.2) # total power timeline display

    def is_active_figure(self) -> bool:
        """ Return whether this signal display figure is active 
            The signal display is considered active if its figure window is the key window in the OS
        """

        if not HAS_APPKIT or self.fig is None:
            logger.warning(
                f"Signal display checking whether figure for {self.dig_id} is active but "
                + ("AppKit not available" if not HAS_APPKIT else "figure is None"))
            return None

        key_window = NSApplication.sharedApplication().keyWindow()
        if key_window is None:
             return None

        key_window_title = key_window.title()                   # Get the title of the key window
        fig_title = self.fig.canvas.manager.get_window_title()  # Get the title of the signal display figure
        return (fig_title == key_window_title)

    def is_active(self) -> bool:
        """ Return whether this signal display instance is active """
        return self.is_active

    def set_is_active(self, active: bool):
        """ Set whether this signal display instance is active """
        self.is_active = active
    
    def _on_focus(self, event):
        """ Handle focus event on the figure """
        logger.info(f"Signal display {event.canvas.figure.get_suptitle()} for {self.dig_id} focused")

    def _on_defocus(self, event):
        """ Handle defocus event on the figure """
        logger.info(f"Signal display {event.canvas.figure.get_suptitle()} for {self.dig_id} defocused")
    
    def set_scan(self, scan : Scan):
        """ # Initialize the figure and axes of the signal displays for a given scan
            :param scan: The Scan object containing the data to display
        """
        if scan is None:
            logger.warning(f"Signal display for {self.dig_id} cannot set_scan when scan is None")
            return

        if not scan.scan_model.dig_id == self.dig_id:
            logger.warning(f"Signal display for {self.dig_id} cannot set_scan for scan with different dig_id {scan.scan_model.dig_id}")
            return
        
        self.scan = scan
        self.sec = None

        # If the figure doesn't exist yet, create it
        if self.fig is None:

            self.fig = plt.figure(num=f"Digitiser {self.dig_id}", figsize=FIG_SIZE)
            self.fig.canvas.mpl_connect('figure_enter_event', self._on_focus)
            self.fig.canvas.mpl_connect('figure_leave_event', self._on_defocus)

            self.sig = [None] * 5  # Initialize axes for the subplots
            self.sig[0] = self.fig.add_subplot(self.gs0[0]) # Power spectrum summed per second
            self.sig[1] = self.fig.add_subplot(self.gs0[1]) # Sky signal per second
            self.sig[2] = self.fig.add_subplot(self.gs0[2]) # Waterfall plot

            self.sig[3] = self.fig.add_subplot(self.gs1[0]) # SDR Saturation Levels
            self.sig[4] = self.fig.add_subplot(self.gs1[1]) # Total power timeline

            self.fig.subplots_adjust(top=0.78)

        else:
            # Clear existing axes for reuse
            for ax in self.sig:
                if ax is not None:
                    ax.cla()

            # Remove the waterfall subplot and its colorbar entirely, then recreate
            if self.sig[2] is not None:
                self.sig[2].remove()
                self.sig[2] = None

            # Recreate the waterfall subplot
            self.sig[2] = self.fig.add_subplot(self.gs0[2])

        # Reset the power image reference
        self.pwr_im = None

        # Update the figure suptitle for the new scan
        self.fig.suptitle(
            f"Scan: {scan.scan_model.scan_id} Center Freq: {scan.scan_model.center_freq/1e6:.2f} MHz, "
            f"Gain: {scan.scan_model.gain} dB, Sample Rate: {scan.scan_model.sample_rate/1e6:.2f} MHz, "
            f"Channels: {scan.scan_model.channels} Load: {scan.scan_model.load}",
            fontsize=12, y=0.96
        )

        self.extent = [
            (scan.scan_model.center_freq + scan.scan_model.sample_rate / -2) / 1e6,
            (scan.scan_model.center_freq + scan.scan_model.sample_rate / 2) / 1e6,
            scan.scan_model.duration,
            0
        ]

        # Set axes properties
        self.init_pwr_spectrum_axes(self.sig[0], "Power/Sec (SPR,BSL)", self.extent, units="{np.abs(shift fft(signal))**2}")
        self.init_pwr_spectrum_axes(self.sig[1], "Power/Sec (SPR/BSL)", self.extent)
        self.init_saturation_axes(self.sig[3])
        self.init_total_power_axes(self.sig[4], self.scan.scan_model.duration)

    def get_scan(self) -> Scan:
        """ Get the current scan being displayed """
        return self.scan
   
    def display(self):
        """
        Update the signal displays for the current scan.
        """

        # Close the figure if the signal display is not active
        if self.is_active is False:
            if self.fig is not None:
                plt.close(self.fig)
                self.fig = None
            self.scan = None
            return
        else: # Else ensure we have an active figure with a valid scan to display
            is_active_fig = self.is_active_figure()
            # If no scan, no figure or figure is not active, then return
            if self.scan is None or self.fig is None or is_active_fig == False:
                return

        # Get the number of loaded seconds in the scan (starts at 1...scan.duration)
        l_sec = self.scan.get_loaded_seconds()
        # If no seconds are loaded in the scan or the current displayed scan second is the same as loaded scan seconds, return
        if l_sec <= 0:
            return

        logger.info(f"Signal display updating for scan {self.scan.scan_model.scan_id}, from second {self.sec} to {l_sec} of {self.scan.scan_model.duration}")

        # If current second being displayed is None, initialize the plots
        if self.sec is None:

            self.sig[2].cla()
            self.pwr_im = self.sig[2].imshow(self.scan.spr / self.scan.bsl, aspect='auto', extent=self.extent)
            self.sig[4].plot([1], [np.sum(self.scan.spr[0, :])], color='red', label='Total Power')  # Plot total power across all channels for the first second
            self.sec = 0

        else:
            # Clear existing plot data from the axes without removing titles and labels
            self.clear_axes_data(self.sig[0])
            self.clear_axes_data(self.sig[1])
            self.clear_axes_data(self.sig[3])
            self.clear_axes_data(self.sig[4])

            # Update the existing power spectrum image with new data
            self.pwr_im.set_data(self.scan.spr / self.scan.bsl)

        # Plot total power across all channels for each second up to the current second
        self.sig[4].plot(
            np.arange(1, l_sec + 1), 
            [np.sum(self.scan.spr[s, :]) for s in range(l_sec)], 
            color='red', 
            label='Total Power (TPW)'
        )  
            
        int_gain_cal = [1.0] * 10  # Placeholder for internal gain calibration values
        label = 'Load (BSL)' if np.mean(int_gain_cal) == 1.0 else 'Gain (BSL)'

        # Plot the summed power spectrum and sky signal for the current second
        self.sig[0].plot(
            np.linspace(self.extent[0], self.extent[1], self.scan.scan_model.channels), 
            self.scan.spr.flatten()[(l_sec-1)*self.scan.scan_model.channels:(l_sec)*self.scan.scan_model.channels], 
            color='red', 
            label='Signal (SPR)'
        )

        self.sig[0].plot(
            np.linspace(self.extent[0], self.extent[1], self.scan.scan_model.channels), 
            self.scan.bsl, 
            color='black', 
            label=label
        )
        self.sig[0].legend(loc='lower right')

        self.sig[1].plot(
            np.linspace(self.extent[0], self.extent[1], self.scan.scan_model.channels), 
            self.scan.spr.flatten()[(l_sec-1)*self.scan.scan_model.channels:(l_sec)*self.scan.scan_model.channels] / self.scan.bsl, 
            color='orange', 
            label='Signal (SPR/BSL)'
        )
        self.sig[1].legend(loc='lower right')

        self.sig[3].bar(0, self.scan.mean_real, color='blue', label='I' if self.sec == 0 else '_nolegend_')
        self.sig[3].bar(1, self.scan.mean_imag, color='orange', label='Q' if self.sec == 0 else '_nolegend_')
        # Draw a line at the 33% and 66% marks
        self.sig[3].axhline(y=33, color='green', linestyle='--', label='33%')
        self.sig[3].axhline(y=66, color='red', linestyle='--', label='66%')
        self.sig[3].legend(loc='lower right')

        if l_sec == self.scan.scan_model.duration:
            tpw = np.zeros(self.scan.scan_model.duration, dtype=np.float64)  # Initialise (scans * duration * iterations) array for total power sky timeline
            # Sum the power spectrum for each second in the current scan
            tpw[:] = np.sum(self.scan.spr, axis=1) # Total Power per second
            # Avg of the total power per second
            avg_tpwr = np.mean(tpw[:])
            self.sig[4].axhline(y=avg_tpwr, color='red', linestyle='--', label=f'Mean {avg_tpwr:.3e}')
        self.sig[4].legend(loc='lower right')

        # If we cannot determine which figure is active, draw all figures
        if is_active_fig is None:
            plt.draw()                      # A figure will become active when plt.draw() is called
            plt.pause(0.0001)
        elif is_active_fig == True:
            self.fig.canvas.draw()          # Draw only this figure
            self.fig.canvas.flush_events()  # Process events for this figure only
        else:
            self.fig.canvas.draw_idle()     # Schedules a redraw without forcing a window focus change
            self.fig.canvas.flush_events()  # Process events for this figure only

        self.sec = l_sec

    def save_scan_figure(self, output_dir: str) -> bool:
        """ Save the current scan figure to disk
            :param output_dir: The directory to save the figure in
        """
        if self.scan is None or self.fig is None:
            return False

        if output_dir is None or output_dir == "":
            output_dir = "."

        prefix = gen_file_prefix(dt=self.scan.scan_model.read_start, entity_id=self.scan.scan_model.dig_id, gain=self.scan.scan_model.gain, 
            duration=self.scan.scan_model.duration, sample_rate=self.scan.scan_model.sample_rate, center_freq=self.scan.scan_model.center_freq, 
            channels=self.scan.scan_model.channels, instance_id=self.scan.scan_model.scan_id, filetype="sigfig")

        filename = f"{output_dir}/" + prefix + ".png"

        # Expand user (~) and ensure the directory exists before saving
        filepath = Path(filename).expanduser()
        filepath.parent.mkdir(parents=True, exist_ok=True)

        self.fig.savefig(str(filepath))
        logger.info(f"Signal display scan {self.scan.scan_model.scan_id} figure saved to {filepath}")
        return True

    def init_pwr_spectrum_axes(self, axes, title, extent, units='[a.u.]'):
        """ Initialise pwr spectrum plot axes """

        # Set up the summed power spectrum plot
        axes.set_title(title) # Summed Power Spectrum / Sky Signal
        axes.set_xlabel("Frequency [MHz]")
        axes.set_ylabel("Power Spectrum"+ f" {units}")
        axes.set_xlim(extent[0], extent[1])
        axes.grid(True)

    def init_waterfall_axes(self, axes):
        """ Initialise waterfall plot axes """

        # Set up the waterfall plot
        axes.set_title("Waterfall Plot of Spectrum")
        axes.set_xlabel("Frequency [MHz]")
        axes.set_ylabel("Time [sec]")
        axes.set_aspect('auto')
        axes.set_facecolor('black')
        axes.grid(False)

    def init_saturation_axes(self, axes):
        """ Initialise SDR saturation levels plot axes """

        # Set up the SDR saturation levels plot
        axes.set_title("SDR Saturation Level")
        axes.set_xlabel("Mean(I), Mean(Q)")
        axes.set_ylabel("Saturation [%]")
        axes.set_ylim(0, 100)
        axes.set_facecolor('white')
        axes.grid(True)

    def init_total_power_axes(self, axes, duration):
        """ Initialise total power timeline plot axes """

        # Set up the total power timeline plot
        axes.set_title("Total Power Timeline")
        axes.set_xlabel("Time [sec]")
        axes.set_ylabel("Total Power [a.u.]")
        axes.set_facecolor('white')
        axes.set_xlim(1, duration)
        axes.grid(True)

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


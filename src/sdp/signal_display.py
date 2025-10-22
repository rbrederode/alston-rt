from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdp.scan import Scan
    
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

from matplotlib.gridspec import GridSpec
from queue import Queue
from util import gen_file_prefix

import logging
logger = logging.getLogger(__name__)

FIG_SIZE = (14, 7)  # Default figure size for plots

class SignalDisplay:

    def __init__(self):

        #mpl.use('TkAgg')        # Use TkAgg backend for interactive display

        self.scan = None        # Current scan being displayed
        self.sec = None         # Current second being displayed

        self.fig = None         # Figure for the signal displays    
        self.sig = [None] * 5   # Axes for the signal subplots
        self.pwr_im = None      # Image for the power spectrum
        self.extent = None      # Extent for the imshow plot

        self.gs0 = GridSpec(1, 3, width_ratios=[1, 1, 1], left=0.07, right=0.93, top=0.90, bottom=0.3, wspace=0.2) # signal displays
        self.gs1 = GridSpec(1, 2, width_ratios=[0.32, 0.68], height_ratios=[1], left=0.07, right=0.93, top=0.20, bottom=0.07, wspace=0.2) # total power timeline display

    def set_scan(self, scan : Scan):
        """ # Initialize the figure and axes of the signal displays for a given scan
            :param scan: The Scan object containing the data to display
        """
        self.scan = scan
        self.sec = None
        
        plt.close(scan.id)  # Close any existing figure with the same scan number

        self.fig = plt.figure(num=scan.id, figsize=FIG_SIZE)
        self.sig = [None] * 5  # Initialize axes for the subplots

        self.sig[0] = self.fig.add_subplot(self.gs0[0]) # Power spectrum summed per second
        self.sig[1] = self.fig.add_subplot(self.gs0[1]) # Sky signal per second
        self.sig[2] = self.fig.add_subplot(self.gs0[2]) # Waterfall plot

        self.sig[3] = self.fig.add_subplot(self.gs1[0]) # SDR Saturation Levels
        self.sig[4] = self.fig.add_subplot(self.gs1[1]) # Total power timeline

        self.fig.subplots_adjust(top=0.78)
        self.fig.suptitle(f"Scan: {scan.id}-{scan.id} Center Frequency: {scan.center_freq/1e6:.2f} MHz, Gain: {scan.gain} dB, Sample Rate: {scan.sample_rate/1e6:.2f} MHz, Channels: {scan.channels}", fontsize=12, y=0.96)

        self.extent = [(scan.center_freq + scan.sample_rate / -2) / 1e6, (scan.center_freq + scan.sample_rate / 2) / 1e6, scan.duration, 0]

    def display(self):
        """
        Update the signal displays for the current scan.
        """

        # If no scan is set, return
        if self.scan is None:
            return

        # Get the number of loaded seconds in the scan (starts at 1...scan.duration)
        l_sec = self.scan.get_loaded_seconds()
        # If no seconds are loaded, return
        if l_sec <= 0:
            return

        logger.info(f"Signal display updating for scan {self.scan.id}, from second {self.sec} to {l_sec} of {self.scan.duration}")

        if self.sec is None:

            self.sig[2].cla()
            self.pwr_im = self.sig[2].imshow(self.scan.spr / self.scan.bsl, aspect='auto', extent=self.extent)
            self.fig.colorbar(self.pwr_im, ax=self.sig[2], label='Power Spectrum [a.u.]')
            self.sig[4].plot([1], [np.sum(self.scan.spr[0, :])], color='red', label='Total Power')  # Plot total power across all channels for the first second
            self.sec = 0

        else:
            
            # Clear the previous plots (all except the waterfall plot self.sig[2])
            self.sig[0].cla()
            self.sig[1].cla()
            self.sig[3].cla()
            self.sig[4].cla()

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
            np.linspace(self.extent[0], self.extent[1], self.scan.channels), 
            self.scan.spr.flatten()[(l_sec-1)*self.scan.channels:(l_sec)*self.scan.channels], 
            color='red', 
            label='Signal (SPR)'
        )

        self.sig[0].plot(
            np.linspace(self.extent[0], self.extent[1], self.scan.channels), 
            self.scan.bsl, 
            color='black', 
            label=label
        )
        self.sig[0].legend(loc='lower right')

        self.sig[1].plot(
            np.linspace(self.extent[0], self.extent[1], self.scan.channels), 
            self.scan.spr.flatten()[(l_sec-1)*self.scan.channels:(l_sec)*self.scan.channels] / self.scan.bsl, 
            color='orange', 
            label='Signal (SPR/BSL)'
        )
        self.sig[1].legend(loc='lower right')

        row_start = int(np.ceil((l_sec-1) * self.scan.sample_rate / self.scan.channels))
        row_end = int(np.ceil(l_sec * self.scan.sample_rate / self.scan.channels))

        if hasattr(self.scan, 'raw'):
            # Plot 10% of raw IQ samples for the current second
            indices = np.linspace(row_start, row_end - 1, int(self.scan.raw.shape[0]*0.01), dtype=int)

            mean_real = np.mean(np.abs(self.scan.raw[row_start:row_end, ].real))*100  # Find the mean real value in the raw samples (I)
            mean_imag = np.mean(np.abs(self.scan.raw[row_start:row_end, ].imag))*100  # Find the mean imaginary value in the raw samples (Q)

            self.sig[3].bar(0, mean_real, color='blue', label='I')
            self.sig[3].bar(1, mean_imag, color='orange', label='Q')
            # Draw a line at the 33% and 66% marks
            self.sig[3].axhline(y=33, color='green', linestyle='--', label='33%')
            self.sig[3].axhline(y=66, color='red', linestyle='--', label='66%')
            self.sig[3].legend(loc='lower right')
        
        if l_sec == self.scan.duration:
            tpw = np.zeros(self.scan.duration, dtype=np.float64)  # Initialise (scans * duration * iterations) array for total power sky timeline
             # Sum the power spectrum for each second in the current scan
            tpw[:] = np.sum(self.scan.spr, axis=1) # Total Power per second
            # Avg of the total power per second
            avg_tpwr = np.mean(tpw[:])
            self.sig[4].axhline(y=avg_tpwr, color='red', linestyle='--', label=f'Mean {avg_tpwr:.3e}')
        self.sig[4].legend(loc='lower right')

        self.init_pwr_spectrum_axes(self.sig[0], "Power/Sec (SPR,BSL)", self.extent, units="{np.abs(shift fft(signal))**2}")  # Initialize the summed power spectrum axes
        self.init_pwr_spectrum_axes(self.sig[1], "Power/Sec (SPR/BSL)", self.extent)  # Initialize the sky signal axes
        self.init_waterfall_axes(self.sig[2])                        # Initialize the waterfall plot axes
        self.init_saturation_axes(self.sig[3])                       # Initialize the saturation axes
        self.init_total_power_axes(self.sig[4], self.scan.duration)  # Initialize the total power timeline axes

        # Show the signal displays to the user
        plt.draw()
        plt.pause(0.0001)

        self.sec = l_sec

    def save_scan_figure(self, output_dir: str) -> bool:
        """ Save the current scan figure to disk
            :param output_dir: The directory to save the figure in
        """
        if self.scan is None or self.fig is None:
            return False

        if output_dir is None or output_dir == "":
            output_dir = "."

        prefix = gen_file_prefix(dt=self.scan.read_start, feed=self.scan.feed, gain=self.scan.gain, duration=self.scan.duration, 
            sample_rate=self.scan.sample_rate, center_freq=self.scan.center_freq, channels=self.scan.channels, entity_id=self.scan.id, filetype="sigfig")

        filename = f"{output_dir}/" + prefix + ".png"

        self.fig.savefig(filename)
        logger.info(f"Signal display scan {self.scan.id} figure saved to {filename}")
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


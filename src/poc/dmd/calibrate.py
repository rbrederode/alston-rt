import os
import numpy as np
import datetime
import argparse
import logging

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

from astropy import units as u
from scipy.signal import savgol_filter
from scipy.ndimage import gaussian_filter1d

import proto as dmd

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Or DEBUG for more verbosity
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Alston Observatory'
EPSILON = 0.1

def load_hotcold_data(args, scan_center_freq, extent, duration, fft_size, scan=None):

    global OUTPUT_DIR

    # If the hot and cold power spectrum filenames are not provided
    if args.pwr_hot.lower() == 'none' or args.pwr_cold.lower() == 'none':
        logger.error("No calibration filenames provided. Please specify valid hot and cold power spectrum CSV files.")
        return

    # If the hot power spectrum file date and time is provided, use that to generate the prefix
    if len(args.pwr_hot) == 17:
        prefix = args.pwr_hot + dmd.gen_file_prefix(args, duration=duration, center_freq=scan_center_freq, type='cal', freq_scan=scan)[17:]
        logger.info(f"Looking for hot file with prefix {prefix} in {OUTPUT_DIR}")
        read_hot_files = [f for f in os.listdir(OUTPUT_DIR) if prefix in f and f.endswith('cal.csv')]
    else: # If the hot power spectrum file date and time is not provided, use the full filename
        logger.info(f"Looking for hot file with name {args.pwr_hot} in {OUTPUT_DIR}")
        read_hot_files = [f for f in os.listdir(OUTPUT_DIR) if args.pwr_hot in f and f.endswith('cal.csv')]

    # If the cold power spectrum file date and time is provided, use that to generate the prefix
    if len(args.pwr_cold) == 17:
        prefix = args.pwr_cold + dmd.gen_file_prefix(args, duration=duration, center_freq=scan_center_freq, type='cal', freq_scan=scan)[17:]
        logger.info(f"Looking for cold file with prefix {prefix} in {OUTPUT_DIR}")
        read_cold_files = [f for f in os.listdir(OUTPUT_DIR) if prefix in f and f.endswith('cal.csv')]
    else: # If the cold power spectrum file date and time is not provided, use the full filename
        logger.info(f"Looking for cold file with name {args.pwr_cold} in {OUTPUT_DIR}")
        read_cold_files = [f for f in os.listdir(OUTPUT_DIR) if args.pwr_cold in f and f.endswith('cal.csv')]

    if not read_hot_files or not read_cold_files:
        logger.error(f"No matching hot or cold files found in {OUTPUT_DIR}. Please generate the power spectrum files first.")
        return None, None

    # Load the power spectrum files
    hot_pwr = load_file(read_hot_files[0]) if read_hot_files else None
    cold_pwr = load_file(read_cold_files[0]) if read_cold_files else None

    return hot_pwr, cold_pwr

def load_file(filename):
    """
    Load a power spectrum from a CSV file in the output directory.
    If the file does not exist, prompt the user to generate it first.
    """
    global OUTPUT_DIR

    # Look for files in the output directory ending with 'load.csv' that match the prefix i.e. same parameters (center_freq, duration etc)
    files = [f for f in os.listdir(OUTPUT_DIR) if filename in f and f.endswith('.csv')]
    
    if files: # If we found matching load files
        file = sorted(files)[-1] # Identify the most recent matching file and use that one
        logger.info(f"Loading power spectrum {file}")

        pwr = np.genfromtxt(OUTPUT_DIR + "/" + file, delimiter=",")
    else:
        logger.error(f"No power spectrum file with name {filename} found in {OUTPUT_DIR}. Please generate a power spectrum file first.")
        pwr = None

    return pwr

def load_baseline(prefix, int_bsl_pwr, scan=None):
    """
    Load the baseline power spectrum from a CSV file in the output directory.
    If the file does not exist, prompt the user to generate it first.
    """

    # Look for files in the output directory ending with 'load.csv' that match the prefix i.e. same parameters (center_freq, duration etc)
    load_files = [f for f in os.listdir(OUTPUT_DIR) if prefix[17:] in f and f.endswith('load.csv')]
    
    if load_files: # If we found matching load files
        load_file = sorted(load_files)[-1] # Identify the most recent load file and use that one
        logger.info(f"Loading baseline power spectrum {load_file}")

        bsl = np.genfromtxt(OUTPUT_DIR + "/" + load_file, delimiter=",")

        int_bsl_pwr[scan,] = bsl  # Record the baseline power spectrum for the given scan
        return load_file
    else:
        logger.error(f"No load file with parameters {prefix[17:]} found in {OUTPUT_DIR}. Please generate a load file first using the -l option.")

    return None

def save_calibration_csv(data, filename):
    """
    Save the calibration data to CSV file in the output directory.
    """
    global OUTPUT_DIR

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)  # Create the output directory if it does not exist

    if data is not None:
        # Save the calibration data to a CSV file
        np.savetxt(f"{OUTPUT_DIR}/{filename}.csv", data, delimiter=",", fmt="%s")
        logger.info(f"Calibration file saved as {filename}.csv")
    else:
        logger.warning("Calibration data is None. Not saving calibration file.")

def init_args():
    """
    Initialize command line arguments for the SDR observation script.
    """
    parser = argparse.ArgumentParser(description='SDR Observation')
    parser.add_argument('-ph', '--pwr_hot', type=str, help='CSV filename containing power spectrum for hot calibration', default='hot.csv')
    parser.add_argument('-pc', '--pwr_cold', type=str, help='CSV filename containing power spectrum for cold calibration', default='cold.csv')
    parser.add_argument('-th', '--temp_hot', type=float, help='Temperature of hot source in Kelvin', default=293.0)
    parser.add_argument('-tc', '--temp_cold', type=float, help='Temperature of cold source in Kelvin', default=10.0)
    parser.add_argument('-s', '--sample_rate', type=float, help='Sample rate in Hz e.g. 2.048e6 for 2.048 MHz, max sample rate is 3.2 MHz, however max rate that does not drop samples is 2.56 MHz https://www.rtl-sdr.com/about-rtl-sdr/', default=2.4e6)
    parser.add_argument('-c', '--center_freq', type=float, help='Center frequency in Hz e.g. 1420.4e6 for 1420.4 MHz, valid range is 25 - 1750 MHz ref: NESDR SMArt v5 RTL-SDR Software Defined Radio Data Sheet', default=1420.4e6)
    parser.add_argument('-d', '--duration', type=int, help='Observation duration (integration time)', default=60)
    parser.add_argument('-f', '--fft_size', type=int, help='Number of frequency bins (FFT size)', default=1024)
    parser.add_argument('-b', '--bandwidth', type=float, help='Bandwidth in MHz e.g. 2.4', default=1.0)
    parser.add_argument('-t', '--telescope', type=str, help='Telescope descriptor e.g. TableTop or Helical', default='')
    parser.add_argument('-g', '--gain', type=str, help='SDR gain in dB between 0-49.6 or "auto"', default='12')

    return parser.parse_args()

def init_cal_displays():

    gs0 = GridSpec(1, 3, width_ratios=[1, 1, 1], left=0.07, right=0.93, top=0.93, bottom=0.07, wspace=0.2) # calibration displays

    # Initialize the figure and axes for the calibration displays
    fig = plt.figure(num=1000, figsize=dmd.FIG_SIZE)
    cal = [None] * 3  # Initialize axes for the subplots

    cal[0] = fig.add_subplot(gs0[0]) # Uncalibrated hot and cold spectra
    cal[1] = fig.add_subplot(gs0[1]) # Calculated Gain and Tsys 
    cal[2] = fig.add_subplot(gs0[2]) # Calibrated hot and cold spectra

    # Set the titles and labels for the uncalibrated spectra plot
    cal[0].set_title('Uncalibrated Spectra')
    cal[0].set_xlabel('Frequency (Hz)')
    cal[0].set_ylabel('Power [a.u.]')
    cal[0].grid(True)

    # Set the titles and labels for the calibration spectra plot
    cal[1].set_title('Gain / Tsys Spectra')
    cal[1].set_xlabel('Frequency (Hz)')
    cal[1].set_ylabel('Gain [dB] / Tsys [dB]')
    cal[1].grid(True)

    # Set the titles and labels for the calibrated spectra plot
    cal[2].set_title('Calibrated Spectra')
    cal[2].set_xlabel('Frequency (Hz)')
    cal[2].set_ylabel('Brightness Temperature [K]')
    cal[2].grid(True)

    return gs0, fig, cal

def main():

    args = init_args()  # Initialize command line arguments

    cal_start = datetime.datetime.now()  # Get the current date and time for the start of calibration

    start_freq = args.center_freq - (args.bandwidth * 1e6 / 2 + args.sample_rate * (1-dmd.USABLE_BANDWIDTH)/2)  # Start of usable frequency
    end_freq = args.center_freq + (args.bandwidth * 1e6 / 2 + args.sample_rate * (1-dmd.USABLE_BANDWIDTH)/2)  # End of usable frequency
    logger.info(f"Start Frequency: {start_freq/1e6:.2f} MHz, End Frequency: {end_freq/1e6:.2f} MHz")

    # Calculate the number of scans (each of sample_rate * usable_bandwidth) and overlaps to cover start_freq to end_freq
    freq_scans, freq_overlap, scan_iterations, scan_duration, scans_meta = dmd.determine_scans(start_freq=start_freq, end_freq=end_freq, sample_rate=args.sample_rate, duration=args.duration)  

    scan_start_freq = start_freq

    # Initialise integrated (over scans) numpy arrays to hold the uncalibrated hot and cold power spectra
    int_hot_pwr = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for hot power spectrum
    int_cold_pwr = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for cold power spectrum

    # Initialise integrated (over scans) numpy arrays to hold the calculated and adjusted gain and tsys calibration
    int_gain_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for gain calibration
    int_adj_gain_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for adjusted gain calibration
   
    int_tsys_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for Tsys calibration
    int_adj_tsys_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for adjusted Tsys calibration

    # Initialise integrated (over scans) numpy arrays to hold the calibrated hot and cold power spectra
    int_hot_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for calibrated hot power spectrum
    int_cold_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for calibrated cold power spectrum

    # Initialise (scan X fft_size) array for uncalibrated and calibrated baseline load
    int_bsl_pwr = np.ones((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for uncalibrated baseline load power spectrum
    int_bsl_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for calibrated baseline load power spectrum

    # Initialize the calibration displays
    gs0, fig, cal = init_cal_displays()  

    hotline = coldline = None  # Initialize the hot line for the uncalibrated spectra plot
    gainline = tsysline = None  # Initialize the gain and tsys line for the calibration spectra plot
    calhotline = calcoldline = None  # Initialize the hot and cold lines for the calibrated spectra plot
    calbslline = bslline = None  # Initialize the baseline load line for the calibrated spectra plot

    # Calculate the start and end bins for the usable bandwidth
    adj = 0.15  # 5% of the usable bandwidth

    start_bin = int((1-(dmd.USABLE_BANDWIDTH+adj))/2 * args.fft_size)
    end_bin = int(((dmd.USABLE_BANDWIDTH+adj) + (1-(dmd.USABLE_BANDWIDTH+adj))/2) * args.fft_size)

     # Calculate the number of bins that represent 10% of the usable bandwidth
    delta = int(0.10 * (end_bin - start_bin))
    logger.info(f"Usable bandwidth: {(dmd.USABLE_BANDWIDTH+adj)*100:.1f}%, Start bin: {start_bin}, End bin: {end_bin}, FFT size: {args.fft_size}, Delta: {delta}")

    # Iterate over the number of scans
    for scan in range(freq_scans):

        scan_center_freq = (scan_start_freq + args.sample_rate/2)

        extent = [(scan_center_freq + args.sample_rate/-2)/1e6,
                (scan_center_freq + args.sample_rate/2)/1e6]

        # Load the hot and cold power spectra and baseline load power spectrum for the current scan
        int_hot_pwr[scan,:], int_cold_pwr[scan,:] = load_hotcold_data(args, scan_center_freq, extent, args.duration, args.fft_size, scan=scan)
        bsl_file = load_baseline(dmd.gen_file_prefix(args, center_freq=scan_center_freq, freq_scan=scan, type='load'), int_bsl_pwr, scan) 

        # Check if the hot and cold power spectra are valid
        if np.mean(int_hot_pwr[scan,:]) == 0.0 or np.mean(int_cold_pwr[scan,:]) == 0.0:
            logger.error("Error loading hot/cold data files. Please check the file paths and try again.")
            exit(1)

        # Calculate the average of the hot and cold power spectra
        avg_hot = np.mean(int_hot_pwr[0:scan+1,:])
        avg_cold = np.mean(int_cold_pwr[0:scan+1,:])

        # Remove the previous average lines on the plot if they exist
        if hotline or coldline:
            hotline.remove()
            coldline.remove()
            
        if bslline:
            bslline.remove()

        # Create labels for the hot, cold, and baseline power spectra
        hot_label=f"Hot Pwr" if f"Hot Pwr" not in [l.get_label() for l in cal[0].lines] else ""
        cold_label=f"Cold Pwr" if f"Cold Pwr" not in [l.get_label() for l in cal[0].lines] else ""
        bsl_label=f"BSL Pwr" if f"BSL Pwr" not in [l.get_label() for l in cal[0].lines] else ""
        hot_smooth_label=f"Hot Smooth Pwr" if f"Hot Smooth Pwr" not in [l.get_label() for l in cal[0].lines] else ""
        cold_smooth_label=f"Cold Smooth Pwr" if f"Cold Smooth Pwr" not in [l.get_label() for l in cal[0].lines] else ""

        # Plot the uncalibrated hot and cold power spectra
        cal[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_hot_pwr[scan,:], color='red', label=hot_label)
        cal[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_cold_pwr[scan,:], color='blue', label=cold_label)

        # If a baseline power spectrum file was provided
        if bsl_file is not None:
            # Calculate the average baseline power spectrum
            avg_bsl = np.mean(int_bsl_pwr[0:scan+1,:])  

            # Plot the baseline power spectrum and the avg of the baseline power spectrum
            cal[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_bsl_pwr[scan,:], color='black', label=bsl_label)
            bslline = cal[0].axhline(avg_bsl, color='black', linestyle='--', label=f'Avg BSL: {avg_bsl:.2f}')  # Add a horizontal line for the average baseline power spectrum

        # Add average lines for hot and cold power spectra
        hotline = cal[0].axhline(avg_hot, color='red', linestyle='--', label=f'Avg Hot: {avg_hot:.2f}')
        coldline = cal[0].axhline(avg_cold, color='blue', linestyle='--', label=f'Avg Cold: {avg_cold:.2f}')

        # Apply Savitzky-Golay filter to hot, cold and baseline power spectra to smooth them
        int_hot_pwr[scan,:] = savgol_filter(int_hot_pwr[scan,:], window_length=(args.fft_size//8), polyorder=3)
        int_cold_pwr[scan,:] = savgol_filter(int_cold_pwr[scan,:], window_length=(args.fft_size//8), polyorder=3)
        int_bsl_pwr[scan,:] = savgol_filter(int_bsl_pwr[scan,:], window_length=(args.fft_size//8), polyorder=3) if bsl_file is not None else np.ones(args.fft_size)

        # Plot the smoothed uncalibrated hot and cold power spectra
        cal[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_hot_pwr[scan,:], color='maroon', label=hot_smooth_label)
        cal[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_cold_pwr[scan,:], color='dodgerblue', label=cold_smooth_label)

        # Add labels and legend to the uncalibrated spectra plot
        cal[0].legend()  
         
        plt.draw()
        plt.pause(0.1)  # Pause to allow the plot to update

         # Calculate gain and tsys calibration solutions using smoothed hot and cold power spectra
        int_gain_cal[scan, ] = (int_hot_pwr[scan,] - int_cold_pwr[scan,]) / (args.temp_hot - args.temp_cold)  # Calculate the gain from hot and cold power spectra and temperatures
        int_tsys_cal[scan, ] = int_hot_pwr[scan,]*(args.temp_hot - args.temp_cold) / (int_hot_pwr[scan,] - int_cold_pwr[scan,]) - args.temp_hot  # Calculate tsys from hot and cold power spectra and temperatures
        
        avg_tsys = np.mean(int_tsys_cal[:, start_bin:end_bin])  # Calculate the average Tsys over the usable bandwidth

        tsys_start_mean = np.mean(int_tsys_cal[scan, start_bin:start_bin + delta])  # Calculate the mean Tsys at the start of the usable bandwidth
        tsys_end_mean = np.mean(int_tsys_cal[scan, end_bin - delta:end_bin])  # Calculate the mean Tsys at the end of the usable bandwidth

        # Populate the adjusted tsys numpy array with a straightline fit of the avg tsys calibration
        int_adj_tsys_cal[scan, ] = np.interp(np.arange(args.fft_size), [0,args.fft_size], [avg_tsys, avg_tsys])
        logger.info(f"Scan {scan}/{freq_scans}: Tsys Start: {10*np.log10(tsys_start_mean):.2f}dB, End: {10*np.log10(tsys_end_mean):.2f}dB, Center: {10*np.log10(np.mean(int_tsys_cal[scan, start_bin:end_bin])):.2f}dB")
        #int_adj_tsys_cal[scan, ] = int_tsys_cal[scan, ]

        # Smooth the tsys calibration using a maximal smoothing (10) Gaussian filter
        #int_adj_tsys_cal[scan, ] = gaussian_filter1d(int_tsys_cal[scan, ], sigma=10) 
        #int_tsys_cal[scan, :] = savgol_filter(int_tsys_cal[scan, :], window_length=51, polyorder=3)  # Smooth the tsys calibration using Savitzky-Golay filter

        # If a baseline load power spectrum file was not provided
        if bsl_file is None:
            pass
            # Smooth the gain calibration using a maximal smoothing (10) Gaussian filter
            #int_adj_gain_cal[scan, ] = gaussian_filter1d(int_gain_cal[scan, ], sigma=10)
            #int_gain_cal[scan, :] = savgol_filter(int_gain_cal[scan, :], window_length=51, polyorder=3)  # Smooth the gain calibration using Savitzky-Golay filter
        else:
            # Calculate the average gain and baseline load power spectrum over the usable bandwidth of the scan
            avg_gain = np.mean(int_gain_cal[scan,start_bin:end_bin])  # Calculate the average gain
            avg_bsl = np.mean(int_bsl_pwr[scan,start_bin:end_bin]) # Calculate the average baseline load
            logger.info(f"Scan {scan}/{freq_scans}: Gain Avg: {avg_gain:.2f}, BSL Avg: {avg_bsl:.2f} Factor: {avg_gain/avg_bsl:.2f}")

            # Adjust the baseline load spectra by a factor based on the ratio of average gain to average baseline load
            # We want to use the baseline load as the gain reference, so we align it with the gain calibration
            #int_adj_gain_cal[scan, ] = int_bsl_pwr[scan, ] * (avg_gain/avg_bsl) 
            int_adj_gain_cal[scan, ] = int_gain_cal[scan, ]

        # Remove the previous gain and tsys avg lines from the plot if they exist
        if gainline or tsysline:
            gainline.remove()
            tsysline.remove()

        # Calculate average gain and tsys over the entire usable bandwidth
        avg_gain = np.mean(int_gain_cal[:, start_bin:end_bin])
        

        gain_label=f"Gain" if f"Gain" not in [l.get_label() for l in cal[1].lines] else ""
        adj_gain_label=f"Adj Gain" if f"Adj Gain" not in [l.get_label() for l in cal[1].lines] else ""
        tsys_label=f"Tsys" if f"Tsys" not in [l.get_label() for l in cal[1].lines] else ""
        adj_tsys_label=f"Adj Tsys" if f"Adj Tsys" not in [l.get_label() for l in cal[1].lines] else ""


        cal[1].plot(np.linspace(extent[0], extent[1], args.fft_size), 10 * np.log10(np.where(int_gain_cal[scan, :] > 0, int_gain_cal[scan, :], np.finfo(float).eps)), color='purple', label=gain_label)
        cal[1].plot(np.linspace(extent[0], extent[1], args.fft_size), 10 * np.log10(np.where(int_adj_gain_cal[scan, :] > 0, int_adj_gain_cal[scan, :], np.finfo(float).eps)), color='black', label=adj_gain_label)
        cal[1].plot(np.linspace(extent[0], extent[1], args.fft_size), 10 * np.log10(np.where(int_tsys_cal[scan, :] > 0, int_tsys_cal[scan, :], np.finfo(float).eps)), color='orange', label=tsys_label)

        #cal[1].plot(np.linspace(extent[0], extent[1], delta), 10 * np.log10(np.where(int_tsys_cal[scan, start_bin:start_bin+delta] > 0, int_tsys_cal[scan, start_bin:start_bin+delta], np.finfo(float).eps)), color='green', label=tsys_label)
        #cal[1].plot(np.linspace(extent[0], extent[1], args.fft_size), 10 * np.log10(np.where(int_tsys_cal[scan, :] > 0, int_tsys_cal[scan, :], np.finfo(float).eps)), color='orange', label=tsys_label)
        #cal[1].plot(np.linspace(extent[0], extent[1], delta), 10 * np.log10(np.where(int_tsys_cal[scan, end_bin - delta:end_bin] > 0, int_tsys_cal[scan, end_bin - delta:end_bin], np.finfo(float).eps)), color='green', label=tsys_label)

        cal[1].plot(np.linspace(extent[0], extent[1], args.fft_size), 10 * np.log10(np.where(int_adj_tsys_cal[scan, :] > 0, int_adj_tsys_cal[scan, :], np.finfo(float).eps)), color='tomato', label=adj_tsys_label)

        # Add average lines for hot and cold power spectra
        gainline = cal[1].axhline(10 * np.log10(avg_gain), color='purple', linestyle='--', label=f'Avg Gain: {avg_gain:.2f} {10 * np.log10(avg_gain):.2f}dB')
        tsysline = cal[1].axhline(10 * np.log10(avg_tsys), color='orange', linestyle='--', label=f'Avg Tsys: {avg_tsys:.2f}K {10 * np.log10(avg_tsys):.2f}dB')

        # Add labels and legend to the calibration spectra plot
        cal[1].legend()  
        
        plt.draw()
        plt.pause(0.1)  # Pause to allow the plot to update

        # Calculate the calibrated hot and cold power spectra using the adjusted gain and tsys calibration data
        int_hot_cal[scan, :] = int_hot_pwr[scan, :]/int_adj_gain_cal[scan, :] - int_adj_tsys_cal[scan, :]
        int_cold_cal[scan, :] = int_cold_pwr[scan, :]/int_adj_gain_cal[scan, :] - int_adj_tsys_cal[scan, :]

        # Remove the previous calibrated hot, cold and baseline avg lines from the plot if they exist
        if calhotline or calcoldline:
            calhotline.remove()
            calcoldline.remove()
        
        if calbslline:
            calbslline.remove()

        # If a baseline load power spectrum file was provided
        if bsl_file is not None:
            # Calculate the calibrated baseline load power spectrum using the adjusted gain and tsys calibration data
            int_bsl_cal[scan, :] = int_bsl_pwr[scan, :]/int_adj_gain_cal[scan, :] - int_adj_tsys_cal[scan, :]
            bsl_label=f"BSL" if f"BSL" not in [l.get_label() for l in cal[2].lines] else ""
            # Calculate the average calibrated baseline load power spectrum over the usable bandwidth
            avg_calbsl = np.mean(int_bsl_cal[:, start_bin:end_bin])
            
            # Plot the calibrated baseline load power spectrum and the avg of the calibrated baseline load power spectrum
            cal[2].plot(np.linspace(extent[0], extent[1], end_bin-start_bin), int_bsl_cal[scan, start_bin:end_bin], color='black', label=bsl_label)
            calbslline = cal[2].axhline(avg_calbsl, color='black', linestyle='--', label=f'Avg BSL: {avg_calbsl:.2f}K')

        # Calculate average calibrated hot and cold spectra over the usable bandwidth
        avg_calhot = np.mean(int_hot_cal[:, start_bin:end_bin])
        avg_calcold = np.mean(int_cold_cal[:, start_bin:end_bin])

        hot_label=f"Hot" if f"Hot" not in [l.get_label() for l in cal[2].lines] else ""
        cold_label=f"Cold" if f"Cold" not in [l.get_label() for l in cal[2].lines] else ""

        # Plot the calibrated hot and cold power spectra
        cal[2].plot(np.linspace(extent[0], extent[1], end_bin-start_bin), int_hot_cal[scan, start_bin:end_bin], color='red', label=hot_label)
        cal[2].plot(np.linspace(extent[0], extent[1], end_bin-start_bin), int_cold_cal[scan, start_bin:end_bin], color='blue', label=cold_label)

        # Add average lines for calibrated hot and cold spectra
        calhotline = cal[2].axhline(avg_calhot, color='red', linestyle='--', label=f'Avg Hot: {avg_calhot:.2f}K')
        calcoldline = cal[2].axhline(avg_calcold, color='blue', linestyle='--', label=f'Avg Cold: {avg_calcold:.2f}K')

        cal[2].legend()
        
        plt.pause(0.1)  

        # Generate filenames for gain and tsys calibration CSV files
        gain_filename = dmd.gen_file_prefix(args, dt=cal_start, duration=args.duration, center_freq=scan_center_freq, freq_scan=scan, type='gain')
        tsys_filename = dmd.gen_file_prefix(args, dt=cal_start, duration=args.duration, center_freq=scan_center_freq, freq_scan=scan, type='tsys')

        # Save the gain and Tsys calibration CSV files
        save_calibration_csv(int_adj_gain_cal[scan, :], gain_filename)
        save_calibration_csv(int_adj_tsys_cal[scan, :], tsys_filename)

        scan_start_freq += args.sample_rate - freq_overlap

    # Save the calibration plots as PNG files
    filename = f"{dmd.gen_file_prefix(args, dt=cal_start, duration=args.duration, type='cal')}.png"
    plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=300)

    # Print the list of output files generated during the observation
    out_files = dmd.print_output_files(cal_start)

    plt.pause(0.1)  # Show all plots at the end of the script

    # Ask the user if they want to close the application and optionally delete output files
    logger.info("Close the application (press ENTER)\nClose the application and DELETE the output files (press dd)")
    confirm = input().strip().lower()
    if confirm in ['dd']:
        for f in out_files:
            os.remove(OUTPUT_DIR + "/" + f)
        logger.info(f"Deleted output files")

    plt.close('all')

if __name__ == "__main__":
    main()

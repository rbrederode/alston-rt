import argparse
import datetime
import io
import os
import shutil
import time
import json
import select
import sys
import subprocess
import logging

import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Ellipse
from math import cos, sin, radians

from polarplot import PolarPlot
from pecplot import PECPlot
from motion import Motion

from rtlsdr import RtlSdr, RtlSdrTcpClient

from scipy import fftpack
from scipy.ndimage import gaussian_filter1d
from scipy.stats import shapiro, normaltest, norm
import scipy.constants as const

from astropy import units as u
from astropy.coordinates import SpectralCoord, EarthLocation, SkyCoord, AltAz, ICRS
from astropy.wcs import WCS
from astropy.time import Time
from astropy.io import fits

from geopy.geocoders import Nominatim

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Or DEBUG for more verbosity
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),                    # Log to console
        logging.FileHandler("proto.log", mode="a")  # Log to a file
    ]
)
logger = logging.getLogger(__name__)

f_e = 1420.405751768 * u.MHz  # Rest frequency of HI hyperfine transition
speed_of_light = const.speed_of_light * (u.meter / u.second)

OUTPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Alston Observatory'  # Directory to store samples
#OUTPUT_DIR = '/Volumes/DATA SDD/Alston Radio Telescope/Samples/Home'  # Directory to store samples
#OUTPUT_DIR = '~/Samples'  # Directory to store samples
USABLE_BANDWIDTH = 0.65  # Percentage of usable bandwidth for a scan
FIG_SIZE = (14, 7)  # Default figure size for plots
MAX_RECONNECT_ATTEMPTS = 5  # Maximum number of attempts to reconnect to the SDR device

def velocity2LSR(coord: SkyCoord, observing_location: EarthLocation, observing_time: Time):
    """ Calculates the velocity adjustment for the Local Standard of Rest 
        coord:  Expected in the ICRS frame of reference i.e. RA DEC
        observing_location: EarthLocation of observer
        observing_time: Observing Time

        Validation by comparison to the Greenbank calculator here:
        https://www.gb.nrao.edu/cgi-bin/radvelcalc.py?UTDate=2025%2F07%2F16&UTTime=17%3A40%3A00&RA=06%3A41%3A00&DEC=09%3A52%3A59
        Lat Long for GMB Telescope: 38.432994106420274, -79.8400693370822
        """

    # ITRS (International Terrestial Reference System) frame of reference (a geocentric system)
    itrs = observing_location.get_itrs(obstime=observing_time)
    frequency = SpectralCoord(
        f_e, 
        observer=itrs, 
        target=coord) #Shift expected from just local motion
    # Return a new SpectralCoord with the velocity of the observer altered, but not the position
    f_shifted = frequency.with_observer_stationary_relative_to('lsrk') #correct for kinematic local standard of rest
    f_shifted = f_shifted.to(u.GHz)
    v = -freq2vel(f_shifted, f_e)
    v_adj = v.to(u.km/u.second)
    return v_adj

def freq2vel(freq, rest=f_e):
    """
    Calculates velocity from measured frequency via doppler shift.
    
    :param freq: array of frequency quantities (including units)
    :param rest (optional): Rest frequency, defaults to 1.42 GHz
    :returns vel: velocity inferred by doppler shift
    """
    return ((rest - freq) * speed_of_light / freq).to(u.km/u.second)

def velocities(args, int_mpr, int_bsl):

    velocities = []

    for pointing in range(ntrials):
        velocities.append([])
        for k in range(len(freqs[pointing])):
            velocities[pointing].append(freq2vel(freqs[pointing][k] * u.GHz))

def arg_to_location(s : str) -> tuple[EarthLocation, str]:
    """ Convert a string input to an EarthLocation object and an address.
        The input can be a comma-separated string of longitude and latitude or a location name.
        If the input is a location name, it uses geopy to find the coordinates.
    """
    try:
        if ',' in s:
            list = [item.strip() for item in s.split(',')]
            if len(list) < 2:
                raise argparse.ArgumentTypeError("Need at least two comma separated values: longitude,latitude")
            else:
                list[0] = float(list[0])  # Convert Longitude to float
                list[1] = float(list[1])  # Convert Latitude to float
            address = '' if len(list) <= 2 else str(" ".join(list[2:]))
        else:
            geolocator = Nominatim(user_agent="Alston Radio Telescope")
            location = geolocator.geocode(s)
            list = [location.longitude, location.latitude]
            address = location.address

        observing_location = EarthLocation.from_geodetic(lat=list[0], lon=list[1], height=100*u.m)  

        return (observing_location, address)
    except ValueError:
        raise argparse.ArgumentTypeError("Each value must be a floating point number")

def arg_to_SkyCoord(s: str) -> SkyCoord:
        #convert the input RA and Dec strings to an Astropy SkyCoord object
        try:
            rastr, decstr = s.split(',')
            # Explicitly set the frame to FK5 and equinox to the current time instead of the default ICRS (which is effectively J2000)
            coord = SkyCoord(rastr, decstr, unit=(u.hourangle, u.deg), frame='fk5', equinox=Time.now())
            return coord
        except:
            logger.error("Error parsing RA,Dec strings: {} {}".format(rastr,decstr))
            sys.exit()

def RADec_to_altaz(coord: SkyCoord, observing_location: EarthLocation, observing_time: Time) -> SkyCoord:
    """
    Convert RA and Dec to AltAz coordinates.
    
    :param coord: SkyCoord object with RA and Dec
    :param observing_location: EarthLocation object with observer's location
    :param observing_time: Time object with observation time
    :returns: AltAz coordinates as a SkyCoord object
    """
    aa = AltAz(location=observing_location, obstime=observing_time)
    return coord.transform_to(aa)

    #observing_location = EarthLocation(lat='52.2532', lon='351.63910339111703', height=100*u.m)  
    #observing_time = Time('2017-02-05 20:12:18')  
    #aa = AltAz(location=observing_location, obstime=observing_time)

    #coord = SkyCoord('4h42m', '-38d6m50.8s')
    #coord.transform_to(aa)

def init_args():
    """
    Initialize command line arguments for the SDR observation script.
    """
    parser = argparse.ArgumentParser(description='SDR Observation')
    parser.add_argument('-g', '--gain', type=str, help='SDR gain in dB between 0-49.6 or "auto"', default='auto')
    parser.add_argument('-l', '--load', action='store_true', help='Generate load file for calibration', default=False)
    parser.add_argument('-s', '--sample_rate', type=float, help='Sample rate in Hz e.g. 2.048e6 for 2.048 MHz, max sample rate is 3.2 MHz, however max rate that does not drop samples is 2.56 MHz https://www.rtl-sdr.com/about-rtl-sdr/', default=2.048e6)
    parser.add_argument('-c', '--center_freq', type=float, help='Center frequency in Hz e.g. 1420.4e6 for 1420.4 MHz, valid range is 25 - 1750 MHz ref: NESDR SMArt v5 RTL-SDR Software Defined Radio Data Sheet', default=1420.40e6)
    parser.add_argument('-d', '--duration', type=int, help='Observation duration (integration time)', default=10)
    parser.add_argument('-f', '--fft_size', type=int, help='Number of frequency bins (FFT size)', default=512)
    parser.add_argument('-p', '--ppm', type=int, help='Frequency correction in PPM', default=-140)
    parser.add_argument('-b', '--bandwidth', type=float, help='Bandwidth in MHz e.g. 2.4', default=2.4)
    parser.add_argument('-w', '--write', action='store_true', help='Save IQ samples to an *.iq file', default=False)
    parser.add_argument('-r', '--read', type=str, help='Date&time of IQ sample file to read (e.g. 2025-06-23T102208) or "latest"', default='none')
    parser.add_argument('-t', '--telescope', type=str, help='Telescope descriptor e.g. TableTop or Helical', default='')
    parser.add_argument('-o', '--observation', type=str, help='Observation descriptor e.g. Butterfly Nebula', default='')
    parser.add_argument('-loc', '--location', type=arg_to_location, dest='loc', help='Location of the observer', default='53.187052,-2.256079, Congleton') # Default is home
    parser.add_argument('-target', type=arg_to_SkyCoord, dest='tar', help="right ascension and declination of the target in the format ra0,dec0 e.g. 4h42m,-38d6m50.8s", default=None)
    parser.add_argument('-cal', '--calibrate', action='store_true', help='Generate hot / cold calibration spectra', default=False)
    parser.add_argument('-host', '--host', type=str, help='Hostname/IP address of an SDR TCP server', default=None)
    parser.add_argument('-port', '--port', type=int, help='Port of the SDR server', default=1234)
    parser.add_argument('-imu', '--imu', type=str, help='IMU device identifier e.g. "/dev/tty.usbserial-1120" Use "auto" to detect IMU devices', default=None)
    parser.add_argument('-baud', '--baud', type=int, help='Baud rate for the IMU device e.g. 9600', default=9600)

    return parser.parse_args()

def create_output_directory():
    """
    Create a directory to store the outputs of the observation if it doesn't exist.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)  # Create output directory

def gen_file_prefix(args, duration=None, center_freq=None, dt=datetime.datetime.now(), freq_scan=None, scan_iter=None, type='generic'):
    """
    Generate a unique prefix for output files based on current datetime and parameters.
    """
    
    center_freq = args.center_freq if center_freq is None else center_freq
    duration = args.duration if duration is None else duration

    scan_txt = '' if freq_scan is None else f"-scan {freq_scan}" if scan_iter is None else f"-scan {freq_scan}-{scan_iter}"

    # Generate a unique prefix for output files based on current datetime and parameters
    return dt.strftime("%Y-%m-%dT%H%M%S") + \
        (("-t" + str(args.telescope)) if args.telescope != '' else '') + \
        "-g" + str(args.gain) + \
        "-d" + str(duration) + \
        "-s" + str(round(args.sample_rate/1e6,2)) + \
        "-c" + str(round(center_freq/1e6,2)) + \
        "-fft" + str(args.fft_size) + \
        scan_txt + \
        ("-" + type if type is not None else '')

def init_scan_data_arrays(sample_rate, duration, fft_size):
    """
    Initialize data arrays for raw samples, power spectrum, summed power & baseline
    The data arrays are flushed every time a new scan is started.
        :param sample_rate: Sample rate in Hz
        :param duration: Duration of the scan in seconds
        :param fft_size: FFT size for the analysis
    """    

    # Calculate the number of rows in the spectrogram based on duration and sample rate
    num_rows = int(np.ceil(duration * sample_rate / fft_size))  # number of rows in the spectrogram

    # Initialise the global data arrays that hold data for a given scan of {duration} seconds
    global raw, pwr, mpr, bsl, abl
    
    raw = np.zeros((num_rows, fft_size), dtype=np.complex64) # complex64 for raw IQ samples i.e. 8 bytes per sample (4 bytes for real and 4 bytes for imaginary parts)
    pwr = np.zeros((num_rows, fft_size), dtype=np.float64) # float64 for power spectrum data
    mpr = np.zeros((duration, fft_size), dtype=np.float64) # float64 for summed pwr for each second in duration
    bsl = np.ones((fft_size,), dtype=np.float64) # float64 for baseline power spectrum over duration

def validate_args(args):
    """
    Validate command line arguments.
    """
    if args.gain not in ['auto'] and (not args.gain.isdigit() or not (0 <= int(args.gain) <= 49.6)):
        raise ValueError(f"Invalid gain value: {args.gain}. It should be an integer between 0 and 49.6 or 'auto'.")
    if args.sample_rate < 230000 or args.sample_rate > 3200000:
        raise ValueError(f"Invalid sample rate: {args.sample_rate}. It should be between 230 kHz and 3.2 MHz. ref: NESDR SMArt v5 RTL-SDR Software Defined Radio Data Sheet")
    if args.center_freq < 24e6 or args.center_freq > 1766e6:
        raise ValueError(f"Invalid center frequency: {args.center_freq}. It should be between 25 MHz and 1750 MHz. ref: NESDR SMArt v5 RTL-SDR Software Defined Radio Data Sheet")
    if args.duration <= 0:
        raise ValueError(f"Invalid duration: {args.duration}. It should be a positive integer.")
    if args.fft_size <= 0:
        raise ValueError(f"Invalid FFT size: {args.fft_size}. It should be a positive integer.")
    if args.ppm < -1000 or args.ppm > 1000:
        raise ValueError(f"Invalid PPM value: {args.ppm}. It should be between -1000 and 1000.")
    if args.bandwidth < 1 or args.bandwidth > 20:
        raise ValueError(f"Invalid bandwidth: {args.bandwidth}. It should be a positive value between 1 and 20 MHz.")
    if args.read != 'none' and args.write:
        raise ValueError("Cannot use both --read and --write options at the same time. Please choose one.")
    
    if args.load and args.calibrate:
        logger.error(f"Cannot use both --load and --calibrate options at the same time. Please choose one.")
        exit(1)
    
    if args.read != 'none' and (args.load or args.calibrate):
        logger.info(f"Load / calibration files will be generated. This will be based on previous raw IQ sample files.")
        confirm = input("Are you sure you want to proceed (yes/no): ").strip().lower()
        if confirm != 'yes' and confirm != 'y':
            logger.info("Calibration cancelled.")
            exit(0)

    # If we are generating a load file and not reading samples, ask the user to confirm before proceeding
    if args.load and args.read == 'none':
        # Make sure the LNA Assembly is terminated with a load resistor
        logger.info(f"A load file will be generated. Ensure that the LNA Assembly is terminated with a load resistor OR that the feed is completely isolated.")
        confirm = input("Are you ready to proceed (yes/no): ").strip().lower()
        if confirm != 'yes' and confirm != 'y':
            logger.info("Load generation cancelled.")
            exit(0)
    
    if args.loc is None or len(args.loc) < 2:
        raise ValueError("Observer location must be provided as a comma-separated string of longitude, latitude, and optionally an address.")
    else:
        logger.info(f"Observer Latitude and Longitude provided: {args.loc[0].lat} {args.loc[0].lon} and City: {args.loc[1]}")

    if args.tar is None:
        logger.error("No target RA and Dec provided. Cannot provide Az/Alt coordinates.")
    else:
        logger.info(f"Target RA and Dec provided: {args.tar.to_string('dms')}")
        logger.info(f"Target RA and Dec provided: {args.tar.to_string('hmsdms')}")
        logger.info(f"Target Galactic coordinates: {args.tar.galactic.to_string('decimal')}")

        now = Time(Time.now(), scale='utc', location=args.loc[0])  # Get the current time in UTC at the observer's location
        logger.info(f"Target Az Alt coordinates (now): {RADec_to_altaz(args.tar, args.loc[0], now).to_string('dms')}")
        logger.info(f"Apparant Sidereal Time (now): {now.sidereal_time('apparent')}")

        #now = Time(Time.now(), scale='utc', location=args.loc[0])  # Get the current time in UTC at the observer's location
        #print(f"Target Az Alt coordinates: {RADec_to_altaz(args.tar, args.loc[0], now).to_string('dms')} Apparant Sidereal Time {now.sidereal_time('apparent')} ",end='\r')    

def load_baseline(prefix, scan=None):
    """
    Load the baseline power spectrum from a CSV file in the output directory.
    If the file does not exist, prompt the user to generate it first.
    """

    # Look for files in the output directory ending with 'load.csv' that match the prefix i.e. same parameters (center_freq, duration etc)
    load_files = [f for f in os.listdir(OUTPUT_DIR) if prefix[17:] in f and f.endswith('load.csv')]
    
    if load_files: # If we found matching load files
        load_file = sorted(load_files)[-1] # Identify the most recent load file and use that one
        logger.info(f"Loading baseline power spectrum {load_file}")

        global bsl, int_bsl

        bsl = np.genfromtxt(OUTPUT_DIR + "/" + load_file, delimiter=",")

        #mean_bsl = np.mean(bsl)  # Calculate the mean baseline power spectrum
        #bsl = bsl* 4.0 / mean_bsl  # Normalize the baseline power spectrum to the mean value

        int_bsl[scan,] = bsl  # Record the baseline power spectrum for the given scan
        return load_file
    else:
        logger.warning(f"No load file with parameters {prefix[17:]} found in {OUTPUT_DIR}. Please generate a load file first using the -l option.")
        
    return None

def load_tsys_calibration(prefix, scan=None):
    """ Load the Tsys calibration from a CSV file in the output directory.
    If the file does not exist, prompt the user to generate it first.
    """
    global int_tsys_cal

    # Look for files in the output directory ending with 'tsys.csv' that match the prefix i.e. same parameters (center_freq, duration etc)
    tsys_files = [f for f in os.listdir(OUTPUT_DIR) if prefix[17:] in f and f.endswith('tsys.csv')]

    if tsys_files:  # If we found matching Tsys files
        tsys_file = sorted(tsys_files)[-1]  # Identify the most recent Tsys file and use that one
        logger.info(f"Loading Tsys calibration {tsys_file}")

        int_tsys_cal[scan, :] = np.genfromtxt(OUTPUT_DIR + "/" + tsys_file, delimiter=",")
        return tsys_file
    else:
        logger.warning(f"No Tsys calibration file with parameters {prefix[17:]} found in {OUTPUT_DIR}. Please generate a Tsys calibration file first.")
    
    return None

def load_gain_calibration(prefix, scan=None):
    """
    Load the gain calibration from a CSV file in the output directory.
    If the file does not exist, prompt the user to generate it first.
    """
    global int_gain_cal

    # Look for files in the output directory ending with 'gain.csv' that match the prefix i.e. same parameters (center_freq, duration etc)
    gain_files = [f for f in os.listdir(OUTPUT_DIR) if prefix[17:] in f and f.endswith('gain.csv')]

    if gain_files:  # If we found matching gain files
        gain_file = sorted(gain_files)[-1]  # Identify the most recent gain file and use that one
        logger.info(f"Loading gain calibration {gain_file}")

        int_gain_cal[scan, :] = np.genfromtxt(OUTPUT_DIR + "/" + gain_file, delimiter=",")
        return gain_file
    else:
        logger.warning(f"No gain calibration file with parameters {prefix[17:]} found in {OUTPUT_DIR}. Please generate a gain calibration file first.")
        
    return None

def mpr_stats(args, mpr):

    """ Potentially useful statistics for the summed power spectrum (mpr) """

    smooth = gaussian_filter1d(int_mpr[scan], 3)
    smooth_dx = np.gradient(smooth)  # Calculate the first derivative of the smoothed signal
    smooth_2dx = np.gradient(smooth_dx)  # Calculate the second derivative of the smoothed signal
    smooth_3dx = np.gradient(smooth_2dx)  # Calculate the second derivative of the smoothed signal

    mean_dx = np.abs(np.mean(smooth_dx))  # Maximum value of the first derivative
    logger.info(f"Mean of the abs of first derivative: {mean_dx:.2f}")

    # find inflexion points
    infls = np.where(np.diff(np.sign(smooth_dx)))[0]  # Find indices where the first derivative changes sign
    infls = infls * (extent[1] - extent[0]) / args.fft_size + extent[0]

    sig[0].plot(np.linspace(extent[0], extent[1], args.fft_size), smooth, color='orange', label='Smoothed Signal')

    for i, infl in enumerate(infls, 1):
        sig[0].axvline(x=infl, color='k', label=f'Inflection Point {i}')
        logger.info(f"Inflection Point {i}: Frequency = {infl:.2f} MHz % of Bandwidth = {(infl - extent[0])/(extent[1] - extent[0])*100:.2f}%")

    sig[0].plot(np.linspace(extent[0], extent[1], args.fft_size), np.max(smooth)*(smooth_dx)/(np.max(smooth_dx)-np.min(smooth_dx)), color='green', label='1st Derivative of Smoothed Signal')
    sig[0].plot(np.linspace(extent[0], extent[1], args.fft_size), np.max(smooth)*(smooth_2dx)/(np.max(smooth_2dx)-np.min(smooth_2dx)), color='red', label='2nd Derivative of Smoothed Signal')


def read_scan(args, scan_center_freq, extent, duration, fft_size, freq_scan=None, scan_iter=None):
    """
    Read raw IQ samples corresponding to a scan from a file matching the specified args parameters.
    Process the samples to compute the power spectrum.
    """
    global raw, pwr, mpr, bsl

    if args.read.lower() == 'none':
        return
    elif args.read.lower() == 'latest':
        prefix = gen_file_prefix(args, duration=duration, center_freq=scan_center_freq, freq_scan=freq_scan, scan_iter=scan_iter,type='raw')[17:]  # Generate file prefix based on args and scan center frequency
        logger.info(f"Looking for the latest iq file with parameters {prefix} in {OUTPUT_DIR}")
        read_files = [f for f in os.listdir(OUTPUT_DIR) if prefix in f and f.endswith('raw.iq')]
    else:
        prefix = args.read + gen_file_prefix(args, duration=duration, center_freq=scan_center_freq, freq_scan=freq_scan, scan_iter=scan_iter,type='raw')[17:]
        logger.info(f"Looking for iq file with parameters {prefix} in {OUTPUT_DIR}")
        read_files = [f for f in os.listdir(OUTPUT_DIR) if prefix in f and f.endswith('raw.iq')]

    # If we found matching files
    if read_files:  
        read_file = sorted(read_files)[-1]  # Identify the most recent file and use that one
        logger.info(f"Reading IQ samples from {read_file}")

        # Load the IQ samples from the file
        raw = np.fromfile(OUTPUT_DIR+"/"+read_file, dtype=np.complex64)  # Read the IQ samples as complex64
        raw = raw.reshape(-1, args.fft_size)  # Reshape to have rows each of size fft_size columns
        logger.info(f"Loaded {raw.shape[0]} rows of samples with {raw.shape[1]} columns each.")
    else:
        logger.warning(f"No IQ file matching {prefix}.iq found in {OUTPUT_DIR}. Please write a scan first using the -w option.")
        exit(1)
    
    num_rows = raw.shape[0]
    for row in range(num_rows):
        pwr[row,:] = np.abs(np.fft.fftshift(np.fft.fft(raw[row,:])))**2 # The power spectrum is the absolute value of the signal squared

    for sec in range(duration):
        row_start = sec * (num_rows // duration)
        row_end = (sec + 1) * (num_rows // duration) if sec < duration - 1 else num_rows  # Ensure we cover all rows

        # Calculate the sum of the power spectrum for each frequency bin in a given second
        mpr[sec,:] = np.sum(pwr[row_start:row_end,:], axis=0)  # Sum the power spectrum in a given sec for each frequency bin (in columns)
        remove_dc_spike(args.fft_size, mpr[sec,:])

        signal_displays_update(raw, mpr, bsl, sec, duration, row_start, row_end, extent, fft_size)

    return read_file

def init_waterfall_axes(axes, extent):
    """ Initialise waterfall plot axes """

    # Set up the waterfall plot
    axes.set_title("Waterfall Plot of Spectrum")
    axes.set_xlabel("Frequency [MHz]")
    axes.set_ylabel("Time [sec]")
    axes.set_aspect('auto')
    axes.set_facecolor('black')
    axes.grid(False)

def init_pwr_spectrum_axes(axes, title, extent, units='[a.u.]'):
    """ Initialise pwr spectrum plot axes """

    # Set up the summed power spectrum plot
    axes.set_title(title) # Summed Power Spectrum / Sky Signal
    axes.set_xlabel("Frequency [MHz]")
    axes.set_ylabel("Power Spectrum"+ f" {units}")
    axes.set_xlim(extent[0], extent[1])
    #axes.set_ylim(0, fft_size)
    axes.grid(True)
    
def init_total_power_axes(axes, duration):
    """ Initialise total power timeline plot axes """

    # Set up the total power timeline plot
    axes.set_title("Total Power Timeline")
    axes.set_xlabel("Time [sec]")
    axes.set_ylabel("Total Power [a.u.]")
    #axes.set_aspect('auto')
    axes.set_facecolor('white')
    axes.set_xlim(1, duration)
    axes.grid(True)

def init_saturation_axes(axes):
    """ Initialise SDR saturation levels plot axes """

    # Set up the SDR saturation levels plot
    axes.set_title("SDR Saturation Level")
    axes.set_xlabel("Mean(I), Mean(Q)")
    axes.set_ylabel("Saturation [%]")
    axes.set_ylim(0, 100)
    axes.set_facecolor('white')
    axes.grid(True)

def plot_polarplot(args, fig, axes):
    """ Setup a polar plot for an observation of the sky.
        Add the Sun, Moon and primary Target to the plot 
        Plot the polar plot on the provided figure and axes."""

    # Initialise a polar plot using the observer location, and current local time
    polar = PolarPlot(args.loc[0], datetime.datetime.now().astimezone())

    # Initialise the polar plot to track the observation (Sun, Moon, Target)
    polar.add_sun()
    polar.add_moon()
    if not args.tar is None:
        target = args.observation if args.observation != '' else 'Target'
        polar.add_target(args.tar, target)

    polar.plot(fig, axes)

    return polar

def convert_polar_to_cartesian(theta_deg, radius=1.0):
    """ Convert polar coordinates to cartesian coordinates for plotting
        :param theta_deg: Angle in degrees
        :param radius: Radius of the circle, default is 1.0
        :returns: x, y coordinates in cartesian system

        Assume theta_deg zero is at the top (North) and increases counter clockwise.
    """
    theta_deg = theta_deg + 90  # Convert to 0 degrees at the top (north), increasing counter clockwise
    theta_deg = theta_deg % 360  # Normalize angle to [0, 360) degrees
    
    # Convert polar coordinates to cartesian coordinates
    x = radius * np.cos(np.radians(theta_deg))
    y = radius * np.sin(np.radians(theta_deg))

    return x, y

def plot_galactic_direction(args, fig, axes):
    """ Plot the galactic direction of the target on a galactic coordinate system.
        :param args: Command line arguments containing target coordinates
        :param fig: Matplotlib figure object
        :param axes: Matplotlib axes object to plot on
    """

    # By NASA/JPL-Caltech/ESO/R. Hurt - http://www.eso.org/public/images/eso1339e/, Public Domain, https://commons.wikimedia.org/w/index.php?curid=28274906
    img = plt.imread("./Milky Way.jpg")
    
    axes.set_title("Target Line of Sight")
    axes.grid(False)
    
    axes.set_xlim(-1, 1)  # Galactic longitude from 0 to 360 degrees
    axes.set_ylim(-0.45, 1.0)  # Galactic latitude from -90 to 90 degrees
    axes.imshow(img, extent=[-1, 1, -0.45, 1.0], aspect='auto')  # Set the extent to cover the galactic coordinates

    axes.plot([0, 0], [0, 0], 'o', markersize=8, color = 'orange',label='Sun')  # Sun
    axes.plot([0], [0.275], 'o', markersize=8, color = 'blue', label='Milky Way Centre')  # Galactic Center

    if args.tar is not None:
        
        logger.info(f"Target Galactic coordinates: Longitude {args.tar.galactic.l.value} Latitude {args.tar.galactic.b.value}")

        # Adjust the radius based on the target's galactic coordinates
        radius = 0.4 if args.tar.galactic.l.value > 90 and args.tar.galactic.l.value < 180 else 0.9
        (x, y) = convert_polar_to_cartesian(args.tar.galactic.l.value, radius=radius)  # Convert galactic coordinates to cartesian coordinates for plotting

        # Draw a line from the target to the Sun
        axes.annotate(
            f'Target $l$ = {round(args.tar.galactic.l.value,3)}°',  
            xy=(0, 0), xytext=(x, y),
            fontsize=8, fontname='DejaVu Sans Mono',
            va='bottom', ha='center',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.7),
            zorder=20,
            arrowprops=dict(arrowstyle='->', color='red', linewidth=3, linestyle='--')
        )
        
    axes.legend(loc='upper right')
    axes.axis('off') # Hide the axes

def download_survey_fits(args, survey='hi4pi', size=30):
    """ Download a FITs file for the specified survey and target coordinates.
        :param args: Command line arguments containing target coordinates
        :param survey: Name of the survey to download, default is 'hi4pi'
        :param size: Size of the survey in degrees, default is 25 degrees
    """

    global OUTPUT_DIR

    # Check if a target is provided
    if args.tar is None:
        logger.warning("No target coordinates provided. Cannot download survey FITs file.")
        return None
   
    # Check if java is installed
    if not shutil.which("java"):
        logger.warning("Java is not installed. Please install Java to download the survey FITs file.")
        return None 

    try:
        # Check if skyview.jar is available
        skyview_path = os.path.expanduser("~/skyview.jar")
        if not os.path.exists(skyview_path):
            logger.info("skyview.jar is not found. Downloading it from https://skyview.gsfc.nasa.gov/jar/skyview.jar")
            # Use curl to download skyview.jar
            os.system(f"curl -L -o {skyview_path} https://skyview.gsfc.nasa.gov/jar/skyview.jar")
        
            if not os.path.exists(skyview_path):
                logger.warning("Failed to download skyview.jar. Please download it manually from https://skyview.gsfc.nasa.gov/jar/skyview.jar and place it in your home directory.")
                return None

        # Execute command line executable to download the FITs file
        os.system(f"java -jar {skyview_path} position='{args.tar.ra.deg},{args.tar.dec.deg}' size={size} survey={survey} plotcolor=RED grid=G gridlabels pixels=1000 draw='reset,scale 1d,color red,circle 0 0 25' output='{OUTPUT_DIR}/survey.fits'")
        logger.info(f"Downloaded survey FITs file for target {args.tar.to_string('hmsdms')} to {OUTPUT_DIR}/survey.fits")
    except Exception as e:
        logger.exception(f"Error downloading survey FITs file: {e}")
        return None

    # Return the path to the FITs file
    return f"{OUTPUT_DIR}/survey.fits"

def plot_survey_fits(args, fig, axes, survey='hi4pi', fov=25, fits_file=f"'{OUTPUT_DIR}/survey.fits'"):
    """ Plot the FITs file for the specified survey and target coordinates.
        :param args: Command line arguments containing target coordinates
        :param fig: Matplotlib figure object
        :param axes: Matplotlib axes object to plot on
        :param survey: Name of the survey to plot, default is 'hi4pi'
        :param fov: Field of view in degrees, default is 25 degrees
        :param fits_file: Path to the FITs file to plot
    """
    # Check if the FITs file exists
    if fits_file is None or not os.path.exists(fits_file):
        logger.warning(f"FITs file {fits_file} does not exist. Please download it first.")

        axes.set_title(f"Survey FITs file not found")
        axes.grid(True)
        axes.set_xlabel("RA")
        axes.set_ylabel("Dec")
        axes.annotate(
            f'Survey FITs file not found for target',  
            xy=(0.5, 0.5), xytext=(0.5, 0.5),
            fontsize=8, fontname='DejaVu Sans Mono',
            va='bottom', ha='center',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.7),
            zorder=20)

        return None

    # Open the FITs file and plot it
    with fits.open(fits_file) as hdul:
        data = hdul[0].data  # Get the data from the first HDU
        width = hdul[0].header['NAXIS1']  # Number of columns (X dimension)
        height = hdul[0].header['NAXIS2'] # Number of rows (Y dimension)
        x_scale = hdul[0].header['CDELT1']  # Pixel scale in degrees
        y_scale = hdul[0].header['CDELT2']  # Pixel scale in degrees
        logger.info(f"Image dimensions: width={width}, height={height}")

    axes.imshow(data, cmap='plasma', origin='lower')
    fig.colorbar(axes.images[0], ax=axes, label='Brightness Temperature (K)')
    axes.set_title(f"{survey.upper()} Survey Data for Target")
    axes.set_xlabel("RA")
    axes.set_ylabel("Dec")
    axes.grid(True)

    # Calculate the width and height of a FOV oval/circle in pixels
    width_pix = fov / abs(x_scale)   # x_scale: degrees / pixels/degree (may be negative)
    height_pix = fov / abs(y_scale)  # y_scale: degrees / pixels/degree (may be negative)

    # Center of the image in pixels
    center_x = width / 2
    center_y = height / 2

    # Create and add the ellipse
    ellipse = Ellipse(
        (center_x, center_y),
        width=width_pix,
        height=height_pix,
        edgecolor='red',
        facecolor='none',
        linestyle='--',
        linewidth=1.5, 
        label='FOV'  # Label for the FOV
    )
    axes.add_patch(ellipse)
    axes.legend(loc='upper right')

def init_pec_analysis(args, fig, axes, motion=None):
    """ Initialize the PEC analysis plot """

    pec = PECPlot(args.loc[0], args.tar, motion=motion)

    pec.start_recording()
    pec.plot(figure=fig, axes=axes)

    return pec

def init_cam_displays(args, fits_file=None, motion=None):
    """ Initialize the displays for control and monitoring functions.
    This includes a polar plot for observing the sky, a galactic direction plot, and a survey FITs image.
        :param args: Command line arguments containing target coordinates and other parameters
        :param fits_file: Path to the FITs file to plot, default is None
        :param motion: Motion sensor object for PEC analysis, default is None
    """

    global OUTPUT_DIR, FIG_SIZE

     # Initialize the grid space for the control & monitoring displays
    gs0 = GridSpec(2, 1, width_ratios=[1], height_ratios=[1, 1], left=0.07, right=0.43, top=0.93, bottom=0.07, wspace=0.2) # control and monitoring displays
    gs1 = GridSpec(2, 1, width_ratios=[1], height_ratios=[1, 0.2], left=0.53, right=0.93, top=0.93, bottom=0.07, wspace=0.2) # control and monitoring displays

    hdu_list = fits.open(fits_file) if fits_file else None  # Open the survey FITs file if it exists
    wcs = WCS(hdu_list[0].header) if hdu_list is not None else None  # Get the World Coordinate System information from the FITs header

    # Create Figure and Axes for the Polar Plot and show the target tracks 
    cam_fig = plt.figure(num=200, figsize=FIG_SIZE)
    cam = [None] * 4  # Initialize axes for the subplots

    cam[0] = plt.subplot(gs0[0], polar=True)  # Polar plot for observing the sky
    cam[1] = plt.subplot(gs1[0]) # Image of Milky Way
    cam[2] = plt.subplot(gs0[1], projection=wcs)  # Survey FITs image
    cam[3] = plt.subplot(gs1[1]) # PEC Analysis

    polar = plot_polarplot(args, cam_fig, cam[0])  # Initialize the polar plot for observing the sky
    plot_galactic_direction(args, cam_fig, cam[1])  # Initialize the galactic plot
    plot_survey_fits(args, cam_fig, cam[2], survey='hi4pi', fits_file=fits_file)  # Initialize the survey FITs plot
    pec = init_pec_analysis(args, cam_fig, cam[3], motion=motion)

    return cam_fig, cam, polar, pec

def init_metadata(args, scans_meta):

     # Initialise metadata structure to store information about the SDR observation
    metadata = {}

    # Capture metadata about SDR device config
    metadata["sdr"] = {
        "sample_rate": args.sample_rate,
        "gain": args.gain,
        "bandwidth": args.bandwidth,
        "ppm": args.ppm
    }

    # Capture metadata about the observation
    metadata["observation"] = {
        "telescope": args.telescope,
        "location": {
            "latitude": str(args.loc[0].lat),
            "longitude": str(args.loc[0].lon),
            "address": args.loc[1]
        },
        "target": {
            "ra": str(args.tar.ra.to_string(unit=u.hour, sep=':', pad=True) if args.tar is not None else "None"),
            "dec": str(args.tar.dec.to_string(unit=u.deg, sep=':', pad=True) if args.tar is not None else "None"),
            "galactic": str(args.tar.galactic.to_string(unit=u.deg, pad=True) if args.tar is not None else "None"),
        },
        "descriptor": args.observation,
        "fft_size": str(args.fft_size),
        "load_file": str(args.load),
        "write_file": str(args.write),
        "read_file": str(args.read),
        "calibrate": str(args.calibrate),
        "scans": scans_meta['scans']
        }

    return metadata

def save_metadata(args, dt, metadata):

     # Save metadata to a JSON file
    filename = f"{gen_file_prefix(args,dt=dt,type='metadata')}.json"
    with open(f"{OUTPUT_DIR}/{filename}", 'w') as f:
        json.dump(metadata, f, indent=4)
    logger.info(f"Metadata saved to {filename}.")

def save_integrated_mpr_spectrum(args, dt, duration, freq_scans, start_freq, freq_overlap, int_mpr):
    """
    Save the integrated power spectrum to CSV files, one for each scan
    """
    
    global OUTPUT_DIR

    # Calculate the end frequency based on the start frequency, sample rate, and number of scans
    ssf = start_freq

    # For each scan covering the required bandwidth
    for scan in range(freq_scans):

        scf = (ssf + args.sample_rate/2)
        # Generate a load or signal filename based on the command line arguments
        if args.load:
            filename = f"{gen_file_prefix(args, duration=duration, center_freq=scf, dt=dt, freq_scan=scan, scan_iter=None, type='load')}.csv"
            np.savetxt(f"{OUTPUT_DIR}/{filename}", int_mpr[scan], delimiter=",", fmt="%s")
        elif args.calibrate:
            filename = f"{gen_file_prefix(args, duration=duration, center_freq=scf, dt=dt, freq_scan=scan, scan_iter=None, type='cal')}.csv"
            np.savetxt(f"{OUTPUT_DIR}/{filename}", int_mpr[scan], delimiter=",", fmt="%s")
        else:
            filename = f"{gen_file_prefix(args, duration=duration, center_freq=scf, dt=dt, freq_scan=scan, scan_iter=None, type='mpr')}.csv"
            np.savetxt(f"{OUTPUT_DIR}/{filename}", int_mpr[scan], delimiter=",", fmt="%s")

        ssf += args.sample_rate - freq_overlap

    logger.info(f"Integrated Power spectrum saved")

def save_total_pwr_timeline(args, dt, scans, int_tpw):
    """
    Save the integrated total power per second to CSV file
    """
    
    global OUTPUT_DIR
    
    # Save the total power spectrum to a CSV file
    filename = f"{gen_file_prefix(args, dt=dt,type='tpwr')}.csv"
    np.savetxt(f"{OUTPUT_DIR}/{filename}", int_tpw, delimiter=",", fmt="%s")

    logger.info(f"Integrated Total Power saved")

def print_output_files(dt):
    """ Print a list of output files that match the observation start time. """
    
    global OUTPUT_DIR

    # Find all output files that match the observation start date & time
    out_files = [f for f in os.listdir(OUTPUT_DIR) if dt.strftime("%Y-%m-%dT%H%M%S") in f]
    
    if out_files: # If we found matching output files
        logger.info(f'Generated output files...')
        for f in sorted(out_files):
            logger.info(f" - {f}")
    else:
        logger.warning(f'No output files generated since {dt.strftime("%Y-%m-%dT%H:%M:%S")}.')
        logger.warning(f'Please check the output directory {OUTPUT_DIR}.')

    return out_files

def signal_displays_init(args, scan, iter, duration,scan_center_freq, extent):
    """
    Initialize the signal displays for the summed power spectrum, sky signal, waterfall plot and total power timeline.
    This includes setting up the axes and labels for each plot.
    """

    gs0 = GridSpec(1, 3, width_ratios=[1, 1, 1], left=0.07, right=0.93, top=0.93, bottom=0.3, wspace=0.2) # signal displays
    gs1 = GridSpec(1, 2, width_ratios=[0.32, 0.68], height_ratios=[1], left=0.07, right=0.93, top=0.22, bottom=0.07, wspace=0.2) # total power timeline display

    # Initialize the figure and axes for the signal displays
    plt.close(scan)  # Close any existing figure with the same scan number
    fig = plt.figure(num=scan, figsize=FIG_SIZE)
    sig = [None] * 5  # Initialize axes for the subplots

    sig[0] = fig.add_subplot(gs0[0]) # Power spectrum summed per second
    sig[1] = fig.add_subplot(gs0[1]) # Sky signal per second
    sig[2] = fig.add_subplot(gs0[2]) # Waterfall plot
    sig[3] = fig.add_subplot(gs1[0]) # SDR Saturation Levels
    sig[4] = fig.add_subplot(gs1[1]) # Total power timeline 

    fig.suptitle(f"Scan: {scan}-{iter} Center Frequency: {scan_center_freq/1e6:.2f} MHz, Gain: {args.gain} dB, Sample Rate: {args.sample_rate/1e6:.2f} MHz, FFT Size: {args.fft_size}", fontsize=14)

    # Initialize the axes for the signal displays
    init_pwr_spectrum_axes(sig[0], "Power/Sec", extent, units="{np.abs(shift fft(signal))**2}")  # Initialize the summed power spectrum axes
    init_pwr_spectrum_axes(sig[1], "Sky/Sec", extent)  # Initialize the sky signal axes
    init_waterfall_axes(sig[2], extent)  # Initialize the waterfall plot axes
    init_saturation_axes(sig[3])  # Initialize the SDR saturation levels axes
    init_total_power_axes(sig[4], duration)  # Initialize the total power timeline

    return fig, sig

def signal_displays_update(raw, mpr, bsl, sec, duration, row_start, row_end, extent, fft_size):
    """
    Update the waterfall plot, summed power spectrum, sky signal and total power displays.
    """
    global sig_axes, sig_fig, pwr_im, int_gain_cal
        
    # If this is the first second, initialize the signal displays
    if sec == 0:
        pwr_im = sig_axes[2].imshow(mpr / bsl, aspect='auto', extent=extent)
        sig_fig.colorbar(pwr_im, ax=sig_axes[2], label='Power Spectrum [a.u.]')
        sig_axes[4].plot([1], [np.sum(mpr[0, :])], color='red', label='Total Power')  # Plot total power across all fft bins for the first second
    else:
        # Update the existing power spectrum image with new data
        pwr_im.set_data(mpr / bsl)
        
        # Clear the previous plots (all except the waterfall plot)
        sig_axes[0].cla() 
        sig_axes[1].cla()
        sig_axes[3].cla()  
        sig_axes[4].cla()

        # Plot total power across all fft bins for each second up to the current second
        sig_axes[4].plot(
            np.arange(1, sec + 2),
            [np.sum(mpr[s, :]) for s in range(sec + 1)],  
            color='red',
            label='Total Power (TPW)') 

    label = 'Load (BSL)' if np.mean(int_gain_cal) == 1.0 else 'Gain (BSL)'

    # Plot the summed power spectrum and sky signal for the current second
    sig_axes[0].plot(np.linspace(extent[0], extent[1], fft_size), mpr.flatten()[sec*fft_size:(sec+1)*fft_size], color='red', label='Signal (MPR)')
    sig_axes[0].plot(np.linspace(extent[0], extent[1], fft_size), bsl, color='black', label=label)
    sig_axes[0].legend(loc='lower right')

    sig_axes[1].plot(np.linspace(extent[0], extent[1], fft_size), mpr.flatten()[sec*fft_size:(sec+1)*fft_size] / bsl, color='orange', label='Signal (MPR/BSL)')
    sig_axes[1].legend(loc='lower right')

    # Plot 10% of raw IQ samples for the current second
    indices = np.linspace(row_start, row_end - 1, int(raw.shape[0]*0.01), dtype=int)

    mean_real = np.mean(np.abs(raw[row_start:row_end, ].real))*100  # Find the mean real value in the raw samples (I)
    mean_imag = np.mean(np.abs(raw[row_start:row_end, ].imag))*100  # Find the mean imaginary value in the raw samples (Q)

    sig_axes[3].bar(0, mean_real, color='blue', label='I')
    sig_axes[3].bar(1, mean_imag, color='orange', label='Q')
    # Draw a line at the 33% and 66% marks
    sig_axes[3].axhline(y=33, color='green', linestyle='--', label='33%')
    sig_axes[3].axhline(y=66, color='red', linestyle='--', label='66%')
    sig_axes[3].legend(loc='lower right')

    sig_axes[4].legend(loc='lower right')

    init_pwr_spectrum_axes(sig_axes[0], "Power/Sec (MPR,BSL)", extent, units="{np.abs(shift fft(signal))**2}")  # Initialize the summed power spectrum axes
    init_pwr_spectrum_axes(sig_axes[1], "Power/Sec (MPR/BSL)", extent)  # Initialize the sky signal axes
    init_waterfall_axes(sig_axes[2], extent)  # Initialize the waterfall plot axes
    init_saturation_axes(sig_axes[3])  # Initialize the saturation axes
    init_total_power_axes(sig_axes[4], duration)  # Initialize the total power timeline axes

    # Show the signal displays to the user
    plt.draw()
    plt.pause(0.0001)

def plot_integrated_sky_signal(args, dt, freq_scans, start_freq, freq_overlap, int_mpr, int_bsl, colours, adjusted=False):
    """ Plot the sky signal for all scans. 
        If adjusted is True, only the usable bandwidth in each scan is plotted.
        Furthermore, the scans are adjusted to 'line up' with each other
        This is achieved by calculating the difference between the mean of the last 5% of the previous scan
        and the mean of the first 5% of the current scan...and applying that difference to the current scan.
    """

    global OUTPUT_DIR, USABLE_BANDWIDTH, FIG_SIZE
    global int_tsys_cal, int_gain_cal

    plt.close(100+int(adjusted))  # Close any existing figure with the same number
    fig = plt.figure(num=100+int(adjusted),figsize=FIG_SIZE)
    axes = fig.add_subplot(1, 1, 1)

    # If a calibrator is missing, then we cannot calculate brightness temperature
    if np.mean(int_gain_cal) == 1.0 or np.mean(int_tsys_cal) == 0.0:
        units = "Power (MPR/BSL) [a.u]" # y axis will be in arbitrary units
        temp = int_mpr / int_bsl
    else:
        units = "Brightness Temperature [K]" # y axis is in kelvin
        temp = int_mpr  / int_gain_cal - int_tsys_cal # calculates sky brightness temperature: T(obj) = P(measured) / Gain - Tsys

    label = 'Load' if args.load else 'Sky'

    if adjusted:

        # Calculate the start and end FFT bins based on the usable bandwidth and FFT size
        # We only want to plot the usable bandwidth of the spectrum
        start_bin = int((1-USABLE_BANDWIDTH)/2 * args.fft_size)
        end_bin = int((USABLE_BANDWIDTH + (1-USABLE_BANDWIDTH)/2) * args.fft_size)
        
        # Calculate the number of bins that represent 5% of the usable bandwidth
        delta = int(0.05 * (end_bin - start_bin))
        # Initialize start and end mean values
        end_mean = start_mean = 0.0  
        
        # Calculate the velocity adjustement needed to convert to the Local Standard of Rest
        v_adjustment = velocity2LSR(coord=args.tar, observing_location=args.loc[0], observing_time=Time(dt)) if adjusted and args.tar else 0.0 * u.km / u.second

        # Calculate the middle scan of the number of scans
        mode = freq_scans // 2 if freq_scans > 1 else 0
        order = []

        # Start in the middle scan and plot the adjusted sky signal towards the end of the usable bandwidth
        for i in range(mode, freq_scans):
            freq = [start_freq/1e6 + i * (args.sample_rate/1e6 - freq_overlap/1e6) + args.sample_rate/1e6/args.fft_size * j for j in range(args.fft_size)]
        
            velocities = freq2vel(freq * u.MHz, rest=f_e) - v_adjustment # Calculate LSR (adjusted) velocity from frequency
            
            start_mean = np.mean(temp[i,][start_bin:start_bin+delta])  # Mean of the first 5% of the usable bandwidth
            # If this is the first scan (end_mean will be zero)
            if end_mean == 0.0:
                diff = 0.0 # No previous end mean to compare with
                axes.plot(velocities[start_bin:end_bin], (temp[i,][start_bin:end_bin])+diff, color=colours[i], label=f'{label} {i}', linewidth=1)
            else:
                # Calculate the diff: mean(end of previous scan) - mean(start of current scan)
                diff = end_mean - start_mean
                axes.plot(velocities[start_bin:end_bin], (temp[i,][start_bin:end_bin])+diff, color=colours[i], label=f'{label} {i}', linewidth=1)

            end_mean = np.mean((temp[i,][end_bin-delta:end_bin]+diff))  # Mean of the last 5% of the usable bandwidth
            order.append(i)  # Keep track of the order of the scans for plotting

        # Initialize start and end mean values
        start_mean = np.mean(temp[mode,][start_bin:start_bin+delta])  
        end_mean = 0.0
        
        # Start in the middle scan and plot the adjusted sky signal towards the beginning of the usable bandwidth
        for i in range(mode-1, -1, -1):
            freq = [start_freq/1e6 + i * (args.sample_rate/1e6 - freq_overlap/1e6) + args.sample_rate/1e6/args.fft_size * j for j in range(args.fft_size)]
        
            velocities = freq2vel(freq * u.MHz, rest=f_e) - v_adjustment # Calculate LSR (adjusted) velocity from frequency

            end_mean = np.mean(temp[i,][end_bin-delta:end_bin])  # Mean of the last 5% of the usable bandwidth
            
            # Calculate the diff: mean(end of previous scan) - mean(start of current scan)
            diff = start_mean - end_mean
            axes.plot(velocities[start_bin:end_bin], (temp[i,][start_bin:end_bin])+diff, color=colours[i], label=f'{label} {i}', linewidth=1)

            start_mean = np.mean((temp[i,][start_bin:start_bin+delta])+diff)  # Mean of the last 5% of the usable bandwidth
            order.append(i)  # Keep track of the order of the scans for plotting

        from scipy.signal import savgol_filter
        # Plot a gaussian smoothing of the sky signal
        smoothed = savgol_filter(temp, window_length=(args.fft_size//8), polyorder=3)
        #smoothed = gaussian_filter1d(temp, sigma=10, axis=1)
        for i in range(freq_scans):
            axes.plot(velocities[start_bin:end_bin], smoothed[i,][start_bin:end_bin]+diff, color='red', linestyle='--', label=f'Smoothed Signal', linewidth=0.5)

        #noise_region_mask = (vel < -150) | (vel > 80)
        #noise_floor = np.median(temp[noise_region_mask])
        #temp_corrected = temp - noise_floor

        axes.axvline(x=0, color='blue', linestyle='--', label='HI Line')
        order.append(freq_scans+1)  # Add an extra order for the HI line
        axes.set_xlabel(f'LSR Velocity {velocities[0].unit} (Adjusted by: {round(v_adjustment,3)})')
        axes.set_ylabel(units)

        handles, labels = axes.get_legend_handles_labels()
        idx_order = np.argsort(order)  # Sort the labels by scan number, argsort will return the indices in the sorted order
        axes.legend([handles[i] for i in idx_order], [labels[i] for i in idx_order], bbox_to_anchor=(1.01, 1), loc='upper left')

    else:

        # For each scan, plot the frequency spectrum
        for i in range(freq_scans):
            freq = [start_freq/1e6 + i * (args.sample_rate/1e6 - freq_overlap/1e6) + args.sample_rate/1e6/args.fft_size * j for j in range(args.fft_size)]
            axes.plot(freq, temp[i,], color=colours[i],label=f'{label} {i}', linewidth=0.5)

        axes.axvline(x=1420.4, color='blue', linestyle='--', label='HI Line')
        axes.set_xlabel('Frequency [MHz]')
        axes.set_ylabel(units)
        axes.legend(bbox_to_anchor=(1.01, 1), loc='upper left')
    
    # Set the title and adjust the layout
    type = 'Sky Signal by LSR Velocity' if adjusted else 'Sky Signal by Frequency'
    
    axes.set_title(type, fontsize = 14)  
    plt.grid()
    plt.subplots_adjust(right=0.8)  # Adjust right margin to fit legend

    plt.draw()
    plt.pause(0.0001)

    filename = f"{gen_file_prefix(args, dt=dt, type=type)}.png"
    plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=300)

    return fig, axes

def plot_total_pwr_timeline(args, dt, duration, freq_scan, scan_iter, int_tpw, colours):  # Plot the total power timeline
    """ Plot the total power timeline for the observation.
        The total power is the sum of the power spectrum for each second in the observation.
    """

    global OUTPUT_DIR, FIG_SIZE

    # Initialize the figure and axes for the total power timeline
    plt.close(300)  # Close any existing figure with the same number
    fig = plt.figure(num=300, figsize=FIG_SIZE)
    axes = fig.add_subplot(1, 1, 1)

    # Create a time array for the x-axis
    time = np.arange(1, len(int_tpw)+1)  # Time in seconds

    sec_idx = 0

    for scan in range(freq_scan + 1):  # For each frequency scan

        for iter in range(scan_iter + 1):  # For each scan iteration

            if args.load:
                label=f"Load {scan}" if f"Load {scan}" not in [l.get_label() for l in axes.lines] else ""
            else:
                label=f"Sky {scan}" if f"Sky {scan}" not in [l.get_label() for l in axes.lines] else ""

            # If this is not the first second in a new iteration
            if sec_idx > 0:
                # Connect the previous scan's total power to the current scan's total power
                axes.plot([time[sec_idx-1], time[sec_idx]], [int_tpw[sec_idx-1], int_tpw[sec_idx]], color=colours[scan], linewidth=1)
            
            # Plot the total power timeline
            axes.plot(time[sec_idx:sec_idx+duration], int_tpw[sec_idx:sec_idx+duration], color=colours[scan], label=label, linewidth=1)

            sec_idx += duration  # Increment the second index by the scan duration

    axes.set_title('Total Power Timeline', fontsize = 14)
    axes.set_xlabel('Time [s]')
    axes.set_ylabel('Total Power [a.u.]')
    axes.set_xlim(1, len(int_tpw)+1)  # Set x-axis limits to cover the entire duration of the observation   
    axes.legend(bbox_to_anchor=(1.01, 1), loc='upper right')
    plt.grid()

    filename = f"{gen_file_prefix(args, dt=dt, type='Total Power Timeline')}.png"
    plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=300)

    return fig, axes

def remove_dc_spike(fft_size, arr):

    """ Ref: https://pysdr.org/content/sampling.html#dc-spike-and-offset-tuning
    Identify and remove the DC spike (if present) at the center frequency """

    # Review the bins either side the centre of fft_size 
    # We expect the DC spike to occur in the central bin
    start = fft_size//2-1 # Zero indexed array
    end =  fft_size//2+2 # DC spike is in the middle

    # Calculate the mean and std deviation of the reviewed samples
    mean = np.mean(arr[start:end])
    std = np.std(arr[start:end])

    # Create a mask for values above one standard deviation from the mean
    mask = arr[start:end] > (mean + std)
    # Calculate the mean of the reviewed samples excluding the values in the mask
    mean_no_dc = np.mean(arr[start:end][~mask])

    #print(f"Considered samples from {start} to {end}: {arr[start:end]}")
    #print(f"Mean {mean} Std {std} Mask {mask} Mean Excluding DC {np.mean(arr[start:end][~mask])}")

    # Replace values above one standard deviation with the mean of the samples surrounding the DC spike
    arr[start:end][mask] = np.mean(arr[start:end][~mask])

def perform_scan(args, sdr, center_freq, sample_rate, duration, fft_size):

    global raw, pwr, mpr, bsl, extent

    logger.info(f"\nStarting scan at center freq {center_freq}...")
    row = 0

    # For each second in duration, read and process samples
    for sec in range(duration):

        x = np.zeros(int(sample_rate), dtype=np.complex128) # Initialize numpy array as complex128 type with zeroes
        x = sdr.read_samples(sample_rate) # Populate numpy array with one second of SDR samples

        if len(x) < sample_rate:
            logger.warning(f"Not enough samples read for second {sec+1}. Expected {sample_rate}, got {len(x)}. Skipping...")
            continue

        # Reshape the samples to fit into a number of rows, each of fftsize columns and convert to complex64 for better efficiency
        x = x[:len(x) - (len(x) % fft_size)].astype(np.complex64)  # Discard excess samples that don't fit into the FFT size
        x = x.reshape(-1, fft_size) # Reshape to have rows each of size fft_size columns

        # For each row i.e. 'shape[0]' in the reshaped sample set
        # Calculate the power spectrum and record raw and power spectrum data
        for j in range(x.shape[0]):

            raw[row,:] = x[j,:] # Record the raw samples in the complex plane
            pwr[row,:] = np.abs(np.fft.fftshift(np.fft.fft(x[j,:])))**2 # The power spectrum is the absolute value of the signal squared

            row += 1  # Increment the row index for the next set of samples

        # Range of rows (each of fftsize columns) for the current second
        row_start = row - x.shape[0]
        row_end = row - 1

        # Calculate the sum of the power spectrum (mpr) for each frequency bin in a given second
        mpr[sec,:] = np.sum(pwr[row_start:row_end,:], axis=0)  # Sum the power spectrum for each frequency bin (in columns)
        remove_dc_spike(fft_size, mpr[sec,:])

        signal_displays_update(raw, mpr, bsl, sec, duration, row_start, row_end, extent, fft_size)

        logger.info(f"Read SDR ({sec+1} sec): {x.size} samples, Sample Rate {sample_rate/1e6} MHz, Center Frequency {sdr.center_freq/1e6} MHz, Gain {sdr.gain} dB, Sample Power {np.sum(np.abs(x)**2):.2f} [a.u.]")

def sdr_init(sdr, center_freq, gain, sample_rate, ppm):
    """
    Initialize the RTL-SDR device with the specified parameters.
    """

    sdr.center_freq = int(center_freq)  # Hz
    sdr.gain = int(gain) if gain.isdigit() else 'auto'  # dB
    sdr.sample_rate = sample_rate  # Hz
    sdr.ppm = ppm  # PPM for frequency correction

    logger.info(f"Initialised SDR: Center Frequency {sdr.center_freq/1e6} MHz, Gain {sdr.gain} dB, Sample Rate {sdr.sample_rate/1e6} MHz, PPM {sdr.ppm}")

def sdr_check_gain_gaussianity(sdr, sample_rate=2.4e6, duration=1):
    """
    Check the Gaussianity of the SDR samples over a specified duration.
    """

    p_threshold = 0.05 # p-value threshold for Gaussian detection
    sample_limit = 5000  # limit for Shapiro–Wilk

    samples = int(duration * sample_rate)

    x = np.zeros(samples, dtype=np.complex128)
    x = sdr.read_samples(samples)

    # Take a random subset of samples to avoid warning and speed up test
    idx = np.random.choice(samples, size=sample_limit, replace=False)
    r_samples = x.real[idx]
    i_samples = x.imag[idx]

    # Run Gaussianity test (Shapiro–Wilk)
    stat_r, p_r = shapiro(r_samples)
    stat_i, p_i = shapiro(i_samples)

    if p_r > p_threshold and p_i > p_threshold:
        logger.info(f"Gaussianity test at gain={sdr.gain} dB passed: P-Values: Real={p_r:.3f} AND Imaginary={p_i:.3f} greater than threshold {p_threshold} with power {np.sum(np.abs(x)**2):.2f} [a.u.]")
        return True, (p_r, p_i)
    else:
        logger.info(f"Gaussianity test at gain={sdr.gain} dB failed: P-Values: Real={p_r:.3f} OR Imaginary={p_i:.3f} less than threshold {p_threshold} with power {np.sum(np.abs(x)**2):.2f} [a.u.]")
        return False, (p_r, p_i)

def sdr_get_gaussian_gain(sdr, sample_rate=2.4e6, duration=1, fft_size=1024, p_threshold=0.05, max_samples=5000):
    """Iterate through all SDR gain settings to find the optimal gain for Gaussianity.
        Return the gain setting that meets the Gaussianity criteria.
    """

    curr_gain = sdr.gain # remember current SDR gain setting
    num_samples = int(duration * sample_rate) # number of samples to collect

    # Gain settings
    Glist = [1,3,7,9,12,14,16,17,19,21,23,25,28,30,32,34,36,37,39,40,42,43,44,45,48,50]

    # Lists to hold p-values per gain
    p_r_list = []
    p_i_list = []

    # Loop over each gain setting
    for gain in Glist:

        sdr.gain = gain  # Set the SDR gain
        result, (p_r, p_i) = sdr_check_gain_gaussianity(sdr=sdr, sample_rate=sample_rate, duration=duration)

        p_r_list.append(p_r)
        p_i_list.append(p_i)

    gaussian = False
    gauss_gain = None
    for i in range(len(Glist) - 1):
        if (p_r_list[i] > p_threshold and p_i_list[i] > p_threshold and
            p_r_list[i+1] > p_threshold and p_i_list[i+1] > p_threshold):
            gaussian = True
            gauss_gain = Glist[i+1]
            sdr.gain = gauss_gain
            break

    # If we find a gaussian gain
    if gaussian:
        logger.info(f"Optimal SDR gain for gaussianity: {sdr.gain} dB\n")
    else: 
        logger.warning("\nNo SDR gain meets Gaussianity criteria — check signal chain.\n")

        max_p_r = np.max(p_r_list)
        # Set gauss gain to gain in Glist corresponding to maximum p_r_list else curr_gain if max=0.0
        gauss_gain = Glist[np.argmax(p_r_list)] if max_p_r > 0.0 else curr_gain
        sdr.gain = gauss_gain
        logger.warning(f"Propose SDR gain {sdr.gain} dB based on maximum p_r value {max_p_r}\n")

    gaussian_array = np.random.normal(loc=0.0, scale=1, size=num_samples)
    g_hist, g_bins = np.histogram(gaussian_array, range=(-1, 1),bins=fft_size)

    fig = plt.figure(num=400,figsize=FIG_SIZE)
    gs = GridSpec(3, 1, height_ratios=[0.5, 1, 1], left=0.07, right=0.93, top=0.93, bottom=0.07, hspace=0.4) # Gaussianity check displays

    # === Plot p-values vs gain ===
    ax1 = fig.add_subplot(gs[0])
    ax1.plot(Glist, p_r_list, 'ro-', label='Real part p-value')
    ax1.plot(Glist, p_i_list, 'bo-', label='Imag part p-value')
    ax1.axhline(p_threshold, color='k', linestyle='--', label=f'Threshold = {p_threshold}')
    ax1.set_xlabel('Gain')
    ax1.set_ylabel('p-value')
    ax1.set_title('Gaussianity Test (Shapiro–Wilk) vs Gain')
    ax1.legend(loc='upper right')
    ax1.grid(True)

    def add_reference_gaussian(ax, data, fft_size, label_gain):
        r_hist, r_bins = np.histogram(data.real, range=(-1, 1), bins=fft_size)
        i_hist, i_bins = np.histogram(data.imag, range=(-1, 1), bins=fft_size)

        # Convert to percentages
        r_hist = r_hist / np.sum(r_hist) * 100
        i_hist = i_hist / np.sum(i_hist) * 100

        # Plot histograms
        ax.bar(r_bins[:-1], r_hist, width=np.diff(r_bins), color='r', alpha=0.5, label='Real')
        ax.bar(i_bins[:-1], i_hist, width=np.diff(i_bins), color='b', alpha=0.5, label='Imaginary')

        # Std Deviation (spread or width of the distribution) 
        # Gaussian has:
        #    ~68% of data within 1 sigma
        #    ~95% of data within 2 sigma
        #    ~99.7% of data within 3 sigma
        # Set sigma = 0.334 to capture ~99.7% of data within 3 sigma

        mu, sigma = 0.0, 0.334  # Mean and standard deviation for Gaussian distribution
        gauss_array = np.random.normal(loc=mu, scale=sigma, size=len(data))
        g_hist, g_bins = np.histogram(gauss_array, range=(-1, 1), bins=fft_size)
        
        # Convert to percentages
        g_hist = g_hist / np.sum(g_hist) * 100

        ax.plot(g_bins[:-1], g_hist, color='black', label="Gauss")

        ax.set_xlabel('Value')
        ax.set_ylabel('Count (%)')
        ax.set_title(f'Histogram of SDR Samples (Gain = {label_gain} dB)')
        ax.legend(loc='upper right')

    # === Plot original gain setting ===
    sdr.gain = curr_gain
    x = sdr.read_samples(num_samples)
    ax2 = fig.add_subplot(gs[1])
    add_reference_gaussian(ax2, x, fft_size, sdr.gain)

    # === Plot Gaussian gain setting ===
    sdr.gain = gauss_gain
    x = sdr.read_samples(num_samples)
    ax3 = fig.add_subplot(gs[2])
    add_reference_gaussian(ax3, x, fft_size, sdr.gain)

    filename = f"{datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S')}-sdr-gaussian-gain-{gauss_gain}dB.png"
    plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=300)
    plt.pause(0.1)

    sdr.gain = curr_gain # Restore original gain setting
    return gaussian, gauss_gain

def sdr_stabilise(sdr, sample_rate=2.4e6, duration=5):
    """
    Stabilize the SDR by discarding initial samples for a specified duration.
    This allows the SDR to stabilize before starting data acquisition.
    """
    for _ in range(duration):  # Discard samples for each second in the duration
        discard = np.zeros(int(sample_rate), dtype=np.complex128)  # Initialize a numpy array to hold the samples
        discard = sdr.read_samples(sample_rate)
        logger.info(f"Stabilising SDR: Discarded {discard.size} samples, Sample Rate {sample_rate/1e6} MHz, Center Frequency {sdr.center_freq/1e6} MHz, Gain {sdr.gain} dB, Sample Power {np.sum(np.abs(discard)**2):.2f} [a.u.]")
    del discard  # Free up memory

def sdr_info():
    try:
        result = subprocess.run(['rtl_eeprom', '-d', '0'], capture_output=True, text=True)
        # Strangely the rtl_eeprom command returns the device information in stderr, not stdout
        output = result.stderr

        if output.strip() == "No supported devices found.":
            logger.warning("No local RTL-SDR devices found.")
            return None

        info = {}

        for line in output.splitlines():
            if 'Manufacturer' in line:
                info['Manufacturer'] = line.split(':', 1)[1].strip()
            if 'Product' in line:
                info['Product'] = line.split(':', 1)[1].strip()
            if 'Serial number' in line:
                info['Serial'] = line.split(':', 1)[1].strip()
        
        return info

    except Exception as e:
        logger.exception("Error occurred while retrieving SDR information.")
        logger.exception(e)
    
    return None

def sdr_bias_t(enable=True):
    """ Enable or disable the bias tee on the RTL-SDR device.
    This is used to power external devices such as LNA (Low Noise Amplifier) or antenna preamplifiers.
    :param enable: True to enable the bias tee, False to disable it
    """ 

    cmd = ['rtl_biast', '-b', '1'] if enable else ['rtl_biast', '-b', '0']

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            logger.info(f"Switched BiasT to {'ON' if enable else 'OFF'} with command: {' '.join(cmd)}")
        else:
            logger.error(f"Failed to switch BiasT {'ON' if enable else 'OFF'} with command: {' '.join(cmd)}, return code: {result.returncode}")
            logger.error(result.stdout)
            logger.info(result.stderr)

    except Exception as e:
        logger.exception(f"Error occurred while running command: {' '.join(cmd)}")
        logger.exception(e)

    return result.returncode == 0  # Return True if the command was successful, False otherwise

def determine_scans(start_freq, end_freq, sample_rate, duration):
    """
    Calculate the number of frequency scans needed to cover the frequency range from start_freq to end_freq
    and the overlap in the frequency domain. NOTE: The overlap is different to the non-usable bandwidth !!
    
    Calculate the number of scan iterations and scan duration to 
    keep the scan duration within 0-60 seconds i.e. manageable from a performance perspective.

    E.g. We may need 10 scans of 1 minute each to cover the frequency range from start_freq to end_freq,
    where each scan is iterated 5 times to cover the duration of 5 minutes per frequency scan.

    :param start_freq: Start frequency in Hz
    :param end_freq: End frequency in Hz
    :param sample_rate: Sample rate in Hz
    :param duration: Duration of the observation in seconds
    :return: freq_scans, freq_overlap, scan_iterations, scan_duration
    """

    # Calculate the number of frequency scans to cover the bandwidth (ceiling of bandwidth/sample_rate)
    freq_scans = int(-((end_freq-start_freq)) // -(sample_rate * USABLE_BANDWIDTH))  # Ceiling division
    freq_overlap = round((sample_rate * freq_scans - (end_freq-start_freq))/(freq_scans-1) if freq_scans > 1 else 0,4) # Overlap in the frequency domain (Hz) rounded to 4 decimals
    scan_iterations = int(np.ceil(duration / 60))  # Number of iterations of a frequency scan, # e.g. 5 minutes of data will be 5 scans of 1 minute each
    scan_duration = duration // scan_iterations if scan_iterations > 1 else duration  # Duration of each scan in seconds

    logger.info(f"Frequency Scans-Iterations: {freq_scans}-{scan_iterations} each of Scan Duration: {scan_duration} sec(s)")
    logger.info(f"Sample Rate: {sample_rate/1e6} MHz, Overlap: {freq_overlap/1e6:.2f} MHz")

    scans_meta = {}
    scans_meta["scans"] = []  # Initialize the scans list in the metadata structure

    for i in range(freq_scans * scan_iterations):
        scan_num = i // scan_iterations
        scan_iter = i % scan_iterations
        # Calculate the start, end and center frequencies for each scan
        scan_start_freq = start_freq + (scan_num * (sample_rate - freq_overlap)) 
        scan_end_freq = scan_start_freq + sample_rate
        scan_center_freq = scan_start_freq + sample_rate / 2

        # Create a json structure to hold parameters for each scan e.g. scan number, duration, center frequency, start and end frequencies
        scan_meta = {
            "scan_num": scan_num,
            "scan_iter": scan_iter,
            "scan_duration": scan_duration,
            "scan_center_freq": scan_center_freq,
            "scan_start_freq": scan_start_freq,
            "scan_end_freq": scan_end_freq
        }
        scans_meta["scans"].append(scan_meta)  # Append the scan metadata to the scans list

    #overlap = (args.sample_rate * scans - (args.bandwidth * 1e6))/(scans-1) if scans > 1 else 0  # Overlap in the frequency domain
    return freq_scans, freq_overlap, scan_iterations, scan_duration, scans_meta

def on_key_press(event):
    """ Callback function to handle key press events in the polar plot. """
    global cam_key_press
    cam_key_press = True

def main():

    global OUTPUT_DIR, USABLE_BANDWIDTH, FIG_SIZE, bsl, pwr, mpr, raw, extent, sdr, cam_fig, cam_axes, polar, sig_fig, sig_axes, pwr_im
    global int_mpr, int_bsl, int_gain_cal, int_tsys_cal, int_tpw, int_tbl

    args = init_args() # Initialize command line arguments
    validate_args(args) # Validate the command line arguments

    create_output_directory()  # Create output directory if it doesn't exist
    fits_file = download_survey_fits(args, survey='hi4pi', size=30) # Download the survey FITs file for the target coordinates

    # Calculate the start and end frequencies given the center frequency, usable bandwidth and sampling rate
    # Start frequency is center frequency minus half the bandwidth minus half the sample rate adjusted for usable bandwidth
    # End frequency is center frequency plus half the bandwidth plus half the sample rate adjusted for usable bandwidth
    start_freq = args.center_freq - args.bandwidth * 1e6 / 2 - args.sample_rate * (1-USABLE_BANDWIDTH)/2  # Start of frequency scanning
    end_freq = args.center_freq + args.bandwidth * 1e6 / 2 + args.sample_rate * (1-USABLE_BANDWIDTH)/2  # End of frequency scanning
    logger.info(f"Start Frequency: {start_freq/1e6:.2f} MHz, End Frequency: {end_freq/1e6:.2f} MHz")

    # Calculate the number of scans (each of sample_rate * usable_bandwidth) and overlaps to cover start_freq to end_freq
    freq_scans, freq_overlap, scan_iterations, scan_duration, scans_meta = determine_scans(start_freq, end_freq, args.sample_rate, args.duration)  

    # Initialise global scan data arrays i.e. arrays used for a signal scan
    init_scan_data_arrays(args.sample_rate, scan_duration, args.fft_size)  

    # Prepare a colormap to assign different colours to each scan
    cmap = mpl.colormaps['plasma']
    colours = cmap(np.linspace(0, 1, freq_scans)) # Take colours at regular intervals spanning the colormap.

    # Initialise integrated (over duration) numpy arrays to hold the power spectrum and baseline load
    int_mpr = np.zeros((freq_scans, args.fft_size), dtype=np.float64) # Initialise (scan X fft_size) array for integrated power spectrum
    int_bsl = np.ones((freq_scans, args.fft_size), dtype=np.float64) # Initialise (scan X fft_size) array for integrated baseline load

    # Initialise integrated (over scans) numpy arrays to hold the calibration data
    int_gain_cal = np.ones((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for gain calibration
    int_tsys_cal = np.zeros((freq_scans, args.fft_size), dtype=np.float64)  # Initialise (scan X fft_size) array for Tsys calibration

    # Initialise integrated (over scans) numpy arrays to hold the total power timeline
    int_tpw = np.zeros(freq_scans * scan_duration * scan_iterations, dtype=np.float64)  # Initialise (scans * duration * iterations) array for total power sky timeline
    int_tbl = np.zeros(freq_scans * scan_duration * scan_iterations, dtype=np.float64)  # Initialise (scans * duration * iterations) array for total power baseline timeline

    # Record date and time that this observation starts
    obs_start = datetime.datetime.now().astimezone()

    # Create and initialize metadata structure to store information about the observation
    meta = init_metadata(args, scans_meta)

    # Initialize inertial motion sensor to None
    motion = None

    # If an Inertial Measurement Unit (IMU) device is specified, initialise it and connect
    if args.imu:

        connected = False

        try:
            motion = Motion(args.imu, args.baud)  # Initialize the IMU device
            connected = motion.connect()

        except:
            logger.error(f"Failed to connect to IMU device at {args.imu} with baudrate {args.baud}")

        meta["imu"]={
            "device": args.imu,
            "baudrate": args.baud,
            "connected": connected
        }

    # If we are not performing calibration, load calibration data from CSV files for each scan
    if not args.load and not args.calibrate:

        scan_start_freq = start_freq

        for freq_scan in range(freq_scans):

            scan_center_freq = (scan_start_freq + args.sample_rate/2)

            gain_file = None #load_gain_calibration(gen_file_prefix(args, duration=args.duration, center_freq=scan_center_freq, freq_scan=freq_scan, type='gain'))  # Load gain calibration if not generating a load file
            tsys_file = None #load_tsys_calibration(gen_file_prefix(args, duration=args.duration, center_freq=scan_center_freq, freq_scan=freq_scan, type='tsys'))  # Load Tsys calibration if not generating a load file

            if gain_file is None:
                bsl_file = load_baseline(gen_file_prefix(args, duration=args.duration, center_freq=scan_center_freq, freq_scan=freq_scan,type='load'), freq_scan) # Load baseline power spectrum if not generating a load file

                if bsl_file is None and gain_file is None:
                    logger.error(f"Baseline load and gain files for scan {freq_scan} not found. At least one must be present. Exiting...")
                    exit(1)
            else:
                int_bsl[freq_scan, :] = int_gain_cal[freq_scan, :]  # Use the gain calibration as the baseline load if no baseline file is provided

            # Record calibrators in metadata
            meta["observation"]["calibrators"] = {
                "gain_file": gain_file if gain_file is not None else "None",
                "tsys_file": tsys_file if tsys_file is not None else "None",
                "bsl_file":  bsl_file if 'bsl_file' in locals() and bsl_file is not None else "None"
            }  

            scan_start_freq += args.sample_rate - freq_overlap

    # If we are not reading samples from a file, initialize an RTL-SDR device
    if args.read == 'none':

        sdr = None

        # Initialize the RTL-SDR device
        try:
            info = sdr_info()  # Print the SDR device information

            if info is None:

                if args.host is None or args.port is None:
                    logger.error("No RTL-SDR device found and no host/port specified to connect to a server instance. Exiting...")
                    exit(1)
                else:
                    logger.info(f"Connecting to RTL-SDR server at {args.host}:{args.port}...")
                    sdr = RtlSdrTcpClient(hostname=args.host, port=args.port)

            else:
                logger.info(f"Found RTL-SDR device: {info.get('Product', 'Unknown')} by {info.get('Manufacturer', 'Unknown')} with serial number {info.get('Serial', 'Unknown')}")
                
                meta["sdr"] = {}

                meta["sdr"]["product"] = info.get('Product', 'Unknown')  # Add product info to metadata
                meta["sdr"]["manufacturer"] = info.get('Manufacturer', 'Unknown')  # Add manufacturer info to metadata
                meta["sdr"]["serial"] = info.get('Serial', 'Unknown')  # Add serial number to metadata

                # If the device is a Blog V, enable the bias tee and update the metadata accordingly
                if 'Blog V' in info['Product']:
                    sdr_bias_t(enable=True)
                    meta["sdr"]["bias_tee"] = True
                else:
                    meta["sdr"]["bias_tee"] = False  

                # Instantiate and initialise the RTL-SDR device
                sdr = RtlSdr()

            initial_cf = int(start_freq + args.sample_rate/2)  # Set the center frequency for the first scan

            sdr_init(sdr, initial_cf, args.gain, args.sample_rate, args.ppm)  # Configure SDR to appropriate center frequency etc
            gaussian, (p_r, p_i) = sdr_check_gain_gaussianity(sdr=sdr, sample_rate=args.sample_rate, duration=1)

            # If SDR Gaussianity check fails
            if not gaussian:
                logger.warning(f"Gaussianity test at SDR gain={sdr.gain} dB failed.\nFind a Gaussian distribution of samples ? (y/n)")
                confirm = input().strip().lower()

                # If user wants to find a Gaussian distribution of samples
                if confirm == 'y':
                    gaussian, gauss_gain = sdr_get_gaussian_gain(sdr, sample_rate=args.sample_rate, duration=1, fft_size=args.fft_size)
                    confirm = input(f"Configure {gauss_gain} gain on the SDR ? (y/n)").strip().lower()

                    # If the user confirms
                    if confirm == 'y':
                        # Adjust gain to the Gaussian setting
                        args.gain = str(gauss_gain)  
                        sdr_init(sdr, initial_cf, args.gain, args.sample_rate, args.ppm)  

                logger.info("Proceeding with SDR gain set to {}".format(sdr.gain))

        except Exception as e:
            logger.exception(f"Error while trying to instantiate an SDR instance: {e}")
            exit(1)
   
    # We are about to observe the sky if we not doing a 'load' nor are we reading samples from a file
    observing_sky = not args.load and args.read == 'none'

    # If we are observing the sky
    if observing_sky:

        # Initialize the control and monitoring displays
        cam_fig, cam_axes, polar, pec = init_cam_displays(args, fits_file=fits_file, motion=motion)  # Initialize control and monitoring displays

        global cam_key_press

        cam_key_press = False  # Flag to indicate if the cam plots are closed
        # Connect the key press event of the cam plots to a callback function
        cam_fig.canvas.mpl_connect('key_press_event', on_key_press)

        while (not cam_key_press):
            polar.update_targetbox()
            pec.plot(cam_fig, cam_axes[3])
            plt.pause(0.1)  # Pause to allow the plot to update

            # If we are using an RTL-SDR device...start stabilizing it
            if 'sdr' in globals() and sdr is not None:
                sdr_stabilise(sdr, sample_rate=args.sample_rate, duration=1)  # Stabilize the SDR by discarding initial samples for duration seconds
    else:
        cam_fig = cam_axes = polar = None  # If not observing the sky, set polar to None

    # Perform one or more scans
    try:

        if observing_sky and polar:

            # Start tracking the targets on the polar plot
            polar.start_observing()

        # Initialise the start freq for the first scan
        scan_start_freq = start_freq 

        # Index to keep track of the elapsed seconds in the observation
        sec_idx = 0 

        # Record date and time that this observation starts
        obs_start = datetime.datetime.now().astimezone()
        meta["observation"]["obs_start"] = obs_start.isoformat()  # Record the start time in the metadata
        logger.info(f"Observation started at {obs_start} for {args.duration} second(s), with {freq_scans} frequency scan(s) of {scan_duration} seconds each and {scan_iterations} iteration(s).")

        # For each frequency scan
        for freq_scan in range(freq_scans):

            # Determine the scan center freq and extent (range) of the scan on the spectrum
            scan_center_freq = int(scan_start_freq + args.sample_rate/2)
            
            extent = [(scan_center_freq + args.sample_rate/-2)/1e6,
                (scan_center_freq + args.sample_rate/2)/1e6,
                (args.fft_size * pwr.shape[0])/args.sample_rate, 0]

            # If we are using an RTL-SDR device...
            if 'sdr' in globals() and sdr is not None:
                if args.load or freq_scan > 0:  # If we are loading calibration data or this is not the first scan
                    sdr.center_freq = int(scan_center_freq)  # Set the center frequency for scan 1 and onwards (scan 0 is already set)
                    sdr_stabilise(sdr, sample_rate=args.sample_rate, duration=30)  # Stabilize the SDR by discarding initial samples for duration seconds

            # For each scan iteration NOTE: It is important to iterate scans within the frequency scan loop
            # This is because we want to minise adjustments to the SDR center frequency as we need to stabilise it after each change
            for scan_iter in range(scan_iterations):

                # Setup the baseline scan
                bsl = int_bsl[freq_scan,]
                logger.info(f"Using BSL for scan {freq_scan}-{scan_iter} with mean {np.mean(bsl)}, max {np.max(bsl)}, min {np.min(bsl)}")

                sig_fig, sig_axes = signal_displays_init(args, scan=freq_scan, iter=scan_iter, duration=scan_duration, scan_center_freq=scan_center_freq, extent=extent)  # Initialize signal displays

                # If we are reading raw IQ samples from a file
                if args.read != 'none':

                    # Read the raw IQ samples for the scan from the specified file and update the metadata
                    meta['observation']['read_file'] = read_scan(
                        args, 
                        scan_center_freq=scan_center_freq, 
                        extent=extent, 
                        duration=scan_duration, 
                        fft_size=args.fft_size, 
                        freq_scan=freq_scan, 
                        scan_iter=scan_iter)[:17]  # Read the raw IQ samples for the scan from the specified file
                else:

                    try:

                        # If we have a motion sensor 
                        if 'motion' in locals():
                            # Update scan meta data with current motion altitude, azimuth and temperature
                            alt, az = motion.get_altaz() if motion is not None else ("No IMU sensor", "No IMU sensor")
                            temp = motion.get_temperature() if motion is not None else "No IMU sensor"

                            meta["observation"]["scans"][freq_scan * scan_iterations + scan_iter].update({
                                "scan_imu_temp": temp,
                                "scan_imu_alt": alt,
                                "scan_imu_az": az
                            })

                        if 'polar' in globals() and polar is not None:
                            # Get the current altitude and azimuth of the target from the polar plot
                            alt, az = polar.get_target_altaz()
                            meta["observation"]["scans"][freq_scan * scan_iterations + scan_iter].update({
                                "scan_target_alt": alt,
                                "scan_target_az": az
                            })

                        meta["observation"]["scans"][freq_scan * scan_iterations + scan_iter].update({
                            "scan_start": datetime.datetime.now().astimezone().isoformat() })

                        # Perform a scan using the RTL-SDR device
                        perform_scan(args, sdr, scan_center_freq, args.sample_rate, scan_duration, args.fft_size)  # Collect samples and populate data arrays

                        # Update scan meta data to indicate datetime of scan completion and current motion azimuth and elevation
                        meta["observation"]["scans"][freq_scan * scan_iterations + scan_iter].update({
                            "scan_end": datetime.datetime.now().astimezone().isoformat() })

                    except (OSError, EOFError) as e:
                        # OSError = network error, EOFError = server closed connection
                        logger.exception(f"SDR TCP Server Connection lost: {e}")
                        logger.info("Attempting to reconnect to the SDR device...")

                        # Close the SDR device if it is open
                        if sdr is not None:
                            sdr.close()
                            sdr = None  

                        # Attempt to reconnect to the SDR device
                        attempt = 0

                        while sdr is None and attempt < MAX_RECONNECT_ATTEMPTS:  # Keep trying to reconnect until successful
                            try:
                                sdr = RtlSdrTcpClient(hostname=args.host, port=args.port) if args.host and args.port else RtlSdr()
                                logger.info("Reconnected to the SDR device successfully.")

                                sdr_init(sdr, scan_center_freq, args.gain, args.sample_rate, args.ppm)  # Reinitialize the SDR with the same parameters
                                sdr_stabilise(sdr, sample_rate=args.sample_rate, duration=30)  # Stabilize the SDR after reconnecting
                                
                            except Exception as e:
                                logger.exception(f"Failed to reconnect to the SDR device: {e}")
                                time.sleep(5)

                            attempt += 1

                    # If we need to write raw IQ samples to file for this scan
                    if args.write:
                        # Save the IQ samples to a file
                        filename = f"{gen_file_prefix(args, duration=scan_duration, center_freq=scan_center_freq, dt=obs_start, freq_scan=freq_scan, scan_iter=scan_iter, type='raw')}.iq"
                        with open(f"{OUTPUT_DIR}/{filename}", 'wb') as f:
                            raw.tofile(f)
                        logger.info(f"Saving {raw.shape} SDR samples to {filename}")

                logger.info(f"Scan {freq_scan}-{scan_iter} Total int_mpr scan power: {np.sum(int_mpr[freq_scan, :])}[a.u.] Total mpr scan power: {np.sum(mpr)}[a.u.]")

                # Aggregate the power spectrum for the current scan
                np.add(int_mpr[freq_scan, :], np.sum(mpr, axis=0), out=int_mpr[freq_scan, :])
                
                # Sum the power spectrum for each second in the current scan
                int_tpw[sec_idx:sec_idx+scan_duration] = np.sum(mpr, axis=1)

                # Update the signal display plots
                sig_axes[0].cla() # Clear the previous plots
                init_pwr_spectrum_axes(sig_axes[0], "Summed Power/Duration (MPR,BSL)", extent, units="{np.abs(shift fft(signal))**2}")  # Initialize the mean power spectrum axes
                sig_axes[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_mpr[freq_scan], color=colours[freq_scan], label='Signal (MPR)')
                sig_axes[0].plot(np.linspace(extent[0], extent[1], args.fft_size), int_bsl[freq_scan], color='black', label='Baseline (BSL)')
                sig_axes[0].legend(loc='lower right')

                sig_axes[1].cla() # Clear the previous plots
                init_pwr_spectrum_axes(sig_axes[1], "Summed Power/Duration (MPR/BSL)", extent)  # Initialize the sky signal axes
                
                sig_axes[1].plot(
                    np.linspace(extent[0], extent[1], args.fft_size), 
                    int_mpr[freq_scan] / (int_bsl[freq_scan]*scan_duration), 
                    color=colours[freq_scan], 
                    label='Signal (MPR/BSL)')
                
                sig_axes[1].legend(loc='lower right')

                sig_axes[4].cla()  # Clear the previous plots
                init_total_power_axes(sig_axes[4], scan_duration)  # Initialize the total power timeline axes
                
                sig_axes[4].plot(
                    np.arange(1, scan_duration + 1), 
                    int_tpw[sec_idx:sec_idx+scan_duration], 
                    color=colours[freq_scan], 
                    label='Total Power (TPW)')
                
                # Add an average total power line
                sig_axes[4].axhline(y=np.mean(int_tpw[sec_idx:sec_idx+scan_duration]), color='red', linestyle='--', label='Mean')
                sig_axes[4].legend(loc='lower right')

                sig_fig.suptitle(f"Scan: {freq_scan}-{scan_iter} Center Frequency: {scan_center_freq/1e6:.2f} MHz, Gain: {args.gain} dB, Sample Rate: {args.sample_rate/1e6:.2f} MHz, FFT Size: {args.fft_size}", fontsize=14)
                #sig_fig.set_tight_layout(True)

                # Save the sky signal plot for the current scan
                filename = f"{gen_file_prefix(args, duration=scan_duration,center_freq=scan_center_freq, dt=obs_start, freq_scan=freq_scan,scan_iter=scan_iter, type='sigfig')}.png"
                plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=300)
                plt.pause(0.0001)

                plot_total_pwr_timeline(args, dt=obs_start, duration=scan_duration, freq_scan=freq_scan, scan_iter=scan_iter, int_tpw=int_tpw, colours=colours)  # Plot the total power timeline

                plt.draw()

                # Initialise global data arrays used for each scan
                init_scan_data_arrays(args.sample_rate, scan_duration, args.fft_size)  
                
                sec_idx += scan_duration  

             # Increment the start frequency ahead of the next scan
            scan_start_freq += args.sample_rate - freq_overlap

    except KeyboardInterrupt:
        logger.exception("Interrupted by user")
    finally:

        # If an SDR device was initialized
        if 'sdr' in globals() and sdr is not None:
            logger.info("Closing the SDR device...")
            # sdr.stop() seems to throw an error, so we just close it
            sdr.close()

            # If the SDR device is a Blog V
            if 'Blog V' in meta["sdr"]["product"]:
                # Disable the bias tee 
                #pass
                sdr_bias_t(enable=False)

        # Stop tracking polar plot targets if necessary
        if 'polar' in locals() and polar is not None:
            polar.update_tracks()
            polar.update_targetbox()
            polar.stop_observing() 

        # Stop recording PEC if it was initialized
        if 'pec' in locals() and pec is not None:
            pec.stop_recording()

        # Disconnect the motion sensor if it was initialized
        if 'motion' in locals() and motion is not None:
            motion.disconnect()

    # Record date and time that this observation ends
    obs_end = datetime.datetime.now().astimezone()
    logger.info(f"Data acquisition completed at {obs_end} after {obs_end - obs_start} seconds.")
    meta["observation"]["obs_end"] = obs_end.isoformat()  # Add end time to metadata
    meta["observation"]["obs_duration"] = (obs_end - obs_start).total_seconds()  # Add duration to metadata

    # Normalise the integrated power spectrum to power per second before plotting and saving
    int_mpr = int_mpr / ((scan_iter+1) * scan_duration)  # Normalise the integrated power spectrum to power per second

    # Plot the integrated sky signal (an entire loop of frequency scans) as well as the current total power timeline 
    plot_integrated_sky_signal(args, dt=obs_start, freq_scans=freq_scans, start_freq=start_freq, freq_overlap=freq_overlap, int_mpr=int_mpr, int_bsl=int_bsl, colours=colours, adjusted=False)  # Plot the sky signal as Power / Frequency
    plot_integrated_sky_signal(args, dt=obs_start, freq_scans=freq_scans, start_freq=start_freq, freq_overlap=freq_overlap, int_mpr=int_mpr, int_bsl=int_bsl, colours=colours, adjusted=True)  # Plot the sky signal as Power / LSR Velocity

    # Save metadata about the observation to a JSON file
    save_metadata(args, obs_start, meta)
    # Save the total power timeline to a CSV file
    save_total_pwr_timeline(args, obs_start, freq_scans, int_tpw)
    # Save the integrated mean power spectrum to CSV files, one per scan
    save_integrated_mpr_spectrum(args, dt=obs_start, duration=args.duration, freq_scans=freq_scans, start_freq=start_freq, freq_overlap=freq_overlap, int_mpr=int_mpr)

    # Save the cam plots if they were initialized
    if cam_fig:
        cam_fig.savefig(f"{OUTPUT_DIR}/{gen_file_prefix(args, dt=obs_start, type='cam')}.png", dpi=300)

    # Print the list of output files generated during the observation
    out_files = print_output_files(obs_start)

    # Ask the user if they want to close the application and optionally delete output files
    logger.info(f"Close the application (press ENTER)\nClose the application and DELETE the output files (press dd)")
    confirm = input().strip().lower()
    if confirm in ['dd']:
        for f in out_files:
            os.remove(OUTPUT_DIR + "/" + f)
        logger.info(f"Deleted output files")

    plt.close('all')

if __name__ == "__main__":
    main()

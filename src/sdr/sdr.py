from rtlsdr import RtlSdr, RtlSdrTcpClient

import numpy as np
import sys
import subprocess
import threading
import time
import functools

import logging
logger = logging.getLogger(__name__)

DEFAULT_READ_SIZE = 256*1024  # Default number of samples/bytes to read from the SDR

""" Decorator to ensure thread-safe access to SDR methods. If the SDR is not connected, it logs a warning and returns a default value.
    :param default: Value to return if the SDR is not connected
"""
def sdr_guard(default=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            with SDR._mutex:
                if self.rtlsdr is None:
                    logger.warning("SDR device not connected.")
                    return default
                return func(self, *args, **kwargs)
        return wrapper
    return decorator

class SDR:
    """ Software Defined Radio (SDR) interface class for RTL-SDR devices.
        This class provides methods to connect to the SDR, retrieve device information,
        enable/disable the bias tee, and stabilize the device by discarding initial samples.
    """

    _mutex = threading.RLock()      # Mutex controlling access to the SDR

    def __init__(self):
        """ Initialize the SDR interface and connect to the first available RTL-SDR device.     """
        with SDR._mutex:

            try:
                self.rtlsdr = RtlSdr()
            except Exception as e:
                logger.exception(f"SDR could not connect due to exception: {e}")
                self.rtlsdr = None

        info = self.get_eeprom_info()
        if info:
            logger.info(f"SDR connected, device information: {info}")
        else:
            logger.warning("SDR connected but unable to retrieve device information.")

    def __del__(self):

        with SDR._mutex:

            if self.rtlsdr:
                self.rtlsdr.close()
                self.rtlsdr = None
                logger.info("SDR connection closed.")
        
    def get_eeprom_info(self):

        with SDR._mutex:

            if self.rtlsdr is None:
                logger.warning("SDR device not connected.")
                return None

            try:
                result = subprocess.run(['rtl_eeprom', '-d', '0'], capture_output=True, text=True)
            except Exception as e:
                logger.exception(f"SDR exception occurred while retrieving SDR information. {e}")
                raise e

            eeprom_info = {}

            # Strangely the rtl_eeprom command returns the device information in stderr, not stdout
            output = result.stderr

            if output.strip() == "No supported devices found.":
                logger.warning("No local RTL-SDR devices found.")
            else: 
                for line in output.splitlines():
                    if 'Manufacturer' in line:
                        eeprom_info['Manufacturer'] = line.split(':', 1)[1].strip()
                    if 'Product' in line:
                        eeprom_info['Product'] = line.split(':', 1)[1].strip()
                    if 'Serial number' in line:
                        eeprom_info['Serial'] = line.split(':', 1)[1].strip()
            
            return eeprom_info

    def set_bias_t(self, enable=True):
        """ Enable or disable the bias tee on the RTL-SDR device.
        This is used to power external devices such as LNA (Low Noise Amplifier) or antenna preamplifiers.
        :param enable: True to enable the bias tee, False to disable it
        """ 

        with SDR._mutex:

            if self.rtlsdr is None:
                logger.warning("SDR device not connected.")
                return False

            cmd = ['rtl_biast', '-b', '1'] if enable else ['rtl_biast', '-b', '0']

            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
            except Exception as e:
                logger.exception(f"SDR exception occurred while running command: {' '.join(cmd)} {e}")
                raise e

            if result.returncode == 0:
                logger.info(f"SDR switched BiasT to {'ON' if enable else 'OFF'} with command: {' '.join(cmd)}")
            else:
                logger.error(f"SDR failed to switch BiasT {'ON' if enable else 'OFF'} with command: {' '.join(cmd)}, return code: {result.returncode}")
                logger.error(result.stdout)
                logger.error(result.stderr)

            return result.returncode == 0  # Return True if the command was successful, False otherwise

    def stabilise(self, sample_rate=2.4e6, time_in_secs=5):
        """
        Stabilize the SDR by discarding initial samples for a specified duration.
        We typically see the SDR lose power over time as it warms up.
        """

        with SDR._mutex:

            if self.rtlsdr is None:
                logger.warning("SDR device not connected.")
                return False

            self.set_sample_rate(sample_rate)

            logger.info(f"SDR stabilising: Discarding samples for {time_in_secs} seconds at Sample Rate {sample_rate/1e6} MHz, Center Frequency {self.get_center_freq()/1e6} MHz, Gain {self.get_gain()}")

            for _ in range(time_in_secs):  # Discard samples for each second in the duration
                discard = np.zeros(int(sample_rate), dtype=np.complex128)  # Initialize a numpy array to hold the samples
                discard = self.read_samples(num_samples=sample_rate) # Read samples from the SDR
                logger.info(f"SDR stabilising: Discarded {discard.size} samples, Sample Rate {sample_rate/1e6} MHz, Center Frequency {self.get_center_freq()/1e6} MHz, Gain {self.get_gain()} dB, Sample Power {np.sum(np.abs(discard)**2):.2f} [a.u.]")
            del discard  # Free up memory

    @sdr_guard(default=None)
    def get_center_freq(self):
        return self.rtlsdr.center_freq

    @sdr_guard(default=None)
    def set_center_freq(self, value):
        self.rtlsdr.center_freq = value

    @sdr_guard(default=None)
    def get_sample_rate(self):
        return self.rtlsdr.sample_rate

    @sdr_guard(default=None)
    def set_sample_rate(self, value):
        self.rtlsdr.sample_rate = value

    @sdr_guard(default=None)
    def get_bandwidth(self):
        return self.rtlsdr.bandwidth

    @sdr_guard(default=None)
    def set_bandwidth(self, value):
        self.rtlsdr.bandwidth = value

    @sdr_guard(default=None)
    def get_gain(self):
        return self.rtlsdr.gain

    @sdr_guard(default=None)
    def set_gain(self, value):
        self.rtlsdr.gain = value

    @sdr_guard(default=None)
    def get_freq_correction(self):
        return self.rtlsdr.freq_correction

    @sdr_guard(default=None)
    def set_freq_correction(self, value):
        self.rtlsdr.freq_correction = value

    @sdr_guard(default=None)
    def get_gains(self):
        return self.rtlsdr.get_gains()

    @sdr_guard(default=None)
    def get_tuner_type(self):
        return self.rtlsdr.get_tuner_type()

    @sdr_guard(default=None)
    def set_direct_sampling(self, value):
        self.rtlsdr.direct_sampling = value

    @sdr_guard(default=None)
    def read_bytes(self, num_bytes=DEFAULT_READ_SIZE):
        return self.rtlsdr.read_bytes(num_bytes)

    @sdr_guard(default=None)
    def read_samples(self, num_samples=DEFAULT_READ_SIZE):
        time_enter = time.time()
        x = self.rtlsdr.read_samples(num_samples)
        x = np.array(x, dtype=np.complex64)
        time_exit = time.time()
        logger.info(f"SDR READ SAMPLES: requested {num_samples} samples, read {x.size} samples in {time_exit - time_enter:.3f} seconds")
        return x

def main():

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,  # Or DEBUG for more verbosity
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger(__name__)

    sdr = SDR()
    info = sdr.get_eeprom_info()
    if info:
        logger.info(f"SDR Information: {info}")
    
    sdr.stabilise()

    sdr.set_sample_rate(2.0e6)
    logger.info(f"SDR Sample Rate set to {sdr.get_sample_rate()/1e6} MHz")

    sdr.set_center_freq(435e6)  # Set frequency to 435 MHz
    logging.info(f"SDR Center Frequency set to {sdr.get_center_freq()/1e6} MHz")
    sdr.set_sample_rate(2.4e6)  # Set sample rate to 2.4 MHz
    logging.info(f"SDR Sample Rate set to {sdr.get_sample_rate()/1e6} MHz")
    sdr.set_gain(8.7)             # Set gain to 8.7 dB
    logging.info(f"SDR Gain set to {sdr.get_gain()} dB")

    logging.info(f"SDR Config - Center Frequency: {sdr.get_center_freq()/1e6} MHz, Sample Rate: {sdr.get_sample_rate()/1e6} MHz, Gain: {sdr.get_gain()} dB")

    if sdr.get_center_freq() != 435e6 or sdr.get_sample_rate() != 2.4e6 or sdr.get_gain() != 8.7:
        logger.error("SDR configuration did not apply correctly.")

    logging.info(f"SDR Tuner Type: {sdr.get_tuner_type()}, Available Gains: {sdr.get_gains()}")

    samples = sdr.read_samples(256*1024)
    logger.info(f"Read {len(samples)} samples from SDR.")

if __name__ == "__main__":
    main()
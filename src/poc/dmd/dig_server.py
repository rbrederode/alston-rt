import proto as dmd
import logging
import socket

from rtlsdr import RtlSdrTcpServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Or DEBUG for more verbosity
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't have to be reachable, just a valid IP
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

if __name__ == '__main__':

    ip_address = str(get_ip_address())

    print(f"IP Address of localhost: {ip_address}")

    info = dmd.sdr_info()

    # If the device is a Blog V, enable the bias tee and update the metadata accordingly
    if 'Blog V' in info['Product']:
        dmd.sdr_bias_t(enable=True)

    # Change host/port as needed
    try:
        server = RtlSdrTcpServer(hostname=ip_address, port=1234)
        print(f"Starting pyrtlsdr TCP server on {ip_address}:1234")
        server.run_forever()
    finally:

        # If the device is a Blog V, enable the bias tee and update the metadata accordingly
        if 'Blog V' in info['Product']:
            dmd.sdr_bias_t(enable=False)

    
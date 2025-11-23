import os
import time
import logging
from logging.handlers import RotatingFileHandler

class MillisecondFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        t = time.strftime(datefmt, ct)
        s = "%s:%03d" % (t, record.msecs)
        return s

# ensure log directory exists
log_dir = os.path.expanduser("~/.alston/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "alston.log")

# Configure default console logging first (ensures StreamHandler exists and root level is set)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# write to file with rotation
file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)  # or INFO
file_handler.setFormatter(MillisecondFormatter(
    '%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

# avoid adding the same handler twice (use handler class+filename check)
root = logging.getLogger()
already = any(isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == file_handler.baseFilename for h in root.handlers)
if not already:
    root.addHandler(file_handler)

logging.getLogger("ipc.tcp_server").setLevel(logging.INFO)  # Only INFO and above for tcp_server
logging.getLogger("ipc.tcp_client").setLevel(logging.INFO)  # Only INFO and above for tcp_client
logging.getLogger("util.timer").setLevel(logging.INFO)  # Only INFO and above for timer
logging.getLogger("env.processor").setLevel(logging.INFO)  # Only INFO and above for processor
logging.getLogger("env.app_processor").setLevel(logging.INFO)  # Only INFO and above for app processor
logging.getLogger("api.tm_dig").setLevel(logging.INFO)  # Only INFO and above for tm_dig api
logging.getLogger("api.sdp_dig").setLevel(logging.INFO)  # Only INFO and above for sdp_dig api
logging.getLogger("obs.oet").setLevel(logging.INFO)  # Only INFO and above for oet.obs
logging.getLogger("sdp.scan").setLevel(logging.INFO)  # Only INFO and above for sdp.scan
logging.getLogger("sdp.signal_display").setLevel(logging.INFO)  # Only INFO and above for sdp.signal_display
logging.getLogger("sdp.sdp").setLevel(logging.INFO)  # Only INFO and above for sdp.sdp
logging.getLogger("tm.tm").setLevel(logging.INFO)  # Only INFO and above for tm.tm
logging.getLogger("__main__").setLevel(logging.INFO)  # Only INFO and above for local application
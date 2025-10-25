import logging
import time

class MillisecondFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        t = time.strftime(datefmt, ct)
        s = "%s:%03d" % (t, record.msecs)
        return s

# Configure logging
logging.basicConfig(level=logging.INFO)  # Or DEBUG for more verbosity
for handler in logging.root.handlers:
    handler.setFormatter(MillisecondFormatter(
        '%(asctime)s %(levelname)s: %(message)s',  # log format
        datefmt='%Y-%m-%d %H:%M:%S'               # date format
    ))

logging.getLogger("ipc.tcp_server").setLevel(logging.INFO)  # Only INFO and above for tcp_server
logging.getLogger("ipc.tcp_client").setLevel(logging.INFO)  # Only INFO and above for tcp_client
logging.getLogger("util.timer").setLevel(logging.INFO)  # Only INFO and above for timer
logging.getLogger("env.processor").setLevel(logging.INFO)  # Only INFO and above for processor
logging.getLogger("env.app_processor").setLevel(logging.INFO)  # Only INFO and above for app processor
logging.getLogger("api.tm_dig").setLevel(logging.INFO)  # Only INFO and above for tm_dig api
logging.getLogger("api.sdp_dig").setLevel(logging.INFO)  # Only INFO and above for sdp_dig api
logging.getLogger("sdp.scan").setLevel(logging.INFO)  # Only INFO and above for sdp.scan
logging.getLogger("sdp.signal_display").setLevel(logging.INFO)  # Only INFO and above for sdp.signal_display
logging.getLogger("tm.tm").setLevel(logging.INFO)  # Only INFO and above for tm.tm
logging.getLogger("__main__").setLevel(logging.INFO)  # Only INFO and above for local application
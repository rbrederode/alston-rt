import logging
import threading
import time
import queue
import json
import argparse
from datetime import timezone, datetime

from queue import Queue

from ipc.message import APIMessage
from env import events
from ipc.tcp_client import TCPClient
from api.tm_sdp import TM_SDP
from env.app_processor import AppProcessor
from util.timer import Timer, TimerManager
from util.xbase import XSoftwareFailure

import logging
logger = logging.getLogger(__name__)

if __name__ == "__main__":

    # Setup logging configuration
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
        handlers=[
            logging.StreamHandler(),                     # Log to console
            ]
    )

    arg_parser = argparse.ArgumentParser(description="set_debug")
    arg_parser.add_argument("--host", type=str, required=False, help="TCP server host",default="localhost")
    arg_parser.add_argument("--port", type=int, required=False, help="TCP server port", default=50001)
    arg_parser.add_argument("--system", type=str, required=False, help="System name e.g. sdp", default="sdp")
    arg_parser.add_argument("--debug", type=str, required=False, help="Debug on/off", default="on")

    api_msg = APIMessage()

    queue = Queue()

    Timer.manager = TimerManager()
    Timer.manager.start()

    class Driver:
        def __init__(self):
            self.app_name = "tm"
            pass

        def get_interface(self, system_name):

            from api.tm_dig import TM_DIG
            from api.tm_sdp import TM_SDP

            if system_name in ["tm", "dig"]:
                return (TM_DIG(), None)
            elif system_name == "sdp":
                return (TM_SDP(), None)
            else:
                raise XSoftwareFailure(f"Driver has no interface for system {system_name}")

    tm = AppProcessor(name="tm", event_q=queue, driver=Driver())
    tm.start()

    # Start the TCP client and connect to the server
    client = TCPClient(queue=queue, host=arg_parser.parse_args().host, port=arg_parser.parse_args().port)
    client.connect()
    
    time.sleep(1)

    set_debug = {}
    set_debug["msg_type"] = "req"
    set_debug["action_code"] = "set"
    set_debug["property"] = "debug"
    set_debug["value"] = arg_parser.parse_args().debug

    api_msg.set_json_api_header(
        api_version="1.0",
        dt=datetime.now(timezone.utc),
        from_system="tm",
        to_system=arg_parser.parse_args().system,
        api_call=set_debug
    )

    client.send(api_msg)

    time.sleep(5)
    client.stop()    
    
    AppProcessor.stop_all()
    Timer.manager.stop()

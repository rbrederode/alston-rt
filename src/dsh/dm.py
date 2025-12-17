
from datetime import datetime, timezone
from queue import Queue
import time
import logging

from api import tm_dm
from env.app import App
from ipc.message import APIMessage
from ipc.action import Action
from ipc.message import AppMessage
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.comms import CommunicationStatus, InterfaceType
from models.health import HealthState
from models.obs import Observation
from models.oda import ObsList, ScanStore
from models.dsh import DishManagerModel
from util import log
from util.xbase import XBase, XStreamUnableToExtract

logger = logging.getLogger(__name__)

# Dish Manager (DM)

class DM(App):
    """A class representing the Dish Manager."""

    dm_model = DishManagerModel(id="dm001")

    def __init__(self, app_name: str = "dm"):

        super().__init__(app_name=app_name, app_model = self.dm_model.app)

        # Telescope Manager interface (TBD)
        self.tm_system = "tm"
        self.tm_api = tm_dm.TM_DM()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), host=self.get_args().tm_host, port=self.get_args().tm_port)
        self.tm_endpoint.start()
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint, InterfaceType.APP_APP)
        # Set initial Telescope Manager connection status
        self.dm_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED

    def add_args(self, arg_parser): 
        """ Specifies the Dish Manager's command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--tm_host", type=str, required=False, help="TCP host to listen on for Telescope Manager commands", default="localhost")
        arg_parser.add_argument("--tm_port", type=int, required=False, help="TCP port for Telescope Manager commands", default=50002)

    def process_init(self) -> Action:
        """ Processes initialisation events.
        """
        logger.debug(f"DM initialisation event")

        action = Action()
        return action

    def process_tm_connected(self, event) -> Action:
        """ Processes Telescope Manager connected events.
        """
        logger.info(f"Dish Manager connected to Telescope Manager: {event.remote_addr}")

        self.dm_model.tm_connected = CommunicationStatus.ESTABLISHED
        
        action = Action()
        
        # Send initial status advice message to Telescope Manager
        tm_adv = self._construct_status_adv_to_tm()
        action.set_msg_to_remote(tm_adv)
        return action

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.info(f"Dish Manager disconnected from Telescope Manager: {event.remote_addr}")

        self.dm_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_tm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Telescope Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Dish Manager received Telescope Manager {api_call['msg_type']} message with action code: {api_call['action_code']}")

        action = Action()
        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"DM timer event: {event}")

        action = Action()
        return action

    def process_status_event(self, event) -> Action:
        """ Processes status update events.
        """
        self.get_app_processor_state()

        action = Action()

        # If connected to Telescope Manager, send status advice message
        if self.dm_model.tm_connected == CommunicationStatus.ESTABLISHED:
            tm_adv = self._construct_status_adv_to_tm()
            action.set_msg_to_remote(tm_adv)

        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.dm_model.tm_connected != CommunicationStatus.ESTABLISHED:
            return HealthState.DEGRADED
        else:
            return HealthState.OK

    def _construct_status_adv_to_tm(self) -> APIMessage:
        """ Constructs a status advice message for the Telescope Manager.
        """
        tm_adv = APIMessage(api_version=self.tm_api.get_api_version())
        tm_adv.set_json_api_header(
            api_version=self.tm_api.get_api_version(), 
            dt=datetime.now(timezone.utc), 
            from_system=self.dm_model.app.app_name, 
            to_system="tm", 
            api_call={
                "msg_type": "adv", 
                "action_code": "set", 
                "property": tm_dm.PROPERTY_STATUS, 
                "value": self.dm_model.to_dict(), 
                "message": "DM status update"
            })
        return tm_adv

def main():
    dm = DM()
    dm.start()

    try:
        while True:
            # If there is a scan in the queue, process it, else sleep & continue 
            time.sleep(0.1)
                
    except KeyboardInterrupt:
        pass
    finally:
        dm.stop()

if __name__ == "__main__":
    main()

from datetime import datetime, timezone
from queue import Queue
import time
import logging

from api import tm_oet
from env.app import App
from ipc.message import APIMessage
from ipc.action import Action
from ipc.message import AppMessage
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.comms import CommunicationStatus
from models.health import HealthState
from models.obs import ObsModel
from models.oda import ObsStore, ScanStore
from models.oet import OETModel
from util import log
from util.xbase import XBase, XStreamUnableToExtract

OUTPUT_DIR = '/Users/r.brederode/samples'  # Directory to store observations

logger = logging.getLogger(__name__)

# Observation Execution Tool (OET)

class OET(App):
    """A class representing the Observation Execution Tool."""

    oet_model = OETModel(id="oet001")

    def __init__(self, app_name: str = "oet"):

        super().__init__(app_name=app_name, app_model = self.oet_model.app)

        # Telescope Manager interface (TBD)
        self.tm_system = "tm"
        self.tm_api = tm_oet.TM_OET()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), host=self.get_args().tm_host, port=self.get_args().tm_port)
        self.tm_endpoint.start()
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint)
        # Set initial Telescope Manager connection status
        self.oet_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED

    def add_args(self, arg_parser): 
        """ Specifies the science data processors command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--tm_host", type=str, required=False, help="TCP host to listen on for Telescope Manager commands", default="localhost")
        arg_parser.add_argument("--tm_port", type=int, required=False, help="TCP port for Telescope Manager commands", default=50002)

        arg_parser.add_argument("--output_dir", type=str, required=False, help="Directory to store observations", default=OUTPUT_DIR)

    def process_init(self) -> Action:
        """ Processes initialisation events.
        """
        logger.debug(f"OET initialisation event")

        action = Action()
        return action

    def process_tm_connected(self, event) -> Action:
        """ Processes Telescope Manager connected events.
        """
        logger.info(f"Observation Execution Tool connected to Telescope Manager: {event.remote_addr}")

        self.oet_model.tm_connected = CommunicationStatus.ESTABLISHED
        
        action = Action()
        
        # Send initial status advice message to Telescope Manager
        tm_adv = self._construct_status_adv_to_tm()
        action.set_msg_to_remote(tm_adv)
        return action

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.info(f"Observation Execution Tool disconnected from Telescope Manager: {event.remote_addr}")

        self.oet_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED

    def process_tm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Telescope Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"Observation Execution Tool received Telescope Manager {api_call['msg_type']} message with action code: {api_call['action_code']}")

        action = Action()
        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"OET timer event: {event}")

        action = Action()
        return action

    def process_status_event(self, event) -> Action:
        """ Processes status update events.
        """
        self.get_app_processor_state()

        action = Action()

        # If connected to Telescope Manager, send status advice message
        if self.oet_model.tm_connected == CommunicationStatus.ESTABLISHED:
            tm_adv = self._construct_status_adv_to_tm()
            action.set_msg_to_remote(tm_adv)

        return action

    def get_health_state(self) -> HealthState:
        """ Returns the current health state of this application.
        """
        if self.oet_model.tm_connected != CommunicationStatus.ESTABLISHED:
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
            from_system=self.oet_model.app.app_name, 
            to_system="tm", 
            api_call={
                "msg_type": "adv", 
                "action_code": "set", 
                "property": tm_oet.PROPERTY_STATUS, 
                "value": self.oet_model.to_dict(), 
                "message": "OET status update"
            })
        return tm_adv

    def add_observation(self, obs: ObsModel) -> None:
        """Add an observation to the OET store."""
        self.oet_model.add_obs(obs)
        
    def get_observations(self) -> list[ObsModel]:
        """Get the list of observations in the OET store."""
        return self.oet_model.processing_obs

    def get_observation(self, obs_id: str) -> ObsModel | None:
        """Get an observation by its ID."""
        for obs in self.oet_model.processing_obs:
            if obs.obs_id == obs_id:
                return obs
        return None

    def exec_observation(self, obs_id: str) -> bool:
        """Execute an observation by its ID."""
        obs = self.get_observation(obs_id)
        if obs:
            
            print(f"Executing observation {obs_id}")
            return True
        else:
            print(f"Observation {obs_id} not found")
            return False

def main():
    oet = OET()
    oet.start()

    try:
        while True:
            # If there is a scan in the queue, process it, else sleep & continue 
            time.sleep(0.1)
                
    except KeyboardInterrupt:
        pass
    finally:
        oet.stop()

if __name__ == "__main__":
    main()
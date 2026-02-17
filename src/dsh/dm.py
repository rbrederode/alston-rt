import astropy.units as u
from astropy.coordinates import get_body
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
from astropy.time import Time
from datetime import datetime, timezone, timedelta
import logging
import pytest
from queue import Queue
import time
import threading

from api import tm_dm
from env.app import App
from ipc.message import APIMessage
from ipc.action import Action
from ipc.message import AppMessage
from ipc.tcp_client import TCPClient
from ipc.tcp_server import TCPServer
from models.comms import CommunicationStatus, InterfaceType
from dsh.drivers.driver import DishDriver
from dsh.drivers.md01.md01_driver import MD01Driver
from models.dsh import DishManagerModel, DriverType, PointingState, DishMode, Capability
from models.health import HealthState
from models.obs import Observation
from models.oda import ObsList, ScanStore
from models.target import TargetModel, PointingType
from util import log
from util.xbase import XBase, XStreamUnableToExtract, XSoftwareFailure

logger = logging.getLogger(__name__)

SIDEREAL_RATE_DEG_PER_SEC = 360.0 / 86164.1  # Sidereal rate in degrees per second (86164.1 seconds in a sidereal day)

# Dish Manager (DM)

class DM(App):
    """A class representing the Dish Manager."""

    dm_model = DishManagerModel(id="dm001")

    def __init__(self, app_name: str = "dm"):

        super().__init__(app_name=app_name, app_model = self.dm_model.app)

        # Telescope Manager interface
        self.tm_system = "tm"
        self.tm_api = tm_dm.TM_DM()
        # Telescope Manager TCP Server
        self.tm_endpoint = TCPServer(description=self.tm_system, queue=self.get_queue(), host=self.get_args().tm_host, port=self.get_args().tm_port)
        self.tm_endpoint.start()
        # Register Telescope Manager interface with the App
        self.register_interface(self.tm_system, self.tm_api, self.tm_endpoint, InterfaceType.APP_APP)
        # Set initial Telescope Manager connection status
        self.dm_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED

        # Interfaces to each respective dish need to be managed by the respective dish drivers
        self.dish_drivers = {}        # Dictionary to hold a dish driver for each dish
        self.dish_locks = {}          # Dictionary of threading locks, one per dish

    def add_args(self, arg_parser): 
        """ Specifies the Dish Manager's command line arguments.
        """
        super().add_args(arg_parser)

        arg_parser.add_argument("--tm_host", type=str, required=False, help="TCP host to listen on for Telescope Manager commands", default="localhost")
        arg_parser.add_argument("--tm_port", type=int, required=False, help="TCP port for Telescope Manager commands", default=50002)

    def _get_dish_lock(self, dsh_id: str) -> threading.RLock:
        """Get or create a threading lock for a specific dish."""
        if dsh_id not in self.dish_locks:
            self.dish_locks[dsh_id] = threading.RLock()
        return self.dish_locks[dsh_id]

    def process_init(self) -> Action:
        """ Processes initialisation event on startup once all app processors are running.
            Runs in single threaded mode and switches to multi-threading mode after this method completes.
        """
        logger.debug(f"DM initialisation event")

        action = Action()

        # Load Dish configuration from disk, config file is located in ./config/<profile>/<model>.json
        # <profile> can be specified as a cmd line argument (provided by the App base class) default='default'
        # Config file defines initial list of dishes to be processed by the DM
        input_dir = f"./config/{self.get_args().profile}"
        filename = "DishList.json"

        try:
            dish_store = self.dm_model.dish_store.load_from_disk(input_dir=input_dir, filename=filename)
        except FileNotFoundError:
            dish_store = None
            logger.warning(f"DM could not load Dish configuration from directory {input_dir} file {filename}")
            
        if dish_store is None:
            logger.error(f"DM initialisation did not find any configured dishes.")
            return action

        self.dm_model.dish_store = dish_store
        logger.info(f"DM loaded Dish configuration from directory {input_dir} file {filename}")

        # Instantiate drivers for each dish and initiate a polling driver timer for each dish
        for dish in self.dm_model.dish_store.dish_list:
            driver_type = dish.driver_type.name
            if driver_type == DriverType.MD01.name:
                driver = MD01Driver(dsh_model=dish)
                self.dish_drivers[dish.dsh_id] = driver

                # Start the polling driver timer for this dish
                action.set_timer_action(Action.Timer(
                    name=f"driver_timer_{dish.dsh_id}_{type(driver).__name__}", 
                    timer_action=driver.get_poll_interval_ms())) 

                logger.info(f"DM instantiated MD01 driver for Dish {dish.dsh_id}")
            else:
                logger.warning(f"DM cannot instantiate driver for Dish {dish.dsh_id} with unknown driver type {driver_type}")

        return action

    def process_tm_connected(self, event) -> Action:
        """ Processes Telescope Manager connected events.
        """
        logger.info(f"DM connected to Telescope Manager: {event.remote_addr}")
        self.dm_model.tm_connected = CommunicationStatus.ESTABLISHED
        
        action = Action()

        # For each dish driver, try to set the dish to STANDBY mode
        for dish_id, dish_driver in self.dish_drivers.items():

            # If the dish does not have an operational capability, skip setting to STANDBY
            if dish_driver.get_capability() not in [Capability.OPERATE_FULL, Capability.OPERATE_DEGRADED]:
                continue

            # If the dish cannot auto-transition to STANDBY_FP from its current mode, skip setting to STANDBY
            if dish_driver.get_mode() not in [DishMode.STARTUP, DishMode.STOW]:
                continue

            dish_lock = self._get_dish_lock(dish_id)
            with dish_lock:
                try:
                    dish_driver.set_dish_mode(DishMode.STANDBY_FP)
                except XBase as e:
                    logger.error(f"DM failed to set STANDBY_FP mode for Dish {dish_id} on TM connect: {e}")
        
        # Send initial status advice message to Telescope Manager
        # Informs TM of current DM status including dish statuses
        tm_adv = self._construct_status_adv_to_tm()
        action.set_msg_to_remote(tm_adv)
        return action

    def process_tm_disconnected(self, event) -> Action:
        """ Processes Telescope Manager disconnected events.
        """
        logger.info(f"DM disconnected from Telescope Manager: {event.remote_addr}")
        self.dm_model.tm_connected = CommunicationStatus.NOT_ESTABLISHED
        
        action = Action()

        # For each dish driver, set the dish to STOW mode for safety if not already in STOW
        for dish_id, dish_driver in self.dish_drivers.items():

            # If the dish does not have an operational capability, skip setting to STOW
            if dish_driver.get_capability() not in [Capability.OPERATE_FULL, Capability.OPERATE_DEGRADED]:
                continue

            # If the dish cannot auto-transition to STOW from its current mode, skip setting to STOW
            if dish_driver.get_mode() not in [DishMode.STANDBY_LP, DishMode.STANDBY_FP, DishMode.CONFIG, DishMode.OPERATE, DishMode.UNKNOWN]:
                continue

            dish_lock = self._get_dish_lock(dish_id)
            with dish_lock:
                try:
                    dish_driver.set_dish_mode(DishMode.STOW)
                except XBase as e:
                    logger.error(f"DM failed to set STOW mode for Dish {dish_id} on TM disconnect: {e}")
                    
        return action

    def process_tm_msg(self, event, api_msg: dict, api_call: dict, payload: bytearray) -> Action:
        """ Processes api messages received on the Telescope Manager service access point (SAP)
            API messages are already translated and validated before being passed to this method.
        """
        logger.info(f"DM received Telescope Manager {api_call['msg_type']} message with action code: {api_call['action_code']}")

        dish_id = api_msg.get('entity', None)
        dish_driver = self.dish_drivers.get(dish_id, None) if dish_id is not None else None
        dish_lock = self._get_dish_lock(dish_id) if dish_id is not None else None

        action = Action()

        # Validate that we have a valid dish driver and lock for the requested dish id
        if dish_driver is None or dish_lock is None:
            msg = f"DM processing event for dish id {dish_id} without valid driver instance and driver lock"
            logger.error(msg + f"\n{api_call}")
            rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_ERROR, message=msg, api_msg=api_msg, api_call=api_call)
            action.set_msg_to_remote(rsp_msg)
            return action

        # If the Telescope Manager API call is to set the dish mode
        if api_call.get('action_code','') == 'set' and api_call.get('property','') == tm_dm.PROPERTY_MODE:

            mode = api_call.get('value', None)
            mode = DishMode(mode) if mode is not None else None
            
            # Prevent concurrent access to the dish driver
            with dish_lock:
                try:
                    dish_driver.set_dish_mode(mode) # Handles invalid or None mode internally
                except XBase as e:
                    msg = f"DM failed to set mode {mode.name if mode is not None else 'None'} for Dish {dish_id}: {e}"
                    logger.error(msg)
                    rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_ERROR, message=msg, api_msg=api_msg, api_call=api_call)
                    action.set_msg_to_remote(rsp_msg)
                    return action

            msg = f"DM successfully set mode {mode} for Dish {dish_id}."
            rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_SUCCESS, message=msg, api_msg=api_msg, api_call=api_call)
            action.set_msg_to_remote(rsp_msg)
            return action

        # If the Telescope Manager API call is to set the dish capability state
        if api_call.get('action_code','') == 'set' and api_call.get('property','') == tm_dm.PROPERTY_CAPABILITY:

            capability = api_call.get('value', None)
            capability = Capability(capability) if capability is not None else None

            # Prevent concurrent access to the dish driver
            with dish_lock:
                try:
                    dish_driver.set_dish_capability(capability) # Handles invalid or None capability internally
                except XBase as e:
                    msg = f"DM failed to set capability {capability.name if capability is not None else 'None'} for Dish {dish_id}: {e}"
                    logger.error(msg)
                    rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_ERROR, message=msg, api_msg=api_msg, api_call=api_call)
                    action.set_msg_to_remote(rsp_msg)
                    return action

            msg = f"DM successfully set capability {capability} for Dish {dish_id}."
            rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_SUCCESS, message=msg, api_msg=api_msg, api_call=api_call)
            action.set_msg_to_remote(rsp_msg)
            return action
  
        # If the Telescope Manager API call is to set a new target for the dish
        if api_call.get('action_code','') == 'set' and api_call.get('property','') == tm_dm.PROPERTY_TARGET:

            # Retrieve the target model and unique target identifier from the API call
            target = TargetModel.from_dict(api_call['value']) if isinstance(api_call.get('value'), dict) else None
            target_id = target.obs_id + f"_{target.tgt_idx}" if target is not None else None

            # Prevent concurrent access to the dish driver
            with dish_lock:
                try:
                    # If no target is provided, clear the current target and set dish to STANDBY mode
                    if target is None or target_id is None:
                        dish_driver.clear_target_tuple()
                        dish_driver.set_dish_mode(DishMode.STANDBY_FP)

                    # Else if a valid target is provided, set the new target and set dish to OPERATE mode (it will initiate slewing) 
                    elif target is not None and target_id is not None:
                        dish_driver.set_target_tuple(target_id, target)
                        dish_driver.set_dish_mode(DishMode.OPERATE)

                    else:
                        raise XSoftwareFailure(f"Invalid target provided to set for dish {dish_id}\n{api_call}")

                except XBase as e:
                    msg = f"DM failed to set target id {target_id if target_id is not None else 'None'} in observation " \
                     f"{target.obs_id if target is not None else 'None' } for Dish {dish_id}: {e}"

                    logger.error(msg + f"\n{target.to_dict() if target is not None else 'No Target'}")
                    rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_ERROR, message=msg, api_msg=api_msg, api_call=api_call)
                    action.set_msg_to_remote(rsp_msg)
                    dish_driver.clear_target_tuple()
                    return action

            msg = f"DM set target {target_id if target_id is not None else 'None'} for Dish {dish_id}."
            logger.info(msg + f"\n{target.to_dict() if target is not None else 'No Target'}")
            rsp_msg = self._construct_rsp_to_tm(status=tm_dm.STATUS_SUCCESS, message=msg, api_msg=api_msg, api_call=api_call)            
            action.set_msg_to_remote(rsp_msg)

        return action

    def process_timer_event(self, event) -> Action:
        """ Processes timer events.
        """
        logger.debug(f"DM timer event: {event}")

        action = Action()

        # Handle a driver timer e.g. driver_timer_dsh001_MD01Driver
        if "driver_timer" in event.name:

            # Extract dish id from the timer event name
            dish_id = event.name.split("_")[2]
            dish_driver = self.dish_drivers.get(dish_id, None) if dish_id is not None else None
            dish_lock = self._get_dish_lock(dish_id) if dish_id is not None else None

            if dish_driver is None or dish_lock is None:
                raise XSoftwareFailure(f"DM driver timer event {event.name} for dish id {dish_id} without driver instance or lock\n{event}")

            with dish_lock:
                # Retrieve the target model and unique target identifier from the driver
                target_id, target = dish_driver.get_target_tuple()

                # Get latest AltAz from the dish driver called regardless of the current pointing state or dish mode
                try:
                    altaz = dish_driver.get_current_altaz()
                except XBase as e:
                    logger.error(f"DM failed to get current AltAz for Dish {dish_id}: {e}")
                    
                    # Review dish failure count to determine if action is needed
                    if self._exceed_failure_threshold(dish_driver, dish_id):
                        self._send_status_adv_to_tm(action, target_id, target)
                        
                        # Tone down the driver poll rate to once per minute to reduce log spam until the issue is resolved
                        action.set_timer_action(
                            Action.Timer(name=f"driver_timer_{dish_id}_{type(dish_driver).__name__}", 
                            timer_action=60000)) 
                        return action

                # If the dish pointing state transitioned to READY, it means we have reached the desired slew position
                # Pointing state would be SLEW if still slewing or TRACK if already tracking (if necessary)
                if target is not None and dish_driver.get_pointing_state() == PointingState.READY:
                    logger.info(f"DM reached slew target and now in READY state for target {target.id} acquisition in observation {target.obs_id} with Dish {dish_id}.")

                    # If we need to track the target, tell the driver to track to it
                    if target.pointing in [PointingType.SIDEREAL_TRACK, PointingType.NON_SIDEREAL_TRACK]:                         
                        try:
                            dish_driver.track()
                        except XBase as e:
                            logger.error(f"DM failed to track for Dish {dish_id} to target {target.id} in observation {target.obs_id}: {e}")
                    
                    self._send_status_adv_to_tm(action, target_id, target)

                elif target is not None and dish_driver.get_pointing_state() == PointingState.TRACK:                     
                    try:
                        dish_driver.track()  # Continue tracking the target
                    except XBase as e:
                        logger.error(f"DM failed to track for Dish {dish_id} to target {target.id} in observation {target.obs_id}: {e}")

        # Restart the driver timer for the dish    
        action.set_timer_action(Action.Timer(
            name=f"driver_timer_{dish_id}_{type(dish_driver).__name__}", 
            timer_action=dish_driver.get_poll_interval_ms())) 
       
        return action

    def _exceed_failure_threshold(self, dish_driver: DishDriver, dish_id: str) -> bool:
        """ Review the failure count of the dish driver. 
            :return: True if failure threshold exceeded and action is necessary, False otherwise.
        """
        threshold = 60000/dish_driver.get_poll_interval_ms()
        failure_count = dish_driver.get_failure_count()

        if failure_count > 3 and failure_count < 10:
            logger.error(f"DM detected sporadic failures {failure_count} getting current AltAz for Dish {dish_id}." + \
                 f" Consider investigating dish driver.")
        elif failure_count >= 10 and failure_count < threshold:
            logger.error(f"DM detected persistent failures {failure_count} getting current AltAz for Dish {dish_id}." + \
                f" Consider investigating dish driver.")
        elif failure_count >= threshold:
            logger.error(f"DM detected unacceptably high failures {failure_count} getting current AltAz for Dish {dish_id}.")
            return True
        return False

    def process_status_event(self, event) -> Action:
        """ Processes status update events.
        """
        self.get_app_processor_state()

        action = self._send_status_adv_to_tm()
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

    def _send_status_adv_to_tm(self, action=None, target_id=None, target=None) -> Action:
        """ Sends a status advice message to the Telescope Manager if connected.
        """
        action = Action() if action is None else action

        if self.dm_model.tm_connected == CommunicationStatus.ESTABLISHED:

            tm_adv = self._construct_status_adv_to_tm()

            # Setting the Obs ID will trigger the Observation Execution Tool to review the observation state
            if target is not None and target_id is not None:
                api_call = tm_adv.get_api_call()
                api_call['obs_data'] = {'obs_id': target.obs_id, 'target_id': target_id}

            action.set_msg_to_remote(tm_adv)
            
        return action

    def _construct_rsp_to_tm(self, status, message, api_msg: dict, api_call: dict) -> APIMessage:
        """ Constructs a response message to the Telescope Manager.
        """
        # Prepare rsp msg to tm containing result of an api call
        tm_rsp = APIMessage(api_msg=api_msg, api_version=self.tm_api.get_api_version())

        tm_rsp.switch_from_to()
        tm_rsp_api_call = {
            "msg_type": "rsp", 
            "action_code": api_call['action_code'], 
            "status": status, 
        }
        if api_call.get('property') is not None:
            tm_rsp_api_call["property"] = api_call['property']

        if api_call.get('value') is not None:
            tm_rsp_api_call["value"] = api_call['value']

        # Only include obs_data if the status indicates an Error
        # This will trigger an OET workflow transition and review
        if status == tm_dm.STATUS_ERROR and api_call.get('obs_data') is not None:
            tm_rsp_api_call["obs_data"] = api_call['obs_data']

        if message is not None:
            tm_rsp_api_call["message"] = message

        tm_rsp.set_api_call(tm_rsp_api_call)  
        return tm_rsp

# Runs tests: pytest dsh/dm.py -v -s 
# -v for verbose output (or -vv or -vvv for more verbosity)
# -s to show print output

def test_get_desired_altaz(dm, md01_driver):
    
    # Test sidereal target
    target_sidereal = TargetModel(
        id="sidereal001",
        pointing=PointingType.SIDEREAL_TRACK,
        sky_coord=SkyCoord(ra=180.0*u.deg, dec=45.0*u.deg, frame='icrs')
    )
    altaz_sidereal = dm._get_desired_altaz(target_sidereal, md01_driver)
    print(altaz_sidereal)
    assert hasattr(altaz_sidereal, 'alt') and hasattr(altaz_sidereal, 'az')
 
    # Test non-sidereal target
    target_nonsidereal = TargetModel(
        id="mars",
        pointing=PointingType.NON_SIDEREAL_TRACK
    )
    altaz_nonsidereal = dm._get_desired_altaz(target_nonsidereal, md01_driver)
    print(altaz_nonsidereal)
    assert hasattr(altaz_nonsidereal, 'alt') and hasattr(altaz_nonsidereal, 'az')
 
    # Test drift scan target
    target_drift = TargetModel(
        id="drift001",
        pointing=PointingType.DRIFT_SCAN,
        altaz=AltAz(alt=30.0*u.deg, az=150.0*u.deg)
    )
    altaz_drift = dm._get_desired_altaz(target_drift, md01_driver)
    print(altaz_drift)
    assert hasattr(altaz_drift, 'alt') and hasattr(altaz_drift, 'az')      

def main():
    dm = DM()
    dm.start()

    try:
        while True:
            # Main thread does nothing currently apart from sleeping
            # All processing is in the DM app processor thread
            time.sleep(0.1)
                
    except KeyboardInterrupt:
        pass
    finally:
        dm.stop()

if __name__ == "__main__":
    main()
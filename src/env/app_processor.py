from env.processor import Processor
from env.events import InitEvent, StatusUpdateEvent
from queue import Queue, Empty
from api.api import API
from ipc.tcp_server import TCPServer
from ipc.message import AppMessage, APIMessage
from ipc.action import Action
from util.xbase import XBase
from util.timer import Timer, TimerManager
from env import events

import logging
logger = logging.getLogger(__name__)

class AppProcessor(Processor):

    def __init__(self, name=None, event_q=None, driver=None):
        super().__init__(name=name, event_q=event_q)
        self.driver = driver

    def initialise(self):
        pass

    def initialise_app(self):

        Processor.single_thread()

        self.initialise()
        logger.debug(f"AppProcessor {self.name} initialised")

        Processor.free_thread()

    def process_status_update(self, event: StatusUpdateEvent):
        
        try:
            event.notify_dequeued()
            # Process status update here
        finally:
            event.notify_update_completed()
    
    def process_event(self, event) -> bool:
        logger.debug(f"AppProcessor {self.name} received event: {event}")

        try:
            if isinstance(event, InitEvent):

                try:
                    self.initialise_app()
                except Exception as e:
                    logger.error(f"AppProcessor: Error initialising app: {e}")

            elif isinstance(event, StatusUpdateEvent):

                try:
                    self.process_status_update(event)
                except Exception as e:
                    logger.error(f"AppProcessor: Error processing status update event {event}: {e}")

            elif isinstance(event, events.TimerEvent):

                if event.timer_cancelled:
                    logger.debug(f"AppProcessor {self.name} ignoring a cancelled timer event: {event}")
                    return True

                if event.user_callback is not None:
                    logger.debug(f"AppProcessor {self.name} received timer event with callback: {event}")
                    try:
                        event.user_callback(event.user_ref)
                    except Exception as e:
                        logger.error(f"AppProcessor {self.name} error in user callback for timer event {event}: {e}")
                        return False

                handler_method = "process_timer_event"

                if hasattr(self.driver, handler_method) and callable(getattr(self.driver, handler_method)):
                    self.performActions(getattr(self.driver, handler_method)(event))
                return True

            elif isinstance(event, events.DataEvent):
            
                api_msg = APIMessage()

                try:
                    api_msg.from_data(event.data)
                    api_msg.add_echo_api_header()

                    api, endpoint = self.driver.get_interface(api_msg.get_from_system())

                    api_transl_msg = api.translate(api_msg.get_json_api_header())
                    api.validate(api_transl_msg)

                    if api_msg.get_to_system() != self.driver.app_name:
                        rsp_msg = APIMessage(api_msg.get_json_api_header())
                        api_call = {
                            "action_code": "decline",
                            "reason": f"Message not intended for dig, but for {api_msg.get_to_system()}"
                        }
                        rsp_msg.set_api_call(api_call)
                        logger.warning(f"AppProcessor {self.name} received API message intended for {api_msg.get_to_system()} i.e. not for this App {rsp_msg}")
                        self.performActions(Action().set_msg_to_remote(rsp_msg))
                        return True

                except XBase as e:
                    logger.error(f"AppProcessor {self.name} failed to process data event from Service Access Point {event.local_sap.description}: {e}")
                    return False

                handler_method = "process_" + api_msg.get_from_system() + "_msg"
                if hasattr(self.driver, handler_method) and callable(getattr(self.driver, handler_method)):
                    self.performActions(getattr(self.driver, handler_method)(
                        event,
                        api_msg.get_json_api_header(),
                        api_msg.get_api_call()
                    ))
                return True

            if isinstance(event, events.ConnectEvent):

                handler_method = "process_" + event.local_sap.description + "_connected"

                if hasattr(self.driver, handler_method) and callable(getattr(self.driver, handler_method)):
                    self.performActions(getattr(self.driver, handler_method)(event))
                return True
                
            if isinstance(event, events.DisconnectEvent):
                handler_method = "process_" + event.local_sap.description + "_disconnected"

                if hasattr(self.driver, handler_method) and callable(getattr(self.driver, handler_method)):
                    self.performActions(getattr(self.driver, handler_method)(event))
                return True

            else:
                return False  # Event not processed

        finally:
            pass

        return True

    def performActions(self, action: Action):
        """Performs the actions specified in the Action object.
            Remove actions from the Action object once performed.
            Leave actions in the Action object if they could not be performed.
            : param action: The Action object containing the actions to perform
            Call the superclass method at the end to process any remaining actions.
        """

        # if no actions to perform, return
        if action is None:
            return

        logger.debug(f"AppProcessor {self.name} performing actions: {action}")

        # Perform message actions
        for msg in action.msgs_to_remote[:]:    # Iterate over a copy [:] of the list to allow removal during iteration

            logger.debug(f"AppProcessor {self.name} performing action: send message to remote: {msg}")

            if isinstance(msg, APIMessage):

                dest_system = msg.get_to_system()
                api, endpoint = self.driver.get_interface(dest_system)

                msg_to_send = msg

                try:
                    api.validate(msg.get_json_api_header())
                    api_header = msg.get_echo_api_header()
                    
                    if api_header is not None:
                        orig_version = api_header.get('api_version', api.get_api_version())
                        msg.remove_echo_api_header()
                        api_transl_msg = api.translate(api_msg=msg.get_json_api_header(), target_version=orig_version)

                        msg_to_send = APIMessage(api_msg=api_transl_msg, payload=msg.get_payload_data())
                    
                    endpoint.send(msg_to_send)  # Send the message to the remote endpoint
                    
                except XBase as e:
                    logger.error(f"AppProcessor {self.name} failed to perform action 'send message to remote' because validate/translate of API message failed: {e} Message: {msg}")
                    continue

                action.msgs_to_remote.remove(msg)  # Remove the msg from the list

            else:
                logger.error(f"AppProcessor {self.name} failed to perform action 'send message to remote' because message is not an APIMessage instance: {msg}")
        
        # Perform timer actions
        for timer in action.timer_actions[:]:  # Iterate over a copy [:] of the list to allow removal during iteration

            logger.debug(f"AppProcessor {self.name} performing action: set timer: {timer}")

            if isinstance(timer, Action.Timer):

                timers = Timer.manager.get_timers_by_name(timer.name)

                for t in timers:
                    logger.debug(f"AppProcessor {self.name} cancelling existing timer: {t}")
                    t.cancel()

                if timer.get_timer_action() != Action.Timer.TIMER_STOP:

                    new_timer = Timer(                      # Create a new timer
                        name=timer.get_name(), 
                        event_q=self.get_queue(), 
                        duration_ms=timer.get_timer_action(), 
                        user_ref=timer.get_echo_data())  

                    Timer.manager.add_timer(new_timer)

                    action.timer_actions.remove(timer)      # Remove the timer action from the list
                    logger.debug(f"AppProcessor {self.name} started new timer: {new_timer}")
                else:
                    logger.error(f"AppProcessor {self.name} unknown timer action: {timer.timer_action}")
            else:
                logger.error(f"AppProcessor {self.name} failed to perform timer action {timer} because it is not an Action.Timer instance")

        for conn_action in action.connection_actions[:]:  # Iterate over a copy [:] of the list to allow removal during iteration

            logger.debug(f"AppProcessor {self.name} performing action: set connection: {conn_action}")

            if isinstance(conn_action, Action.Connection):
                # Placeholder for actual connection handling logic
                action.connection_actions.remove(conn_action)  # Remove the connection action from the list
                logger.debug(f"AppProcessor {self.name} processed connection action: {conn_action}")
            else:
                logger.error(f"AppProcessor {self.name} failed to perform connection action {conn_action} because it is not an Action.Connection instance")

if __name__ == "__main__":
    import queue
    import time

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,  # Or DEBUG for more verbosity
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger(__name__)

    q = queue.Queue()

    test1 = AppProcessor(name="Test1", event_q=q)
    test2 = AppProcessor(name="Test2", event_q=q)
    test3 = AppProcessor(name="Test3", event_q=q)
    test4 = AppProcessor(name="Test4", event_q=q)

    test1.start()
    test2.start()
    test3.start()
    test4.start()

    status_update_event = StatusUpdateEvent()
    status_update_event.notify_queued()

    q.put(InitEvent("ComponentA"))
    q.put(status_update_event)

    time.sleep(1)  # Give threads time to start

    for i in range(300):
        q.put(f"Event {i}")

    time.sleep(2)  # Allow some time for processing

    Processor.stop_all()

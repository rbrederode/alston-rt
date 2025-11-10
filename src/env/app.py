import argparse
import threading
import datetime
import time
import asyncio

from queue import Queue, Empty
from api.api import API
from ipc.tcp_server import TCPServer
from ipc.message import AppMessage, APIMessage
from ipc.action import Action
from models.app import AppModel
from models.proc import ProcessorModel
from models.health import HealthState
from util.xbase import XBase
from util.timer import Timer, TimerManager
from env import events
from env.events import InitEvent, StatusUpdateEvent
from env.processor import Processor
from env.app_processor import AppProcessor

import logging
logger = logging.getLogger(__name__)

class App:

    def __init__(self, app_name: str, app_model: AppModel):

        if app_name is None or app_name.strip() == "":
            raise XBase("App requires a non-empty app name to initialise itself")

        self.app_model = app_model
        self.app_model.app_name = app_name
        self.app_model.app_running = True
        
        self.queue = Queue()                     # Event queue for the application
        self.status_update_event = events.StatusUpdateEvent()  # Reusable status update event
        
        self.interfaces = {}                    # Dictionary to hold registered App interfaces

        self.arg_parser = argparse.ArgumentParser(description=self.app_model.app_name)
        self.add_args(self.arg_parser)
        self.app_model.arguments = vars(self.get_args())

        # Set log level based on verbose argument
        if self.get_args().verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)

        self.app_model.num_processors = max(1, self.get_args().num_processors)
        self.processors = []                    # List to hold processor threads

        self.start_timer_manager()              # Ensure timer manager is started before any timers are created

        self.app_model.health = HealthState.UNKNOWN

    def __del__(self):
        self.stop()

    def get_args(self):
        return self.arg_parser.parse_args()

    def get_queue(self):
        return self.queue

    def get_name(self):
        return self.app_model.app_name

    def get_arg_parser(self):
        return self.arg_parser

    def add_args(self, arg_parser):
        """Specifies the application's command line arguments.
            Subclasses should override this method to add their own arguments.
            Call the superclass method first to ensure base arguments are added.
        """
        arg_parser.add_argument("--verbose", "-v",action="store_true", help="Enable verbose logging")
        arg_parser.add_argument("--num_processors", "-np", type=int, required=False, help="Number of processor threads to create", default=4)

    def start(self):
        """Starts the application."""

        self.queue.put(InitEvent(self.app_model.app_name))  # Start with an initialisation event
        self.start_processors()
        self.start_status_thread()

        logger.info(f"App {self.app_model.app_name} started")

    def run(self):
        
        while self.app_model.app_running:
            try:
                logger.info(f"App {self.app_model.app_name} status thread checking status update event {self.status_update_event}")

                if self.status_update_event.is_update_pending():
                    if self.status_update_event.get_dequeued_count() == 0:
                        # First status update event is still enqueued, so initialisation 
                        # has not completed yet. Skip this status update.
                        pass
                    else:
                        # An update is still pending, and we know initialisation has completed
                        # (since the dequeued count is > 0), so alert the user
                        ms_since_update_queued = self.status_update_event.get_millis_since_update_enqueued()

                        debug_info = []
                        debug_info.append("-"*40+"\n")
                        debug_info.append(f"App {self.app_model.app_name} Debug Info\n")
                        debug_info.append("-"*40+"\n")

                        debug_info.append(f"Queue size is {self.queue.qsize()}\n")
                        debug_info.append(f"Status update event has been pending for {ms_since_update_queued} ms\n")
                        debug_info.append(f"Status update event dequeued count is {self.status_update_event.get_dequeued_count()}\n")
                        debug_info.append(f"Status update event currently being processed {self.status_update_event.is_being_processed()}\n")
                        debug_info.append(f"Number of processors: {len(self.processors)}\n")

                        for processor in self.processors:

                            debug_info.append(f"Processor {processor.name} current event: {processor.get_current_event()}\n")
                            debug_info.append(f"Processor {processor.name} elapsed processing time: {processor.get_current_event_processing_time()} ms\n")

                        debug_info.append("-"*40+"\n")

                        logger.info(f"App {self.app_model.app_name} Debug Info:\n{''.join(debug_info)}")
                else:
                    self.status_update_event.enqueue(self.queue)
                    
                time.sleep(30)  # Sleep briefly to avoid busy-waiting

            except Exception as e:
                logger.error(f"App {self.app_model.app_name} encountered an error: {e}")
    
    def stop(self):
        """Stops the application."""

        self.app_modelapp_running = False # Stops status thread
        self.stop_timer_manager()
        self.stop_processors()

        if not self.queue.empty():
            self.queue.queue.clear()

        logger.info(f"App {self.app_model.app_name} stopped")
        self.app_model.health = HealthState.UNKNOWN

    def start_processors(self):
        """Starts all processor threads."""

        for i in range(self.app_model.num_processors):
            processor = AppProcessor(name=f"{self.app_model.app_name}-Processor-{i+1}", event_q=self.queue, driver=self)
            self.processors.append(processor)
            processor.start()

    def stop_processors(self):
        """Stops all processor threads."""
        Processor.stop_all()
        self.processors = []

        self.app_model.health = HealthState.UNKNOWN

    def start_status_thread(self):
        """Starts a thread to periodically enqueue status update events."""
        thread = threading.Thread(target=self.run, name=f"{self.app_model.app_name}-StatusThread", daemon=True)
        thread.start()
        logger.info(f"App {self.app_model.app_name} started status thread")

    def start_timer_manager(self):
        """Starts the timer manager if not already running."""
        if Timer.manager is None:
            Timer.manager = TimerManager()
            Timer.manager.start()
            logger.info(f"App {self.app_model.app_name} started timer manager")

    def stop_timer_manager(self):
        """Stops the timer manager if running."""
        if Timer.manager is not None:
            Timer.manager.stop()
            Timer.manager = None
            logger.info(f"App {self.app_model.app_name} stopped timer manager")
        
    def register_interface(self, system_name: str, api: API, endpoint):
        """Registers an interface with the application.
            : param system_name: The name of the system the interface is for
            : param api: The API implementation for the interface
            : param endpoint: The endpoint (e.g. TCPServer or TCPClient) for the interface
        """

        if system_name is None or system_name.strip() == "":
            raise XBase("App {self.app_model.app_name} system name must be a non-empty string")

        if api is None:
            raise XBase("App {self.app_model.app_name} API implementation must be provided")

        if endpoint is None:
            raise XBase("App {self.app_model.app_name} endpoint must be provided")

        logger.info(f"App {self.app_model.app_name} registered interface for system '{system_name}' with API version {api.get_api_version()} at endpoint {endpoint}")

        self.interfaces[system_name] = (api, endpoint)
        self.app_model.interfaces.append(system_name)

    def deregister_interface(self, system_name: str):
        """Deregisters an interface from the application.
            : param system_name: The name of the system the interface is for
        """
        if system_name in self.interfaces:
            self.app_model.interfaces.remove(system_name)
            del self.interfaces[system_name]

            logger.info(f"App {self.app_model.app_name} deregistered interface for system '{system_name}'")
        else:
            logger.warning(f"App {self.app_model.app_name} could not find interface for system '{system_name}' to deregister")

    def get_interface(self, system_name: str):
        """Gets the interface for a given system name.
            : param system_name: The name of the system the interface is for
            : return: The API and endpoint if found, else None
        """
        if system_name not in self.interfaces:
            raise XBase(f"App {self.app_model.app_name} has no registered interface for system '{system_name}'")

        return self.interfaces.get(system_name, None)

    def get_app_processor_state(self) -> dict:
        """Updates the app(lication) model with the current state of its processors.
            : return: A dictionary representation of the updated AppModel instance
        """
        processors = []
        for processor in self.processors:
            # Get the current event and convert it to a JSON-serializable form.
            ev = processor.get_current_event()
            if ev is None:
                ev_repr = None
            else:
                # Prefer a structured representation if the event exposes it,
                # otherwise fall back to a short string.
                try:
                    if hasattr(ev, "to_dict") and callable(ev.to_dict):
                        ev_repr = ev.to_dict()
                    else:
                        # Use a concise string representation to avoid embedding
                        # large objects or types that aren't JSON serializable.
                        ev_repr = str(ev)
                except Exception:
                    ev_repr = repr(ev)

            proc_model = ProcessorModel(
                name=processor.name,
                current_event=ev_repr,
                processing_time_ms=processor.get_current_event_processing_time()
            )
            processors.append(proc_model)

        self.app_model.queue_size = self.queue.qsize()
        self.app_model.processors = processors
        self.app_model.last_update = datetime.datetime.now(datetime.timezone.utc)

        return self.app_model.to_dict()

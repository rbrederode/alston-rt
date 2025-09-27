# -*- coding: utf-8 -*-

import time

from queue import Queue
from datetime import datetime

import logging
logger = logging.getLogger(__name__)

class ConnectEvent:

    def __init__(self, local_sap, remote_conn, remote_addr, timestamp=None):
        """Initialize the connect event with the given parameters.
        
            Parameters
                local_sap: local service access point (sap) (e.g. TCPServer or TCPClient) on which the event occurred
                remote_conn: remote socket connection object associated with the event
                remote_addr: remote address associated with the event
                timestamp: Timestamp of the event
        """

        self.local_sap = local_sap  
        self.remote_conn = remote_conn  
        self.remote_addr = remote_addr 
        self.timestamp = timestamp

    def __str__(self):
        """
        Returns a human-readable string representation of this event.
        """
        return f"ConnectEvent@{self.timestamp} - {self.local_sap.description} connected to {self.remote_addr}"

class DisconnectEvent:

    def __init__(self, local_sap, remote_conn, remote_addr, timestamp=None):
        """Initialize the disconnect event with the given parameters.
        
            Parameters
                local_sap: local service access point (sap) (e.g. TCPServer or TCPClient) on which the event occurred
                remote_conn: remote socket connection object associated with the event
                remote_addr: remote address associated with the event
                timestamp: Timestamp of the event
        """

        self.local_sap = local_sap  
        self.remote_conn = remote_conn  
        self.remote_addr = remote_addr 
        self.timestamp = timestamp

    def __str__(self):
        """
        Returns a human-readable string representation of this event.
        """
        return f"DisconnectEvent@{self.timestamp} - {self.local_sap.description} disconnected from {self.remote_addr}"

class DataEvent:

    def __init__(self, local_sap, remote_conn, remote_addr, data, timestamp=None):
        """Initialize the disconnect event with the given parameters.
    
        Parameters
            local_sap: local service access point (sap) (e.g. TCPServer or TCPClient) on which the event occurred
            remote_conn: remote socket connection object associated with the event
            remote_addr: remote address associated with the event
            data: bytes received from the remote end point
            timestamp: Timestamp of the event
        """

        self.local_sap = local_sap  
        self.remote_conn = remote_conn  
        self.remote_addr = remote_addr 
        self.data = data
        self.timestamp = timestamp

    def __str__(self):
        """
        Returns a human-readable string representation of this event.
        """
        if self.data is None:
            return f"DataEvent@{self.timestamp} - {self.local_sap.description} received NO data from {self.remote_addr}\n"
        else:
            hex_data = ' '.join(self.data[i:i+1].hex() for i, val in enumerate(self.data))
            return f"DataEvent@{self.timestamp} - {self.local_sap.description} received data from {self.remote_addr}\n" + \
                f"Data (ascii): {self.data}\n" + \
                f"Data (hex):   {hex_data}\n"

class TimerEvent:

    def __init__(self, id, name=None, user_ref=None, user_callback=None, timestamp=None):
        """Initialize the timer event with the given parameters.

        Parameters
            user_callback: The callback function to be called when the timer expires
            user_ref: A reference specified by the user when the timer was created
            timestamp: Timestamp of the event 
        """
        self.id = id
        self.name = name if name is not None else id
        self.user_ref = user_ref
        self.user_callback = user_callback
        self.timestamp = timestamp
        self.timer_cancelled = False

    def __str__(self):
        """
        Returns a human-readable string representation of this event.
        """
        return f"TimerEvent@{self.timestamp} - name {self.name}, user ref {self.user_ref} user callback {self.user_callback}, cancelled={self.timer_cancelled}"

    def cancel(self) -> bool:
        """
        Cancels the timer event.
        """
        self.timer_cancelled = True
        return True

class InitEvent:
    """ Event indicating that an environment component has been initialised and is ready to operate.
    """
    def __init__(self, component_name: str, timestamp=None):
        self.component_name = component_name
        self.timestamp = timestamp

    def __str__(self):
        return f"InitEvent@{self.timestamp} - Component {self.component_name}"

class StatusUpdateEvent:

    STATUS_ENQUEUED = 0
    STATUS_PROCESSING = 1
    STATUS_DEQUEUED = 2

    _10_MIN = 10 * 60 * 1000 # 10 minutes in milliseconds
    _DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f %Z"

    """ Event indicating a status update from an environment component.
    """
    def __init__(self):
        self.current_status = StatusUpdateEvent.STATUS_DEQUEUED

        self.total_processing_time_ms = 0
        self.total_processing_count = 0 

        self.enqueue_time = None
        self.dequeue_time = None
        self.update_time = None

    def enqueue(self, event_q:Queue):
        self.enqueue_time = time.time()
        self.current_status = StatusUpdateEvent.STATUS_ENQUEUED
        event_q.put(self)

    def notify_dequeued(self):
        self.current_status = StatusUpdateEvent.STATUS_PROCESSING
        self.dequeue_time = time.time()
        self.total_processing_time_ms += self.dequeue_time - self.enqueue_time
        self.total_processing_count += 1

    def notify_update_completed(self):
        self.updated_time = time.time()
        self.current_status = StatusUpdateEvent.STATUS_DEQUEUED
        
    def get_dequeued_count(self):
        return self.total_processing_count

    def is_being_processed(self) -> bool:
        return self.current_status == StatusUpdateEvent.STATUS_PROCESSING

    def is_update_pending(self) -> bool:
        return self.current_status != StatusUpdateEvent.STATUS_DEQUEUED

    def get_average_processing_time(self) -> float:
        if self.total_processing_count == 0:
            return 0.0
        return self.total_processing_time_ms / self.total_processing_count  

    def get_millis_since_update_enqueued(self) -> float:
        return (time.time() - self.enqueue_time)

    def get_total_processing_time(self) -> float:
        return self.total_processing_time_ms

    def __str__(self):

        enqueue_str = "[None]"
        dequeue_str = "[None]"
        update_str = "[None]"

        status_str = "UNKNOWN"

        if self.current_status == StatusUpdateEvent.STATUS_ENQUEUED:
            status_str = "ENQUEUED"
        elif self.current_status == StatusUpdateEvent.STATUS_PROCESSING:
            status_str = "BEING PROCESSED"
        elif self.current_status == StatusUpdateEvent.STATUS_DEQUEUED:
            status_str = "DEQUEUED"

        if self.enqueue_time is not None:
            enqueue_str = datetime.fromtimestamp(self.enqueue_time).strftime(self._DATE_FORMAT)

        if self.dequeue_time is not None:
            dequeue_str = datetime.fromtimestamp(self.dequeue_time).strftime(self._DATE_FORMAT)

        if self.update_time is not None:
            update_str = datetime.fromtimestamp(self.update_time).strftime(self._DATE_FORMAT)

        return f"StatusUpdateEvent (" + \
            f"Enqueued Timestamp={enqueue_str}, " + \
            f"Dequeued Timestamp={dequeue_str}, " + \
            f"Updated Timestamp={update_str}, " + \
            f"Current Status={status_str}, " + \
            f"Total Processing Count={self.total_processing_count}, " + \
            f"Total Processing Time (ms)={self.total_processing_time_ms}, " + \
            f"Average Processing Time (ms)={self.get_average_processing_time():.2f})"
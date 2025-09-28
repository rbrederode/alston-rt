#!/usr/bin/env python3

import selectors
import socket
import errno
import os
import sys
import threading
import time
import struct
import traceback
import json
from queue import Queue
from datetime import datetime, timezone

from ipc import message
from env import events
from env.app_processor import AppProcessor
from util.timer import Timer, TimerManager
from util.xbase import XSoftwareFailure

import logging
logger = logging.getLogger(__name__)

DEST_IP = socket.gethostbyname(socket.gethostname())
DEST_PORT = 50000

MAX_BLOCK_SIZE = 65535   # Define a maximum block size for sending data

class TCPClient:
    """TCP Client class to create connections and send data to/from a server using IPv4.
        It runs in non-blocking mode and processes events in its own daemon thread.
        Events (connected, disconnected, data received) are added to a queue
        for further processing by the calling process. """

    def __init__(self, description="TCP Client", queue=None, host=DEST_IP, port=DEST_PORT, max_block_size=MAX_BLOCK_SIZE):
        """Initialize the TCP client with the given host and port.

            Parameters
                description: Description of the client
                queue: Queue to keep track of events
                host: Destination IP address
                port: Port number """
    
        self.description = description
        self.host = host
        self.port = port
        self.sel = selectors.DefaultSelector()
        
        # AF_INET: IPv4, SOCK_STREAM: TCP
        self.client_socket = None
        self._create_socket()

        self.started = True     # Flag to indicate if the client daemon thread is running
        self.connected = False  # Flag to indicate if the client is connected to a server

        # Create & start a thread to handle events, set it as a daemon thread (killed when the main thread exits)
        self.event_handler = threading.Thread(target=self._process_events)
        self.event_handler.daemon = True 
        self.event_handler.start()
        self.event_q = queue if queue else Queue() # Queue to keep track of events    
        self.max_block_size = max_block_size if max_block_size > 0 else MAX_BLOCK_SIZE
        self.last_result = -1  # Last result code from connect_ex()

        self._connect_lock = threading.Lock()   # Lock to ensure thread-safe connect attempts
        self._send_lock = threading.Lock()      # Lock to ensure thread-safe sending of messages

    def _create_socket(self):
        """Create a new socket and register it with the selector."""
        
        msg = message.Message() # Create a new (empty) message instance and associate it with the client socket

        self._destroy_socket()  # Ensure any existing socket is destroyed before creating a new one

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setblocking(False)
        self.sel.register(self.client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=msg)
        self.connected = False  # Set the client to not connected

    def _destroy_socket(self):
        """Destroy the current socket and unregister it from the selector."""
        
        if self.client_socket:
            try:
                self.sel.unregister(self.client_socket)
                self.client_socket.close()
            except Exception as e:
                logging.error(f"TCP Client {self.description} error closing socket: {e}")

            self.client_socket = None
            self.connected = False  # Set the client to not connected

    def _process_connection(self):
        """Accept incoming connection events from a client and register the connection with the selector."""

        event = events.ConnectEvent(self, self.client_socket, (self.host, self.port), datetime.now())
        # Add the event to the queue for further processing
        self.event_q.put(event)

        logging.info(f"TCP Client {self.description} connected to host {self.host} port {self.port}")

    def _process_disconnect(self):
        """Process a disconnect from a client and deregister the connection from the selector."""
        
        # Create a disconnect event and add it to the queue
        event = events.DisconnectEvent(self, self.client_socket, self.client_socket.getpeername(), datetime.now())
        self.event_q.put(event)

        # Unregister the connection from the selector
        self.sel.unregister(self.client_socket)
        self.client_socket.close()  # Close the socket connection
        self.connected = False  # Set the client to not connected

        logging.info(f"TCP Client {self.description} disconnected from host {self.host} port {self.port}")

        # Start a retry timer to attempt to reconnect after a delay
        self.retry_timer = Timer(f"TCPClient-{self.description}", self.event_q, 5000, user_callback=lambda x: self.connect())  # Retry every 5 seconds

    def _process_msg(self, msg):
        """Process incoming msg events from the server and assemble the msg body from the received data."""
        try:
            full_msg = b''
            remaining_blocks = 1

            while remaining_blocks > 0:

                # Step 1: Read a 4-byte header to get the 2-byte block size (0-65,535 bytes) and 2-byte remaining blocks (0-65,535 blocks)
                msg_header = self.recv_all(self.client_socket, 4)

                # Check if the connection has been closed i.e. zero bytes received
                if not msg_header or len(msg_header) < 4:  
                    logger.error(f"TCP Client {self.description} received incomplete header on {self.host} port {self.port} from {self.client_socket.getpeername()}\n" + \
                        f"Header (hex):\n{msg_header.hex() if msg_header else 'None'}\n")
                    self._process_disconnect()
                    return
                # Unpack the 4-byte big-endian header ('>HH' means two big-endian unsigned shorts)
                block_size, remaining_blocks = struct.unpack('>HH', msg_header)

                # Step 2:Read the block of data
                block = self.recv_all(self.client_socket, block_size)

                if not block or len(block) < block_size:  
                    logger.error(f"TCP Client {self.description} received incomplete block on {self.host} port {self.port} from {self.client_socket.getpeername()}\n" + \
                        f"Block size: {block_size}\nReceived size: {len(block) if block else 0}\nRemaining blocks: {remaining_blocks}\n")
                    self._process_disconnect()
                    return

                full_msg += block

            # Step 3: Process the received data stream as a message
            msg.from_data(full_msg)

            # Create a data event and add it to the queue
            event = events.DataEvent(self, self.client_socket, (self.host, self.port), full_msg, datetime.now())
            self.event_q.put(event)

            logging.info(f"TCP Client {self.description} received message from host {self.host} port {self.port}\n{msg}")

        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass
        except (ConnectionRefusedError) as e:
            logging.error(f"TCP Client {self.description} connection refused while receiving msg from {self.host} port {self.port} Data (hex): {full_msg.hex()}")
            self.connected = False
            return
        except Exception as e:
            logging.error(f"TCP Client {self.description} unhandled exception error while receiving msg from {self.host} port {self.port} Data (hex): {full_msg.hex()} Exception: {e}")
            self._destroy_socket()
            return

    def _process_events(self):
        """ Process events in a loop until the client is stopped. """
        
        # While the client has started, keep processing events
        while self.started:
                events = self.sel.select(timeout=1) # Wait for events with a timeout specified in seconds
                
                if self.connected: # Only process events if connected to a server
                    for key, mask in events:

                        # key.data is None for the client socket
                        if key.data is None:
                            raise XSoftwareFailure(f"TCP Client {self.description} no key data associated with the socket")
                        else:
                            try:
                                self._process_msg(key.data)
                            except Exception as e:
                                logging.error(f"TCP Client {self.description} unhandled exception while processing events for {self.host} port {self.port} Data (hex): {key.data.msg_data.hex() if key.data.msg_data else ''} Exception: {e}")

    def connect(self) -> int:
        """Establish a socket connection.
            Returns
                0 or EISCON if the connection was successful
                Error code if the connection failed"""

        with self._connect_lock:

            # Ensure a retry timer is started (if not running) to re-check the connection status every 5 seconds
            if Timer.manager is not None and not Timer.manager.get_timers_by_name(f"TCPClient-{self.description}"):
                self.retry_timer = Timer(f"TCPClient-{self.description}", self.event_q, 5000, user_callback=lambda x: self.connect()) 

            if self.connected:
                return self.last_result

            if self.client_socket is None or self.last_result in (errno.EBADF, errno.EINVAL) or self.client_socket.fileno() == -1: 
                logger.debug(f"TCP Client {self.description} socket is invalid, creating a new socket.")
                self._create_socket()

            logger.debug(f"TCP Client {self.description} attempting to connect to host {self.host} port {self.port}")

            self.last_result = self.client_socket.connect_ex((self.host, self.port)) # Attempt a connect to the server

            if self.last_result in (0, errno.EISCONN):  # Success (0) or socket already connected (EISCONN)
                self.connected = True  
                self._process_connection()
            elif self.last_result == errno.EINPROGRESS:  # Connection in progress or already in progress
                logger.debug(f"TCP Client {self.description} connection in progress to host {self.host} port {self.port}. Result code: {self.last_result}, {errno.errorcode.get(self.last_result)}, {os.strerror(self.last_result)}")
                time.sleep(1)  # Sleep briefly to allow the connection to complete
                self.last_result = self.client_socket.connect_ex((self.host, self.port)) # Re-attempt a connect to the server
                if self.last_result in (0, errno.EISCONN):  # Success (0) or socket already connected (EISCONN)
                    self.connected = True  
                    self._process_connection()
            else:
                self.connected = False

                if self.last_result in (errno.EBADF, errno.ECONNREFUSED):  # Bad file descriptor or connection refused
                    logging.error(f"TCP Client {self.description} socket is invalid, after attempting connect to host {self.host} port {self.port}. Recreating socket.")
                    self._create_socket()
                else:
                    logging.error(
                        f"TCP Client {self.description} failed to connect to host {self.host} port {self.port} "
                        f"with error code {self.last_result}, {errno.errorcode.get(self.last_result)}, {os.strerror(self.last_result)}"
                    )

            return self.last_result

    def send(self, msg: message.Message):
        """Send a message to the server"""

        with self._send_lock:  # Ensure that only one thread can send a message at a time

            time_enter = time.time()

            if not self.connected:
                logging.error(f"TCP Client {self.description} not connected to host {self.host} port {self.port}. Cannot send message.\n{msg}")
                return

            if not isinstance(msg, message.Message):
                logging.error(f"TCP Client {self.description} invalid message type. Expected message.Message, got {type(msg)}.\n{msg}")
                return

            if self.client_socket is None or self.client_socket.fileno() == -1:
                logging.error(f"TCP Client {self.description} socket is invalid. Cannot send message.\n{msg}")
                self.connected = False
                return

            # Iterate over all connections and send the message
            for key in list(self.sel.get_map().values()):
                if key.data is not None:
                    try:
                        logger.debug(f"TCP Client {self.description} sending message to {key.fileobj.getpeername()}\n{msg}")

                        data = msg.to_data()  # Convert the message to bytes 

                        if data is None:
                            raise ValueError(f"TCP Client {self.description} Message to_data() returned None. Message not initialized correctly.\n{msg}")

                        total_len = len(data)
                        offset = 0

                        # If the message exceeds the maximum block size, set the socket to blocking mode temporarily
                        # This prevents "Resource temporarily unavailable" errors on large messages
                        if total_len > self.max_block_size:
                            key.fileobj.setblocking(True)

                        # Send the message in blocks if it exceeds the maximum block size
                        while offset < total_len:
                            block = data[offset:offset + self.max_block_size]
                            block_size = len(block)
                            # Calculate remaining blocks (including this one)
                            remaining_blocks = ((total_len - offset) // self.max_block_size)
                            # Pack both as 2-byte unsigned shorts
                            header = struct.pack('>HH', block_size, remaining_blocks)
                            key.fileobj.sendall(header + block)
                            offset += self.max_block_size

                        if total_len > self.max_block_size:
                            key.fileobj.setblocking(False)

                        logger.info(f"TCP Client {self.description} sent message to {key.fileobj.getpeername()} in {total_len // self.max_block_size + 1} blocks.\n{message.Message.__str__(msg)}")
                    except (OSError, BrokenPipeError, TimeoutError, ConnectionResetError) as e:
                        logger.error(f"TCP Client {self.description} error sending message to host {self.host} port {self.port}\n{e}")

                    except Exception as e:
                        logger.error(f"TCP Client {self.description} general exception sending message to host {self.host} port {self.port}\n{e}")
                        self._process_disconnect()

            time_exit = time.time()
            logger.info(f"TCP Client {self.description} SEND {len(data)} bytes duration: {(time_exit - time_enter)*1000:.2f} ms")
    
    def nrConnections(self):
        """Return the number of connections to the server."""
        return len(self.sel.get_map()) - 1 # Subtract 1 for the client socket itself

    def disconnect(self):
        """Disconnect if currrently connected to the server."""
        for key in list(self.sel.get_map().values()):
            if key.data is not None:
                self._process_disconnect()

        logging.error(f"TCP Client {self.description} disconnected from {self.host} port {self.port}")

    def stop(self):
        """Stop the TCP client and close connections."""
        if not self.started:
            logging.warning(f"TCP Client {self.description} already stopped on host {self.host} port {self.port}")
            return

        # Unregister all sockets
        for key in list(self.sel.get_map().values()):  # Create a copy of the selector values as it may change
            if key.data is not None:
                self._process_disconnect()
            else:
                self.sel.unregister(key.fileobj)

        self.started = False # Set the client to not started

        # Stop the event handler thread
        if self.event_handler.is_alive():
            self.event_handler.join()
        
        self.sel.close() # Close the selector
        logging.info(f"TCP Client {self.description} stopped connecting to host {self.host} port {self.port}")

    def recv_all(self, socket, n):
        """Receive exactly n bytes from the socket."""
        data = b''
        while len(data) < n:
            packet = socket.recv(n - len(data))
            if not packet:
                # Connection closed
                return data if data else None
            data += packet
        return data

if __name__ == "__main__":

    # Setup logging configuration
    logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.StreamHandler(),                     # Log to console
        logging.FileHandler("server.log", mode="a")  # Log to a file
    ]
    )

    set_sample_rate_apicall = {}
    set_sample_rate_apicall["msg_type"] = "req"
    set_sample_rate_apicall["action_code"] = "set"
    set_sample_rate_apicall["property"] = "sample_rate"
    set_sample_rate_apicall["value"] = 2.4e6

    get_sample_rate_apicall = {}
    get_sample_rate_apicall["msg_type"] = "req"
    get_sample_rate_apicall["action_code"] = "get"
    get_sample_rate_apicall["property"] = "sample_rate"

    set_center_freq_apicall = {}
    set_center_freq_apicall["msg_type"] = "req"
    set_center_freq_apicall["action_code"] = "set"
    set_center_freq_apicall["property"] = "center_freq"
    set_center_freq_apicall["value"] = 1.42e6

    read_samples_apicall = {}
    read_samples_apicall["msg_type"] = "req"
    read_samples_apicall["action_code"] = "method"
    read_samples_apicall["method"] = "read_samples"
    read_samples_apicall["params"] = {"num_samples": 2.4e6}

    api_msg = message.APIMessage()

    queue = Queue()

    Timer.manager = TimerManager()
    Timer.manager.start()

    class Driver:
        def __init__(self):
            self.app_name = "tm"
            pass

        def get_interface(self, system_name):

            from api.tm_dig import TM_DIG

            if system_name in ["tm", "dig"]:
                return (TM_DIG(), None)
            else:
                raise XSoftwareFailure(f"Driver has no interface for system {system_name}")

    test1 = AppProcessor(name="Test1", event_q=queue, driver=Driver())
    test1.start()

     # Start the TCP client and connect to the server

    client = TCPClient(queue=queue)
    client.connect()
    
    time.sleep(1)

    api_msg.set_json_api_header(
        api_version="1.0",
        dt=datetime.now(timezone.utc),
        from_system="tm",
        to_system="dig",
        api_call=set_sample_rate_apicall
    )

    client.send(api_msg)

    time.sleep(1)

    api_msg.set_json_api_header(
        api_version="1.0",
        dt=datetime.now(timezone.utc),
        from_system="tm",
        to_system="dig",
        api_call=get_sample_rate_apicall
    )

    client.send(api_msg)

    api_msg.set_json_api_header(
        api_version="1.0",
        dt=datetime.now(timezone.utc),
        from_system="tm",
        to_system="dig",
        api_call=set_center_freq_apicall
    )

    client.send(api_msg)

    api_msg.set_json_api_header(
        api_version="1.0",
        dt=datetime.now(timezone.utc),
        from_system="tm",
        to_system="dig",
        api_call=read_samples_apicall
    )

    client.send(api_msg)
    time.sleep(100)
    client.stop()    
    
    # print content of the queue
    while not queue.empty():
        event = queue.get()
        print(f"Event: {event}")

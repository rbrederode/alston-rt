#!/usr/bin/env python3

import selectors
import socket
import sys
import threading
import time
import struct
import traceback
from queue import Queue
from datetime import datetime

from ipc import message
from env import events
from util.timer import TimerManager, Timer

import logging
logger = logging.getLogger(__name__)

HOST_IP = socket.gethostbyname(socket.gethostname())
HOST_PORT = 60000

MAX_BLOCK_SIZE = 65535   # Define a maximum block size for sending data

class TCPServer:
    """TCP Server class to handle connections and data from/to clients using IPv4.
        It runs in non-blocking mode and processes events in its own daemon thread.
        Events (connected, disconnected, data received) are added to a queue
        for further processing by the calling process. """

    def __init__(self, description="TCP Server", queue=None, host=HOST_IP, port=HOST_PORT, max_block_size=MAX_BLOCK_SIZE):
        """Initialize the TCP server with the given host and port.

            Parameters
                description: Description of the server
                queue: Queue to keep track of events
                host: Host IP address
                port: Port number """
    
        self.description = description
        self.host = host
        self.port = port
        self.sel = selectors.DefaultSelector()

        self.server_socket = None
        self._create_socket()

        self.event_handler = None # Thread to handle server socket events
        self.event_q = queue if queue else Queue() # Queue to keep track of events
    
        self.started = False # Flag to indicate if the server has been started or stopped
        self.max_block_size = max_block_size if max_block_size > 0 else MAX_BLOCK_SIZE

        self._send_lock = threading.Lock() # Lock to ensure thread-safe sending of messages

    def _create_socket(self):
        """Create a new socket and register it with the selector."""
        # AF_INET: IPv4, SOCK_STREAM: TCP
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Avoid bind() exception: OSError: [Errno 48] Address already in use
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.setblocking(False)  # Set the socket to non-blocking mode

    def _destroy_socket(self):
        """Destroy the server socket."""
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None

    def _process_connection(self, client_socket):
        """Accept incoming connection events from a client and register the connection with the selector."""

        # Accept the connection
        conn, addr = client_socket.accept()
        conn.setblocking(False)

        # Create a new (empty) message instance and associate it with the connection
        msg = message.Message()
        self.sel.register(conn, selectors.EVENT_READ, data=msg)
        event = events.ConnectEvent(self, conn, addr, datetime.now())
        # Add the event to the queue for further processing
        self.event_q.put(event)

        logger.info(f"{event}")

    def _process_disconnect(self, client_socket):
        """Process a disconnect from a client and deregister the connection from the selector."""
        
        # Create a disconnect event and add it to the queue
        event = events.DisconnectEvent(self, client_socket, client_socket.getpeername(), datetime.now())
        self.event_q.put(event)

        # Unregister the connection from the selector
        self.sel.unregister(client_socket)
        client_socket.close()

        logger.info(f"{event}")

    def _process_msg(self, client_socket, msg):
        """Process incoming msg events from the client and assemble the msg body from the received data."""
        try:
            full_msg = b''
            remaining_blocks = 1

            while remaining_blocks > 0:

                # Step 1: Read a 4-byte header to get the 2-byte block size (0-65,535 bytes) and 2-byte remaining blocks (0-65,535 blocks)
                msg_header = client_socket.recv(4)

                # Check if the connection has been closed i.e. zero bytes received
                if not msg_header or len(msg_header) == 0:  
                    self._process_disconnect(client_socket)
                    return
                elif len(msg_header) < 4:  # the header is incomplete
                    logger.error(f"TCP Server {self.description} received incomplete header on {self.host} port {self.port} from {client_socket.getpeername()}\nHeader (hex):\n{msg_header.hex()}")
                    return

                # Unpack the 4-byte big-endian header ('>HH' means two big-endian unsigned shorts)
                block_size, remaining_blocks = struct.unpack('>HH', msg_header)

                # Step 2:Read the block of data
                block = client_socket.recv(block_size)

                # Check if the connection was closed mid-message
                if not block or len(block) == 0:  
                    self._process_disconnect(client_socket)
                    break
                elif len(block) < block_size:  # the block is incomplete
                    logger.error(f"TCP Server {self.description} received incomplete block on {self.host} port {self.port} from {client_socket.getpeername()}\n" + \
                        f"Block size: {block_size}\nReceived size: {len(block)}\nRemaining blocks: {remaining_blocks}\n")
                    return

                full_msg += block

            # Step 3: Process the received data stream as a message
            msg.from_data(full_msg)

            # Create a data event and add it to the queue
            event = events.DataEvent(self, client_socket, client_socket.getpeername(), full_msg, datetime.now())
            self.event_q.put(event)

            logger.info(f"TCP Server {self.description} received message on {self.host} port {self.port} from {client_socket.getpeername()} Message:\n{msg}")

        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass

        except Exception as e:
            logger.error(f"TCP Server {self.description} unhandled exception error on {self.host} port {self.port} Message:\n{msg}\nException: {e}")
            self.sel.unregister(client_socket)
            client_socket.close()
            return

    def _process_events(self):
        """ Process events in a loop until the server is stopped. """
        
        # While the server has started, keep processing events
        while self.started:

            # Wait for events with a timeout specified in seconds
            events = self.sel.select(timeout=1) 
            for key, mask in events:

                # key.data is None for the server socket
                if key.data is None:
                    self._process_connection(key.fileobj)
                else:
                    try:
                        if mask & selectors.EVENT_READ:
                            self._process_msg(key.fileobj, key.data)
                        elif mask & selectors.EVENT_WRITE:
                            # Handle write events if needed
                            pass
                    except Exception as e:
                        logger.error(f"TCP Server {self.description} unhandled exception error on {self.host} port {self.port} from {key.fileobj.getpeername()} Data (hex): {key.data.hex()} Exception: {e}")

    def start(self):
        """Start the TCP server i.e. listen for incoming connections
            and start the event handler thread."""
        
        # Check if the server is already started
        if self.started:
            logger.warning(f"TCP Server {self.description} already started on host {self.host} port {self.port}")
            return
        
        self.started = True
        self.server_socket.listen()
        self.sel.register(self.server_socket, selectors.EVENT_READ, data=None)

        logger.info(f"TCP Server {self.description} started listening on host {self.host} port {self.port}")

        # Create & start a thread to handle events, set it as a daemon thread (killed when the main thread exits)
        self.event_handler = threading.Thread(target=self._process_events)
        self.event_handler.daemon = True 
        self.event_handler.start()

    def send(self, msg, client_socket=None):
        """Send a message to a specific connected client."""

        with self._send_lock:  # Ensure that only one thread can send a message at a time

            if client_socket is None:

                client_key = next((key for key in self.sel.get_map().values() if key.data is not None), None)
                if client_key is None:
                    logger.error(f"TCP Server {self.description} no clients connected to server on host {self.host} port {self.port}. Cannot send message.\n{msg}")
                    return
                client_socket = client_key.fileobj    

            if not isinstance(msg, message.Message):
                logging.error(f"TCP Server {self.description} invalid message type. Expected message.Message, got {type(msg)}.\n{msg}")
                return

            if client_socket is None or client_socket.fileno() == -1:
                logging.error(f"TCP Server {self.description} socket is invalid. Cannot send message.\n{msg}")
                return

            if client_socket not in [key.fileobj for key in self.sel.get_map().values() if key.data is not None]:
                logger.error(f"TCP Server {self.description} client socket {client_socket.getpeername()} not connected to server on host {self.host} port {self.port}. Cannot send message.\n{msg}")
                return

            try:
                data = msg.to_data()  # Convert the message to bytes 
                
                total_len = len(data)
                offset = 0

                # If the message exceeds the maximum block size, set the socket to blocking mode temporarily
                # This prevents "Resource temporarily unavailable" errors on large messages
                if total_len > self.max_block_size:
                    client_socket.setblocking(True)

                # Send the message in blocks if it exceeds the maximum block size
                while offset < total_len:
                    block = data[offset:offset + self.max_block_size]
                    block_size = len(block)
                    # Calculate remaining blocks (including this one)
                    remaining_blocks = ((total_len - offset) // self.max_block_size)
                    # Pack both as 2-byte unsigned shorts
                    header = struct.pack('>HH', block_size, remaining_blocks)
                    client_socket.sendall(header + block)
                    offset += self.max_block_size

                if total_len > self.max_block_size:
                    client_socket.setblocking(False)

                logger.info(f"TCP Server {self.description} sent message to {client_socket.getpeername()} in {total_len // self.max_block_size + 1} blocks.\n{message.Message.__str__(msg)}")
            except (OSError, BrokenPipeError, TimeoutError, ConnectionResetError) as e:
                logger.error(f"TCP Server {self.description} error sending message to {client_socket.getpeername()}: {e}")

            except Exception as e:
                logger.error(f"TCP Server {self.description} error sending message to {client_socket.getpeername()}: {e}")
                self._process_disconnect(client_socket)

    def broadcast(self, msg):
        """Send a message to all connected clients."""
        # Iterate over all connections and send the message
        for key in list(self.sel.get_map().values()):
            if key.data is not None:
                self.send(msg, key.fileobj)
    
    def nrConnections(self):
        """Return the number of connections to the server."""
        return len(self.sel.get_map()) - 1 # Subtract 1 for the server socket itself

    def disconnectAll(self):
        """Disconnect all clients currrently connected to the server."""
        for key in list(self.sel.get_map().values()):
            if key.data is not None:
                self._process_disconnect(key.fileobj)

        logger.error(f"TCP Server {self.description}: All clients disconnected from {self.host} port {self.port}")
        
    def stop(self):
        """Stop the TCP server and close all connections."""
        if not self.started:
            logger.warning(f"TCP Server {self.description} already stopped on host {self.host} port {self.port}")
            return

        # Unregister all sockets
        for key in list(self.sel.get_map().values()):  # Create a copy of the selector values as it may change
            if key.data is not None:
                self._process_disconnect(key.fileobj)
            else:
                self.sel.unregister(key.fileobj)

        self.started = False # Set the server to not started

        # Stop the event handler thread
        if self.event_handler.is_alive():
            self.event_handler.join()
        
        self.sel.close() # Close the selector
        logger.info(f"TCP Server {self.description} stopped listening on host {self.host} port {self.port}")

if __name__ == "__main__":

    # Setup logging configuration
    logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.StreamHandler(),                     # Log to console
        logging.FileHandler("server.log", mode="a")  # Log to a file
    ]
    )

    # Example usage of the TCPServer class    
    queue = Queue()

    Timer.manager = TimerManager()
    Timer.manager.start()

    server = TCPServer(queue=queue)
    server.start()
    time.sleep(1000) # Keep the server running for 1000 seconds for testing
    server.stop()    
    
    # print content of the queue
    while not queue.empty():
        event = queue.get()
        print(f"Event: {event}")

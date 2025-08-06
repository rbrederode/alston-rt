#!/usr/bin/env python3

import selectors
import socket
import sys
import threading
import time
import struct
import traceback
import logging
from queue import Queue
from datetime import datetime

import message
import events

logger = logging.getLogger(__name__)

HOST_IP = socket.gethostbyname(socket.gethostname())
HOST_PORT = 12345

class TCPServer:
    """TCP Server class to handle connections and data from/to clients using IPv4.
        It runs in non-blocking mode and processes events in its own daemon thread.
        Events (connected, disconnected, data received) are added to a queue
        for further processing by the calling process. """

    def __init__(self, description="TCP Server", queue=None, host=HOST_IP, port=HOST_PORT):
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

        # AF_INET: IPv4, SOCK_STREAM: TCP
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Avoid bind() exception: OSError: [Errno 48] Address already in use
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.setblocking(False)  # Set the socket to non-blocking mode

        self.event_handler = None # Thread to handle server socket events
        self.event_q = queue if queue else Queue() # Queue to keep track of events
    
        self.started = False # Flag to indicate if the server has been started or stopped

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

        logging.info(f"{event}")

    def _process_disconnect(self, client_socket):
        """Process a disconnect from a client and deregister the connection from the selector."""
        
        # Create a disconnect event and add it to the queue
        event = events.DisconnectEvent(self, client_socket, client_socket.getpeername(), datetime.now())
        self.event_q.put(event)

        # Unregister the connection from the selector
        self.sel.unregister(client_socket)
        client_socket.close()

        logging.info(f"{event}")

    def _process_msg(self, client_socket, msg):
        """Process incoming msg events from the client and assemble the msg body from the received data."""
        try:
            # Step 1: Read a 2-byte header to get the message length
            msg_header = client_socket.recv(2)
            
            # Check if the connection has been closed i.e. zero bytes received
            if not msg_header or len(msg_header) == 0:  
                self._process_disconnect(client_socket)
                return
            elif len(msg_header) < 2:  # the header is incomplete
                logging.error(f"{self.description} received incomplete header on {self.host} port {self.port} from {client_socket.getpeername()} Data (hex): {msg_header.hex()}")
                return

            # Unpack the 2-byte big-endian header to get the message length
            msg_length = struct.unpack('>H', msg_header)[0]

            # Step 2: Read the full message based on the length
            msg_body = b''  
            while len(msg_body) < msg_length:
                msg_buffer = client_socket.recv(msg_length - len(msg_body))
                # Check if the connection was closed mid-message
                if not msg_buffer:  
                    self._process_disconnect(client_socket)
                    break
                msg_body += msg_buffer

            # Check if the message body is complete
            if len(msg_body) < msg_length:
                logging.error(f"{self.description} received incomplete message on {self.host} port {self.port} from {client_socket.getpeername()} Data (hex): {msg_body.hex()}")
                return

            # Step 3: Process the received data stream as a message
            msg.from_data(msg_body)  

            # Create a data event and add it to the queue
            event = events.DataEvent(self, client_socket, client_socket.getpeername(), msg_body, datetime.now())
            self.event_q.put(event)

            logging.info(f"{event}")

        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass
        except Exception as e:
            logging.error(f"{self.description} unhandled exception error on {self.host} port {self.port} from {client_socket.getpeername()} Data (hex): {msg_body.hex()} Exception: {e}")
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
                    except Exception:
                        logging.error(f"{self.description} unhandled exception error on {self.host} port {self.port} from {key.fileobj.getpeername()} Data (hex): {key.data.hex()} Exception: {e}")

    def start(self):
        """Start the TCP server i.e. listen for incoming connections
            and start the event handler thread."""
        
        # Check if the server is already started
        if self.started:
            logging.warning(f"{self.description} already started on host {self.host} port {self.port}")
            return
        
        self.started = True
        self.server_socket.listen()
        self.sel.register(self.server_socket, selectors.EVENT_READ, data=None)

        logging.info(f"{self.description} started listening on host {self.host} port {self.port}")

        # Create & start a thread to handle events, set it as a daemon thread (killed when the main thread exits)
        self.event_handler = threading.Thread(target=self._process_events)
        self.event_handler.daemon = True 
        self.event_handler.start()
    
    def broadcast(self, msg):
        """Send a message to all connected clients."""
        # Iterate over all connections and send the message
        for key in list(self.sel.get_map().values()):
            if key.data is not None:
                try:
                    data = msg.to_data()  # Convert the message to bytes
                    # Send the length of the message as a 2-byte header
                    header = struct.pack('>H', len(data))  # '>H' means big-endian unsigned short (2 bytes)
                    # Send the combined header and the actual data
                    # sendall() is used to ensure all data is sent
                    key.fileobj.sendall(header + data)

                    logging.info(f"Sent message to {key.fileobj.getpeername()}: {msg}")
                except (OSError, BrokenPipeError, TimeoutError, ConnectionResetError) as e:
                    logging.error(f"Error sending message to {key.fileobj.getpeername()}: {e}")
                    
                except Exception as e:
                    logging.error(f"Error sending message to {key.fileobj.getpeername()}: {e}")
                    self._process_disconnect(key.fileobj)
    
    def nrConnections(self):
        """Return the number of connections to the server."""
        return len(self.sel.get_map()) - 1 # Subtract 1 for the server socket itself

    def disconnectAll(self):
        """Disconnect all clients currrently connected to the server."""
        for key in list(self.sel.get_map().values()):
            if key.data is not None:
                self._process_disconnect(key.fileobj)

        logging.error(f"{self.description}: All clients disconnected from {self.host} port {self.port}")
        
    def stop(self):
        """Stop the TCP server and close all connections."""
        if not self.started:
            logging.warning(f"{self.description} already stopped on host {self.host} port {self.port}")
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
        logging.info(f"{self.description} stopped listening on host {self.host} port {self.port}")

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

    data = "Hello, Wörld!".encode('utf-8')
    hex_data = ' '.join(data[i:i+1].hex() for i in range(len(data)))
    print('String', hex_data)
    
    queue = Queue()
    server = TCPServer(queue=queue)
    server.start()
    time.sleep(10)  # Keep the server running for a few seconds for testing

    cmd = message.Message()
    cmd.from_data(b'Start Telescope')

    server.broadcast(cmd)
    time.sleep(100)
    server.stop()    
    
    # print content of the queue
    while not queue.empty():
        event = queue.get()
        print(f"Event: {event}")

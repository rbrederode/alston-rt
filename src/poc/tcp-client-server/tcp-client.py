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

DEST_IP = socket.gethostbyname(socket.gethostname())
DEST_PORT = 12345

class TCPClient:
    """TCP Client class to create connections and send data to/from a server using IPv4.
        It runs in non-blocking mode and processes events in its own daemon thread.
        Events (connected, disconnected, data received) are added to a queue
        for further processing by the calling process. """

    def __init__(self, description="TCP Client", queue=None, host=DEST_IP, port=DEST_PORT):
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
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setblocking(False)  # Set the socket to non-blocking mode
        
        # Create a new (empty) message instance and associate it with the client socket
        msg = message.Message()
        self.sel.register(self.client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=msg)

        self.started = True     # Flag to indicate if the client daemon thread is running
        self.connected = False  # Flag to indicate if the client is connected to a server

        # Create & start a thread to handle events, set it as a daemon thread (killed when the main thread exits)
        self.event_handler = threading.Thread(target=self._process_events)
        self.event_handler.daemon = True 
        self.event_handler.start()
        self.event_q = queue if queue else Queue() # Queue to keep track of events    

    def _process_connection(self):
        """Accept incoming connection events from a client and register the connection with the selector."""

        event = events.ConnectEvent(self, self.client_socket, (self.host, self.port), datetime.now())
        # Add the event to the queue for further processing
        self.event_q.put(event)

        logging.info(f"{self.description} connected to host {self.host} port {self.port}")

    def _process_disconnect(self):
        """Process a disconnect from a client and deregister the connection from the selector."""
        
        # Create a disconnect event and add it to the queue
        event = events.DisconnectEvent(self, self.client_socket, self.client_socket.getpeername(), datetime.now())
        self.event_q.put(event)

        # Unregister the connection from the selector
        self.sel.unregister(self.client_socket)
        self.client_socket.close()  # Close the socket connection
        self.connected = False  # Set the client to not connected

        logging.info(f"{self.description} disconnected from host {self.host} port {self.port}")

    def _process_msg(self, msg):
        """Process incoming msg events from the server and assemble the msg body from the received data."""
        try:
            # Step 1: Read a 2-byte header to get the message length
            msg_header = self.client_socket.recv(2)
            
            # Check if the connection has been closed i.e. zero bytes received
            if not msg_header or len(msg_header) == 0:  
                self._process_disconnect()
                return
            elif len(msg_header) < 2:  # the header is incomplete
                logging.error(f"{self.description} received incomplete header on connection to {self.host} port {self.port} Data (hex): {msg_header.hex()}")
                return

            # Unpack the 2-byte big-endian header to get the message length
            msg_length = struct.unpack('>H', msg_header)[0]

            # Step 2: Read the full message based on the length
            msg_body = b''  
            while len(msg_body) < msg_length:
                msg_buffer = self.client_socket.recv(msg_length - len(msg_body))
                # Check if the connection was closed mid-message
                if not msg_buffer:  
                    self._process_disconnect(client_socket)
                    break
                msg_body += msg_buffer

            # Check if the message body is complete
            if len(msg_body) < msg_length:
                logging.error(f"{self.description} received incomplete message on connection to {self.host} port {self.port} Data (hex): {msg_body.hex()}")
                return

            # Step 3: Process the received data stream as a message
            msg.from_data(msg_body)  

            # Create a data event and add it to the queue
            event = events.DataEvent(self, self.client_socket, (self.host, self.port), msg_body, datetime.now())
            self.event_q.put(event)

            logging.info(f"{self.description} processed message from host {self.host} port {self.port}\n{msg}")

        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass
        except Exception as e:
            logging.error(f"{self.description} unhandled exception error on connection to {self.host} port {self.port} Data (hex): {msg_body.hex()} Exception: {e}")
            self.sel.unregister(self.client_socket)
            self.client_socket.close()
            return

    def _process_events(self):
        """ Process events in a loop until the client is stopped. """
        
        # While the client has started, keep processing events
        while self.started:

            # Wait for events with a timeout specified in seconds
            events = self.sel.select(timeout=1) 
            for key, mask in events:

                # key.data is None for the client socket
                if key.data is None:
                    print("Key Data is None!!!")
                else:
                    try:
                        self._process_msg(key.data)
                    except Exception:
                        logging.error(f"{self.description} unhandled exception error on connection to {self.host} port {self.port} Data (hex): {key.data.hex()} Exception: {e}")

    def connect(self):
        """Start the TCP client by establishing a connection and starting the event handler thread."""
        
        # Check if the client is already connected
        if self.connected:
            logging.warning(f"{self.description} already connected to host {self.host} port {self.port}")
            return
        
        result = self.client_socket.connect_ex((self.host, self.port))
        if result != 0 and result != 36:  # BlockingIOError: [Errno 36] Operation now in progress, which can be ignored
            logging.error(f"{self.description} failed to connect to host {self.host} port {self.port} with error code {result}")
            # TBD: Start timer and retry connection
            return result
        else:
            self.connected = True  # Set the client to connected
            self._process_connection()  
    
    def send(self, msg):
        """Send a message to the server"""
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

                    logging.info(f"Sent message to host {self.host} port {self.port}: {msg}")
                except (OSError, BrokenPipeError, TimeoutError, ConnectionResetError) as e:
                    logging.error(f"Error sending message to host {self.host} port {self.port}: {e}")
                    
                except Exception as e:
                    logging.error(f"Error sending message to host {self.host} port {self.port}: {e}")
                    self._process_disconnect()
    
    def nrConnections(self):
        """Return the number of connections to the server."""
        return len(self.sel.get_map()) - 1 # Subtract 1 for the client socket itself

    def disconnect(self):
        """Disconnect if currrently connected to the server."""
        for key in list(self.sel.get_map().values()):
            if key.data is not None:
                self._process_disconnect()

        logging.error(f"{self.description}: Disconnected from {self.host} port {self.port}")
        
    def stop(self):
        """Stop the TCP client and close connections."""
        if not self.started:
            logging.warning(f"{self.description} already stopped on host {self.host} port {self.port}")
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
        logging.info(f"{self.description} stopped connecting to host {self.host} port {self.port}")

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

    data = '{"byteorder": "big", "content-type": "text/json", "content-encoding": "utf-8", "content-length": 14}Hello, Server!'

    app_msg = message.AppMessage()
    app_msg.from_data(data.encode('utf-8'))
    
    queue = Queue()
    client = TCPClient(queue=queue)
    client.connect()
 
    client.send(app_msg)
    time.sleep(10)
    client.stop()    
    
    # print content of the queue
    while not queue.empty():
        event = queue.get()
        print(f"Event: {event}")

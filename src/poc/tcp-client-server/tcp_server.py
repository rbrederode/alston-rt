# -*- coding: utf-8 -*-

# Server side of a TCP connection 
# Used to transfer data between the client and server
import socket
import threading
import struct
import time
from queue import Queue
from datetime import datetime

# Define constants to be used in the server
HOST_IP = socket.gethostbyname(socket.gethostname())
HOST_PORT = 12345
ENCODING = 'utf-8'

# Define the server class
class TCPServer:
    """TCP Server class to handle incoming connections and messages.
        It runs in blocking mode and processes connections and messages in separate threads."""

    def __init__(self, host=HOST_IP, port=HOST_PORT):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.setblocking(True)  # Set the socket to blocking mode
        self.lock = threading.Lock()  # Create a lock for thread-safe operations
        self.msg_threads = Queue()  # Use a THREAD-SAFE Queue to keep track of the number of msg handler threads
        self.conn_threads = Queue()  # Use a THREAD-SAFE Queue to keep track of the number of connection handler threads
        self.connections = []  # Keep track of client connections

    def accept_connections(self):
        """Accept incoming connections."""
        
        # Server socket runs in blocking mode, so we accept incoming connections
        # until an exception is thrown e.g. server socket is closed
        while True: 

            try:
                # Accept incoming connections
                client_socket, addr = self.server_socket.accept()  
                print(f"{datetime.now()} - Connection accepted on {self.host} {self.port} from {addr}")

                # Create a new thread to handle incoming messages
                msg_handler = threading.Thread(target=self.receive_messages, args=(client_socket, addr))

                # Create a connection dictionary to store the client connection details
                connection = {
                    'client_socket' : client_socket,
                    'client_address': addr,
                    'msg_handler'   : msg_handler,
                    'connected'     : datetime.now(),
                    'disconnected'  : None
                }

                # Lists are not thread-safe, so we use a lock to ensure thread safety
                with self.lock:
                    self.connections.append(connection)  # Add the connection to list
            
                # Start the message handler thread
                msg_handler.start()
            
            except OSError as e:
                print(f"{datetime.now()} - OS error on {self.host} {self.port}: {e}")
                break
            except TimeoutError:
                print(f"{datetime.now()} - Socket accept timed out on {self.host} {self.port}")
                break
            except BlockingIOError:
                print(f"{datetime.now()} - Socket accept blocked on {self.host} {self.port}")
                break
            except Exception as e:
                print(f"{datetime.now()} - Unexpected error on {self.host} {self.port}: {e}")
                break
       
        # Close the client socket if it exists
        if client_socket:
            client_socket.close()

            # Set connection object to disconnected
            with self.lock:
                for conn in self.connections:
                    if conn['client_socket'] == client_socket:
                        conn['disconnected'] = datetime.now()
                        break

    def receive_messages(self, client_socket, addr):
        """Receive messages from the client."""

        # Socket runs in blocking mode, so we receive incoming messages
        # until an exception is thrown e.g. connection is closed
        while True:  

            try:
                # Step 1: Read the 2-byte header to get the message length
                header = client_socket.recv(2)
                if len(header) == 0:  # the connection is closed
                    print(f"{datetime.now()} - Connection closed by {addr}")
                    break
                elif len(header) == 1:  # the header is incomplete
                    print(f"{datetime.now()} - Incomplete header received from {addr}")
                    print(f"{datetime.now()} - Data hex: {header.hex()}")
                    break

                # Unpack the 2-byte big-endian header to get the message length
                message_length = struct.unpack('>H', header)[0]

                # Step 2: Read the full message based on the length
                data = b''  # Initialize an empty bytes object to store the message
                while len(data) < message_length:
                    chunk = client_socket.recv(message_length - len(data))
                    if not chunk:  # If the connection is closed mid-message
                        print(f"{datetime.now()} - Connection closed by {addr} while receiving data")
                        break
                    data += chunk

                if len(data) < message_length:
                    print(f"{datetime.now()} - Incomplete message received from {addr}")
                    break

                # Step 3: Process the received data
                hex_data = ' '.join(data[i:i+1].hex() for i in range(len(data)))
                print("/n")
                print(f"{datetime.now()} - Received {len(data)} bytes from {addr}")
                print(f"{datetime.now()} - Data (decoded): {data.decode(ENCODING)}")
                print(f"{datetime.now()} - Data (hex): {hex_data}\n")

            except ConnectionResetError:
                print(f"{datetime.now()} - Connection reset by {addr}")
                break
            except TimeoutError:
                print(f"{datetime.now()} - Connection timed out with {addr}")
                break
            except OSError as e:
                print(f"{datetime.now()} - OS error with {addr}: {e}")
                break

        # Close the client socket if it exists
        if client_socket:
            client_socket.close()

            # Set connection object to disconnected
            with self.lock:
                for conn in self.connections:
                    if conn['client_socket'] == client_socket:
                        conn['disconnected'] = datetime.now()
                        break

    def status(self):
        """Print the status of the server and its connections."""
        
        print(f"{datetime.now()} - Server status on {self.host} {self.port}:")
        print(f"{datetime.now()} - Number of connections: {len(self.connections)}")
        for conn in self.connections:
            print(f"{datetime.now()} - Connection from {conn['client_address']} at {conn['connected']}")
            if conn['disconnected']:
                print(f"{datetime.now()} - Disconnected from {conn['client_address']} at {conn['disconnected']}")

    def start(self):
        """Start the TCP server i.e. listen for incoming connections."""
        
        self.server_socket.listen(5)
        print(f"{datetime.now()} - Server started on host {self.host} port {self.port}")

        connection_thread = threading.Thread(target=self.accept_connections)
        self.conn_threads.put(connection_thread)  # Add the thread to the connection thread queue
        
        # Start the connection thread
        connection_thread.start()
        
    def stop(self):
        """Stop the TCP server if it is active."""

        # Check if the server socket is already closed
        if self.server_socket is None:
            return # nothing to do

        print(f"{datetime.now()} - Stopping TCP server on {self.host} {self.port}")
 
        # Close all client connections
        with self.lock:
            for conn in self.connections:
                try:
                    conn['client_socket'].close()
                    print(f"{datetime.now()} - Client socket from {conn['client_address']} closed on {self.host} {self.port}")
                except OSError as e:
                    print(f"{datetime.now()} - OS error while closing client socket from {conn['client_address']} on {self.host} {self.port}: {e}")
                except Exception as e:
                    print(f"{datetime.now()} - Unexpected error while closing client socket from {conn['client_address']} on {self.host} {self.port}: {e}")

        # Close the server socket
        try:
            self.server_socket.close()
            print(f"{datetime.now()} - Server socket closed successfully on host {self.host} port {self.port}")
        except OSError as e:
            print(f"{datetime.now()} - OS error while closing server socket on {self.host} {self.port}: {e}")
        except Exception as e:
            print(f"{datetime.now()} - Unexpected error while closing server socket on {self.host} {self.port}: {e}")
        finally:
            self.server_socket = None

    def __del__(self):
        """Destructor to ensure the server socket is closed."""
        
        self.stop()

if __name__ == "__main__":
    server = TCPServer()
    server.start()
    time.sleep(15)  # Keep the server running for a few seconds for testing
    server.status()  # Print the status of the server
    server.stop()
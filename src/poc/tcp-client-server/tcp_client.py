# -*- coding: utf-8 -*-

# Client side of a TCP connection 
# Used to transfer data between the client and server
import socket
import threading
import time
import struct
from datetime import datetime
import message

# Define constants to be used in the server
DEST_IP = socket.gethostbyname(socket.gethostname())
DEST_PORT = 12345
ENCODING = 'utf-8'

# Define the client class
class TCPClient:
    """Define function placeholders and
    test function examples."""

    def __init__(self, host=DEST_IP, port=DEST_PORT):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))
        print(f"{datetime.now()} - Client socket connected to server on {self.host}:{self.port}")

    def receive_data(self):
        """Receive data from the server."""
        try:
            # Receive the header first
            header = self.client_socket.recv(2)
            if not header:
                print(f"{datetime.now()} - No header received from {self.host} {self.port}")
                return None

            # Unpack the header to get the length of the data
            data_length = struct.unpack('>H', header)[0]  # '>H' means big-endian unsigned short (2 bytes)

            # Now receive the actual data
            data = self.client_socket.recv(data_length)
            if not data:
                print(f"{datetime.now()} - No data received from {self.host} {self.port}")
                return None

            msg = message.Message()
            msg.fromData(data)

            print(f"{datetime.now()} - Client socket received {len(data)} bytes from {self.host} {self.port}")
            print(f'{msg}')
            return data.decode(ENCODING)

        except OSError as e:
            print(f"{datetime.now()} - OS error while receiving data from {self.host} {self.port}: {e}")
        except TimeoutError:
            print(f"{datetime.now()} - Socket receive timed out while receiving data from {self.host} {self.port}")
        except ConnectionResetError:
            print(f"{datetime.now()} - Connection reset by server while receiving data from {self.host} {self.port}")
        except Exception as e:
            print(f"{datetime.now()} - Unexpected error while receiving data from {self.host} {self.port}: {e}")

        # Close the socket if an error occurs
        self.client_socket.close()
        print(f"{datetime.now()} - Client socket closed on {self.host}:{self.port}")
        return None

    def send_data(self, data):
        """Send data to the server."""

         # Encode the data and calculate its length
        encoded_data = data.encode(ENCODING)
        data_length = len(encoded_data)

        # Pack the length as a 2-byte big-endian integer
        header = struct.pack('>H', data_length) # '>H' means big-endian unsigned short (2 bytes)

        # Combine the header and the actual data
        message = header + encoded_data

        try:
            self.client_socket.sendall(message)

            hex_data = ' '.join(encoded_data[i:i+1].hex() for i in range(len(encoded_data)))
            print(f"\n{datetime.now()} - Client socket sent {len(data)} bytes to {self.host} {self.port}")
            print(f"{datetime.now()} - Data (decoded): {data}")
            print(f"{datetime.now()} - Data hex: {hex_data}\n")

            return True

        except OSError as e:
            print(f"{datetime.now()} - OS error while sending data to {self.host} {self.port}: {e}")
        except BrokenPipeError:
            print(f"{datetime.now()} - Broken pipe error while sending {data} to {self.host} {self.port}")
        except TimeoutError:
            print(f"{datetime.now()} - Socket send timed out while sending {data} to {self.host} {self.port}")
        except ConnectionResetError:
            print(f"{datetime.now()} - Connection reset by server whilse sending {data} to {self.host} {self.port}")
        except Exception as e:
            print(f"{datetime.now()} - Unexpected error while sending {data} to {self.host} {self.port}: {e}")
        
        # Close the socket if an error occurs
        self.client_socket.close()
        print(f"{datetime.now()} - Client socket closed on {self.host}:{self.port}")
        return False
        
    def close(self):
        """Close the client socket."""
        self.client_socket.close()
        print(f"{datetime.now()} - Client socket closed on {self.host}:{self.port}")

if __name__ == "__main__":
    client = TCPClient()
    data = '{"byteorder": "big", "content-type": "text/json", "content-encoding": "utf-8", "content-length": 14}Hello, Server!'
    client.send_data(data)
    time.sleep(3)
    data = "Goodbye, Server!"
    client.send_data(data)
    client.receive_data()
    time.sleep(10)
    client.close()

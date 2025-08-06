# Message

import io
import json
import sys
import struct

ENCODING = 'utf-8'

class Message:

    def __init__(self):
        """
        Initializes the message instance.
        """

        # The message body is a byte array containing the data stream representation.
        self.msg_data = None   
        self.msg_length = 0  

    def from_data(self, data):
        """
        Converts a byte array to a message.
        Parameters
        msg     An array of bytes containing the data stream representation.
        Returns the offset of the first byte in msg that does not form part of this message.
        """

        # Check if the data is a bytes or bytearray object
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("Message: data must be a bytes or bytearray object.")

        self.msg_length = len(data)
        self.msg_data = data

        if self.msg_length == 0:
            raise ValueError("Message: Data cannot be empty.")

        return 0 # Base class does not unpack any data into fields, so return a zero offset.

    def to_data(self):
        """
        Pack this message instance into its data stream representation.
        Returns an array of bytes containing the data stream representation.
        """
        return self.msg_data

    def __str__(self):
        """
        Returns a human-readable string representation of this event.
        """

        if self.msg_data is None:
            return ""
        else:
            hex_data = ' '.join(self.msg_data[i:i+1].hex() for i in range(self.msg_length))
            return f"Data (ascii): {self.msg_data}, length={self.msg_length}\n" + \
                   f"Data (hex): {hex_data}\n"

class AppMessage(Message):
    """
    A class representing an application message, containing a JSON header
    Inherits from the Message class.
    """

    def __init__(self):
        """
        Initializes the application message instance.
        """
        super().__init__()
        self.json_header_length = None        # The length of the JSON header.
        self.json_header_bytes = bytearray()  # The JSON header is a byte array containing the data stream representation
        self.json_header_dict = None          # The JSON header is a dictionary containing the data stream representation.
        
        self.content_bytes = bytearray()      # The application data is a byte array containing the data stream representation.

    def _json_encode(self, obj, encoding):
        """
        Encodes a Python object into a JSON byte array.
        """
        return json.dumps(obj, ensure_ascii=False).encode(encoding)

    def _json_decode(self, json_bytes, encoding):
        """
        Decodes a JSON byte array into a Python object.
        """
        tiow = io.TextIOWrapper(
            io.BytesIO(json_bytes), encoding=encoding, newline=""
        )
        obj = json.load(tiow)
        tiow.close()
        return obj

    def set_json_header(self, content_type, content_encoding, content_bytes):
        """
        Sets the json header data.
        """
        # Check if the content_bytes is a bytes or bytearray object
        if not isinstance(content_bytes, (bytes, bytearray)):
            raise TypeError("AppMessage: content_bytes must be a bytes or bytearray object.")
        
        self.json_header_dict = {
            "byteorder": sys.byteorder,
            "content-type": content_type,
            "content-encoding": content_encoding,
            "content-length": len(content_bytes),
        }
        self.content_bytes = content_bytes

    def get_json_header(self):
        """
        Returns the JSON header as a dictionary.
        """
        return self.json_header_dict

    def from_data(self, data):
        """
        Converts a byte array to an application message, by decoding the JSON header
        """

        # Call the parent class to handle the initial message processing
        # and retrieve the offset in the data where its processing ends.
        offset = super().from_data(data)

        # Peek ahead to find first occurance of '}' indicating the end of the JSON header
        json_hdr_end = data[offset:].find(b'}')
        if json_hdr_end == -1:
            raise ValueError("Invalid App Message, JSON header: '}' not found.")

        self.json_header_length = json_hdr_end - offset + 1
        self.json_header_bytes = data[offset:self.json_header_length]  
        self.json_header_dict = self._json_decode(
            self.json_header_bytes, "utf-8")
 
        # Check if the JSON header contains the required json key value pairs
        for key in ("byteorder", "content-length", "content-type", "content-encoding"):
            if key not in self.json_header_dict:
                raise ValueError(f"Missing required key in App Message JSON header '{key}'.")

        self.content_bytes = data[self.json_header_length:]
      
        offset += self.json_header_length        
        return offset

    def to_data(self):
        """
        Pack this application message instance into its data stream representation.
        """

        # If there is no JSON header, then let the base class handle the packing
        if self.json_header_dict is None or self.json_header_length == 0:
            return super().to_data()

        self.json_header_bytes = self._json_encode(self.json_header_dict, ENCODING)
        data = self.json_header_bytes + self.content_bytes
        return data

    def __str__(self):
        """
        Returns a human-readable string representation of the app message
        """

        if self.json_header_dict is None:
            return super().__str__()
        else:
            return f"JSON header (length={self.json_header_length}): {json.dumps(self.json_header_dict, indent=4)}\n" + \
                   f"Content bytes: {self.content_bytes}"

if __name__ == "__main__":

    # Example usage of Message
    msg = Message()
    data = "Hello, Wörld!".encode(ENCODING)

    print('-'*50)
    print(f"Test unpacking a Message data stream")
    print('-'*50)
    msg.from_data(data)
    print(f"Unpacked Message: {msg}")

    print('-'*50)
    print("Test packing a Message data stream")
    print('-'*50)
    packed_data = msg.to_data()
    print(f"Packed Message: {packed_data}")


    test_payload = '''{
        "name": "Frieda",
        "is_dog": true,
        "hobbies": [
            "eating",
            "sleeping",
            "barking"
        ],
        "age": 8,
        "address": {
            "work": null,
            "home": [
            "Berlin",
            "Germany"
            ]
        },
        "friends": [
            {
            "name": "Philipp",
            "hobbies": [
                "eating",
                "sleeping",
                "reading"
            ]
            },
            {
            "name": "Mitch",
            "hobbies": [
                "running",
                "snacking"
            ]
            }
        ]
        }'''

    # Example usage of AppMessage
    app_msg = AppMessage()

    print('-'*50)
    print(f"Test packing an AppMessage data stream")
    print('-'*50)
    # Set the JSON header
    app_msg.set_json_header(
        content_type="text/json",
        content_encoding="utf-8",
        content_bytes=test_payload.encode(ENCODING)
    )
    
    print(f"App message to_data {app_msg.to_data()}")
    print(f"App message json_header_dict: {app_msg.get_json_header()}")

    print('-'*50)
    print(f"Test unpacking an app message")    
    print('-'*50)
    data = b'{"byteorder": "big", "content-type": "text/json", "content-encoding": "utf-8", "content-length": 14}{Hello, Server!}'
    app_msg.from_data(data)
    print(f"JSON header: {app_msg.json_header_dict}")
    print(f"JSON header length: {app_msg.json_header_length}")
    print(f"JSON header bytes: {app_msg.json_header_bytes}")

    print('-'*50)
    print("Testing string representation of a message")
    print('-'*50)
    print(f"Message: {msg}")
    print(f"AppMessage: {app_msg}")



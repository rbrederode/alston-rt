# -*- coding: utf-8 -*-

from datetime import datetime

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
        return f"{self.timestamp} - {self.local_sap.description} connected to {self.remote_addr}"

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
        return f"{self.timestamp} - {self.local_sap.description} disconnected from {self.remote_addr}"

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
            return f"{self.timestamp} - {self.local_sap.description} received NO data from {self.remote_addr}\n"
        else:
            hex_data = ' '.join(self.data[i:i+1].hex() for i, val in enumerate(self.data))
            return f"{self.timestamp} - {self.local_sap.description} received data from {self.remote_addr}\n" + \
                f"Data (ascii): {self.data.decode('utf-8')}\n" + \
                f"Data (hex): {hex_data}\n"
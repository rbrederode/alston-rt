#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MD-01 Rotator Controller Simulator

Simulates the MD-01 rotator controller protocol for testing purposes.
Listens on TCP socket and responds to SPID protocol commands.

Protocol specification from http://ryeng.name/blog/3:
- Commands: 13 byte packets
- Responses: 12 byte packets

Command format:
Byte:    0   1    2    3    4    5    6    7    8    9    10   11  12
       -----------------------------------------------------------------
Field: | S | H1 | H2 | H3 | H4 | PH | V1 | V2 | V3 | V4 | PV | K | END |
       -----------------------------------------------------------------
Value:   57  3x   3x   3x   3x   0x   3x   3x   3x   3x   0x   xF  20 (hex)

Commands:
- 0x0F: Stop
- 0x1F: Status
- 0x2F: Set position
"""

import socket
import logging
import sys
import threading
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MD01Simulator:
    """Simulates an MD-01 rotator controller."""
    
    def __init__(self, host='192.168.0.2', port=65000):
        self.host = host
        self.port = port
        self.running = False
        
        # Current simulated position (degrees)
        self.current_alt = 45.0
        self.current_az = 180.0
        
        # Target position for slewing
        self.target_alt = 45.0
        self.target_az = 180.0
        
        # Movement parameters
        self.is_moving = False
        self.slew_rate = 2.0  # degrees per second
        
        # Protocol constants
        self.START_BYTE = 0x57  # Start byte. This is always 0x57 ('W')
        self.END_BYTE = 0x20    # End byte. This is always 0x20 (space)

        # Command (0x0F=stop, 0x1F=status, 0x2F=set)
        self.CMD_STOP = 0x0F    # Stop command
        self.CMD_STATUS = 0x1F  # Status command
        self.CMD_SET = 0x2F     # Set position command
        
        
        
    def _encode_position(self, alt: float, az: float) -> bytes:
        """
        Encode altitude and azimuth into MD-01 protocol format.
        
        Position encoding:
        - Add 360 to normalize to positive range
        - H1-H4: Azimuth as 4 ASCII digits (hundreds, tens, ones, tenths)
        - V1-V4: Elevation as 4 ASCII digits (hundreds, tens, ones, tenths)
        
        :param alt: Altitude in degrees
        :param az: Azimuth in degrees
        :return: 12-byte response packet
        """
        PH = 10 # Pulses per degree, 0A in hex
        PV = 10 # Pulses per degree, 0A in hex
        H = str(int(PH * (360+az)))
        H1 = "0"+H[0]
        H2 = "0"+H[1]
        H3 = "0"+H[2]
        H4 = "0"+H[3]
        V = str(int(PV * (360+alt)))
        V1 = "0"+V[0]
        V2 = "0"+V[1]
        V3 = "0"+V[2]
        V4 = "0"+V[3]
        msg = bytes.fromhex("57"+H1+H2+H3+H4+"0A"+V1+V2+V3+V4+"0A20")
        
        return msg
    
    def _decode_position(self, cmd: bytes) -> tuple:
        """
        Decode altitude and azimuth from MD-01 command packet.
        
        :param cmd: 13-byte command packet
        :return: (alt, az) tuple in degrees
        """

        ans = cmd.hex()
        H1 = float(ans[2:4]) - 30
        H2 = float(ans[4:6]) - 30
        H3 = float(ans[6:8]) - 30
        H4 = float(ans[8:10]) - 30
        V1 = float(ans[12:14]) - 30
        V2 = float(ans[14:16]) - 30
        V3 = float(ans[16:18]) - 30
        V4 = float(ans[18:20]) - 30
        # Calculate angles for AltAz
        az = H1 * 100 + H2 * 10 + H3 + H4 / 10 -360
        alt = V1 * 100 + V2 * 10 + V3 + V4 / 10 -360

        return alt, az
    
    def _process_command(self, cmd: bytes) -> bytes:
        """
        Process a command and return appropriate response.
        
        :param cmd: 13-byte command packet
        :return: 12-byte response packet
        """
        if len(cmd) != 13:
            logger.warning(f"Invalid command length: {len(cmd)} bytes")
            return self._encode_position(self.current_alt, self.current_az)
        
        if cmd[0] != self.START_BYTE or cmd[12] != self.END_BYTE:
            logger.warning("Invalid command format: wrong start/end bytes")
            return self._encode_position(self.current_alt, self.current_az)
        
        command_type = cmd[11]
        
        if command_type == self.CMD_STOP:
            logger.info("STOP command received")
            self.is_moving = False
            self.target_alt = self.current_alt
            self.target_az = self.current_az
            
        elif command_type == self.CMD_STATUS:
            logger.info(f"STATUS command received - Current position: Alt={self.current_alt:.1f}°, Az={self.current_az:.1f}°")
            
        elif command_type == self.CMD_SET:
            target_alt, target_az = self._decode_position(cmd)
            logger.info(f"SET command received - Target position: Alt={target_alt:.1f}°, Az={target_az:.1f}°")
            self.target_alt = target_alt
            self.target_az = target_az
            self.is_moving = True
            
        else:
            logger.warning(f"Unknown command type: 0x{command_type:02X}")
        
        return self._encode_position(self.current_alt, self.current_az)
    
    def _update_position(self):
        """Background thread to simulate rotator movement."""
        while self.running:
            if self.is_moving:
                # Calculate deltas
                delta_alt = self.target_alt - self.current_alt
                delta_az = self.target_az - self.current_az
                
                # Check if we're close enough
                if abs(delta_alt) < 0.1 and abs(delta_az) < 0.1:
                    self.current_alt = self.target_alt
                    self.current_az = self.target_az
                    self.is_moving = False
                    logger.info(f"Reached target position: Alt={self.current_alt:.1f}°, Az={self.current_az:.1f}°")
                else:
                    # Move towards target at slew_rate
                    dt = 0.1  # Update every 100ms
                    max_move = self.slew_rate * dt
                    
                    # Move altitude
                    if abs(delta_alt) > max_move:
                        self.current_alt += max_move if delta_alt > 0 else -max_move
                    else:
                        self.current_alt = self.target_alt
                    
                    # Move azimuth
                    if abs(delta_az) > max_move:
                        self.current_az += max_move if delta_az > 0 else -max_move
                    else:
                        self.current_az = self.target_az
                    
                    logger.debug(f"Moving to target: Alt={self.current_alt:.1f}°, Az={self.current_az:.1f}°")
            
            time.sleep(0.1)
    
    def _handle_client(self, client_socket, address):
        """Handle a single client connection."""
        logger.info(f"Connection from {address}")
        
        try:
            # Receive command (13 bytes)
            data = client_socket.recv(13)
            
            if data:
                logger.info(f"Received: {data.hex()}")
                
                # Process command and generate response
                response = self._process_command(data)
                
                # Send response
                client_socket.send(response)
                logger.info(f"Sent: {response.hex()}")
        
        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
        
        finally:
            client_socket.close()
            logger.debug(f"Connection closed: {address}")
    
    def start(self):
        """Start the simulator server."""
        self.running = True
        
        # Start position update thread
        update_thread = threading.Thread(target=self._update_position, daemon=True)
        update_thread.start()
        
        # Create server socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            logger.info(f"MD-01 Simulator listening on {self.host}:{self.port}")
            logger.info(f"Initial position: Alt={self.current_alt:.1f}°, Az={self.current_az:.1f}°")
            
            while self.running:
                try:
                    # Accept connection
                    client_socket, address = server_socket.accept()
                    
                    # Handle client in separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                
                except KeyboardInterrupt:
                    logger.info("Shutting down simulator...")
                    break
                except Exception as e:
                    logger.error(f"Server error: {e}")
        
        finally:
            server_socket.close()
            self.running = False
            logger.info("Simulator stopped")


if __name__ == "__main__":
    # Parse command-line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='MD-01 Rotator Controller Simulator')
    parser.add_argument('--host', default='192.168.0.2', help='Host address to bind to')
    parser.add_argument('--port', type=int, default=65000, help='Port to listen on')
    parser.add_argument('--alt', type=float, default=45.0, help='Initial altitude (degrees)')
    parser.add_argument('--az', type=float, default=180.0, help='Initial azimuth (degrees)')
    parser.add_argument('--rate', type=float, default=10.0, help='Slew rate (degrees/second)')
    
    args = parser.parse_args()
    
    # Create and start simulator
    simulator = MD01Simulator(host=args.host, port=args.port)
    simulator.current_alt = args.alt
    simulator.current_az = args.az
    simulator.slew_rate = args.rate
    
    try:
        simulator.start()
    except KeyboardInterrupt:
        logger.info("\nSimulator interrupted by user")
        sys.exit(0)

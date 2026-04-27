#!/usr/bin/env python3
"""
UDP -> ROS 2 bridge for RTES raw telemetry packets.

Packet format (network byte order, 20 bytes total):
- uint32 magic (0x52544553, ASCII: RTES)
- uint32 sequence
- uint32 raw_distance_bits (IEEE-754 float bits)
- uint32 filtered_distance_bits (IEEE-754 float bits)
- uint8  fault (0 or 1)
- uint8  state (0=SAFE, 1=WARNING, 2=DANGER, 3=FAULT)
- uint16 reserved
"""

import argparse
import json
import socket
import struct
import sys
from typing import Tuple

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from std_msgs.msg import Bool, Float32, Int32, String


PACKET_FORMAT = "!IIIIBBH"
PACKET_SIZE = struct.calcsize(PACKET_FORMAT)
PACKET_MAGIC = 0x52544553


def bits_to_float32(bits: int) -> float:
    return struct.unpack("!f", struct.pack("!I", bits))[0]


def decode_packet(data: bytes) -> Tuple[int, float, float, bool, int]:
    if len(data) != PACKET_SIZE:
        raise ValueError(f"invalid packet size: got {len(data)}, expected {PACKET_SIZE}")

    magic, sequence, raw_bits, filtered_bits, fault, state, _reserved = struct.unpack(PACKET_FORMAT, data)
    if magic != PACKET_MAGIC:
        raise ValueError(f"invalid magic: 0x{magic:08X}")

    raw_distance = bits_to_float32(raw_bits)
    filtered_distance = bits_to_float32(filtered_bits)
    return sequence, raw_distance, filtered_distance, bool(fault), state


class UdpToRosBridge(Node):
    def __init__(self, bind_ip: str, bind_port: int) -> None:
        super().__init__("rtes_udp_bridge")

        qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
        )

        self.distance_pub = self.create_publisher(Float32, "/distance", qos)
        self.filtered_distance_pub = self.create_publisher(Float32, "/distance_filtered", qos)
        self.fault_pub = self.create_publisher(Bool, "/fault", qos)
        self.state_code_pub = self.create_publisher(Int32, "/parking_state_code", qos)
        self.state_text_pub = self.create_publisher(String, "/parking_state", qos)
        self.sequence_pub = self.create_publisher(Int32, "/sequence", qos)
        self.telemetry_pub = self.create_publisher(String, "/telemetry_raw", qos)

        self.state_map = {
            0: "SAFE",
            1: "WARNING",
            2: "DANGER",
            3: "FAULT",
        }

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((bind_ip, bind_port))
        self.sock.settimeout(0.2)

        self.get_logger().info(f"listening on UDP {bind_ip}:{bind_port}, packet_size={PACKET_SIZE}")
        self.get_logger().info(
            "publishing topics: /distance, /distance_filtered, /fault, "
            "/parking_state_code, /parking_state, /sequence, /telemetry_raw"
        )
        self.timer = self.create_timer(0.01, self._poll_once)

    def _poll_once(self) -> None:
        try:
            data, addr = self.sock.recvfrom(2048)
        except socket.timeout:
            return
        except OSError as exc:
            self.get_logger().error(f"socket error: {exc}")
            return

        try:
            sequence, raw_distance, filtered_distance, fault, state = decode_packet(data)
        except ValueError as exc:
            self.get_logger().warn(f"drop packet from {addr}: {exc}")
            return

        distance_msg = Float32()
        distance_msg.data = raw_distance
        self.distance_pub.publish(distance_msg)

        filtered_msg = Float32()
        filtered_msg.data = filtered_distance
        self.filtered_distance_pub.publish(filtered_msg)

        fault_msg = Bool()
        fault_msg.data = fault
        self.fault_pub.publish(fault_msg)

        state_code_msg = Int32()
        state_code_msg.data = int(state)
        self.state_code_pub.publish(state_code_msg)

        state_text_msg = String()
        state_text_msg.data = self.state_map.get(state, "UNKNOWN")
        self.state_text_pub.publish(state_text_msg)

        sequence_msg = Int32()
        sequence_msg.data = int(sequence)
        self.sequence_pub.publish(sequence_msg)

        telemetry_msg = String()
        telemetry_msg.data = json.dumps(
            {
                "sequence": int(sequence),
                "raw_distance": float(raw_distance),
                "filtered_distance": float(filtered_distance),
                "fault": bool(fault),
                "state_code": int(state),
                "state_text": state_text_msg.data,
            }
        )
        self.telemetry_pub.publish(telemetry_msg)

        self.get_logger().info(
            f"seq={sequence} raw={raw_distance:.2f} filtered={filtered_distance:.2f} "
            f"fault={int(fault)} state={state_text_msg.data} from={addr[0]}:{addr[1]}"
        )

    def destroy_node(self) -> bool:
        try:
            self.sock.close()
        finally:
            return super().destroy_node()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge RTES UDP telemetry to ROS 2 topics")
    parser.add_argument("--bind-ip", default="0.0.0.0", help="IP address to bind UDP listener")
    parser.add_argument("--port", type=int, default=8888, help="UDP port to bind")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    rclpy.init(args=None)
    node = UdpToRosBridge(args.bind_ip, args.port)

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())

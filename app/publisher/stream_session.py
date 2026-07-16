from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from .packet import Packet
from .worker import Worker


@dataclass
class StreamSession:

    # Identity

    stream_id: str
    device_id: str

    # Stream Information

    codec: str = "h264"

    width: int = 0
    height: int = 0
    fps: int = 0

    bitrate: int = 0

    # Runtime

    worker: Optional[Worker] = None

    connected: bool = True

    connected_at: float = field(default_factory=time.time)

    last_packet_at: float = field(default_factory=time.time)

    packets_received: int = 0

    bytes_received: int = 0

    last_sequence: int = -1

    dropped_packets: int = 0

    # Codec Configuration

    sps: Optional[bytes] = None

    pps: Optional[bytes] = None

    vps: Optional[bytes] = None

    # Session Methods

    def attach_worker(self, worker: PublisherWorker) -> None:

        self.worker = worker

    def on_packet(self, packet: Packet) -> None:
        self.last_packet_at = time.time()
        self.packets_received += 1

        # FIX: Measure the underlying raw payload array, not the Packet wrapper instance
        self.bytes_received += len(packet.payload)

        # Packet loss detection
        if self.last_sequence != -1:
            expected = (self.last_sequence + 1) % 65536
            if packet.sequence > expected:
                self.dropped_packets += (packet.sequence - expected)

        self.last_sequence = packet.sequence

        # Save codec configuration configurations dynamically
        if packet.is_config:
            if self.sps is None:
                self.sps = packet.payload
            elif self.pps is None:
                self.pps = packet.payload

        if self.worker is not None:
            self.worker.publish(packet)

    def disconnect(self) -> None:

        self.connected = False

        if self.worker is not None:
            self.worker.stop()

    @property
    def uptime(self) -> float:
        return time.time() - self.connected_at

    @property
    def bitrate_mbps(self) -> float:
        if self.uptime <= 0:
            return 0.0

        return (self.bytes_received * 8) / self.uptime / 1_000_000

    def __repr__(self):
        return (
            f"StreamSession("
            f"stream='{self.stream_id}', "
            f"device='{self.device_id}', "
            f"packets={self.packets_received}, "
            f"bytes={self.bytes_received}, "
            f"dropped={self.dropped_packets})"
        )
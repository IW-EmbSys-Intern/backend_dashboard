# app/publisher/worker.py

from __future__ import annotations

import logging
import queue
import threading
from abc import ABC, abstractmethod
from typing import List, Optional

from .packet import Packet

logger = logging.getLogger(__name__)


class Consumer(ABC):

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    @abstractmethod
    def on_packet(self, packet: Packet) -> None:
        raise NotImplementedError


class Worker(threading.Thread):


    def __init__(
        self,
        stream_id: str,
    ):
        super().__init__(daemon=True)

        self.stream_id = stream_id


        self._running = threading.Event()
        self._running.set()

        # Incoming Packet Queue

        self._packet_queue: queue.Queue[Packet] = queue.Queue(maxsize=1000)

        # Registered Consumers

        self._consumers: List[Consumer] = []

        # Latest Packet
        self.latest_packet: Optional[Packet] = None

        # Statistics
        self.total_packets = 0
        self.total_bytes = 0

    # Consumer Management

    def add_consumer(self, consumer: Consumer) -> None:

        if consumer in self._consumers:
            return

        self._consumers.append(consumer)

        logger.info(
            "Consumer %s attached to stream %s",
            consumer.__class__.__name__,
            self.stream_id,
        )

    def remove_consumer(self, consumer: Consumer) -> None:

        if consumer in self._consumers:
            self._consumers.remove(consumer)

            logger.info(
                "Consumer %s removed from stream %s",
                consumer.__class__.__name__,
                self.stream_id,
            )

    @property
    def consumers(self) -> List[Consumer]:
        return list(self._consumers)

    # Packet Input

    def publish(self, packet: Packet) -> None:
        """
        Called by Session whenever a packet arrives.
        """

        try:
            self._packet_queue.put_nowait(packet)

        except queue.Full:

            logger.warning(
                "Packet queue full for stream %s. Packet dropped.",
                self.stream_id,
            )

    # Thread
    def run(self) -> None:

        logger.info("Worker started [%s]", self.stream_id)

        # Start all consumers

        for consumer in self._consumers:

            try:
                consumer.start()

            except Exception:

                logger.exception(
                    "Failed to start consumer %s",
                    consumer.__class__.__name__,
                )

        # Main Loop

        while self._running.is_set():

            try:
                packet = self._packet_queue.get(timeout=1)

                print(
                    "WORKER RECEIVED:",
                    self.stream_id,
                    packet.sequence,
                    packet.pts
                )

                print(
                    "PACKET INFO:",
                    "SEQ=", packet.sequence,
                    "CONFIG=", packet.is_config,
                    "KEYFRAME=", packet.is_keyframe,
                    "SIZE=", len(packet.payload)  # FIX: Changed from len(packet
                # )
                )

            except queue.Empty:
                continue

            self.latest_packet = packet

            self.total_packets += 1
            self.total_bytes += len(packet.payload)  # FIX: Changed from len(packet)
            # Fan-out

            for consumer in list(self._consumers):

                try:
                    consumer.on_packet(packet)

                except Exception:

                    logger.exception(
                        "Consumer %s raised an exception",
                        consumer.__class__.__name__,
                    )

        # Stop Consumers

        for consumer in self._consumers:

            try:
                consumer.stop()

            except Exception:

                logger.exception(
                    "Failed stopping consumer %s",
                    consumer.__class__.__name__,
                )

        logger.info("Worker stopped [%s]", self.stream_id)

    # Lifecycle

    def stop(self) -> None:
        self._running.clear()

    # Statistics

    @property
    def consumer_count(self) -> int:
        return len(self._consumers)

    @property
    def queue_size(self) -> int:
        return self._packet_queue.qsize()

    @property
    def bitrate_bits(self) -> int:
        return self.total_bytes * 8

    # Debug

    def __repr__(self) -> str:

        return (
            f"Worker("
            f"stream='{self.stream_id}', "
            f"packets={self.total_packets}, "
            f"consumers={len(self._consumers)})"
        )
# app/core/publisher/publisher_worker.py
import threading
import queue
import time
import logging
from typing import List, Any
from app.core.publisher.packet import Packet

logger = logging.getLogger(__name__)


class PublisherWorker:
    def __init__(self, camera_id: str):
        self.camera_id = camera_id
        self.running = False

        self.lock = threading.Lock()
        self.thread = None

        # Dedicated inbound thread-safe queue
        self.packet_queue = queue.Queue(maxsize=120)

        # Thread-safe consumers list (RecordingWorker, RTSP, AI, etc.)
        self.consumers = []
        self.consumer_lock = threading.Lock()

        # Diagnostics
        self.packet_counter = 0
        self.last_fps_time = time.time()

    def start(self):
        with self.lock:
            if self.running:
                return
            self.running = True
            self.thread = threading.Thread(
                target=self._run,
                daemon=True,
                name=f"PubWorker-{self.camera_id}"
            )
            self.thread.start()
        logger.info(f"PublisherWorker started for camera: {self.camera_id}")

    def _run(self):
        while self.running:
            try:
                packet: Packet = self.packet_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            self.packet_counter += 1
            now = time.time()
            if now - self.last_fps_time >= 5.0:
                fps = self.packet_counter / (now - self.last_fps_time)
                logger.info(f"Camera [{self.camera_id}] health: {fps:.2f} Pkts/Sec")
                self.packet_counter = 0
                self.last_fps_time = now

            with self.consumer_lock:
                active_consumers = list(self.consumers)

            for consumer in active_consumers:
                try:
                    consumer.write_packet(packet)
                except Exception as e:
                    logger.error(f"Failed passing packet to consumer {consumer.__class__.__name__}: {e}")

    def push_packet(self, packet: Packet):
        if not self.running:
            return
        try:
            self.packet_queue.put_nowait(packet)
        except queue.Full:
            logger.warning(f"Packet buffer full for camera {self.camera_id}. Dropping frame.")

    def add_consumer(self, consumer: Any):
        with self.consumer_lock:
            if consumer not in self.consumers:
                self.consumers.append(consumer)
                logger.info(f"Added consumer {consumer.__class__.__name__} to camera {self.camera_id}")

    def remove_consumer(self, consumer: Any):
        with self.consumer_lock:
            if consumer in self.consumers:
                self.consumers.remove(consumer)
                logger.info(f"Removed consumer {consumer.__class__.__name__} from camera {self.camera_id}")

    def stop(self):
        with self.lock:
            if not self.running:
                return
            self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
        logger.info(f"PublisherWorker stopped for camera: {self.camera_id}")
# app/core/publisher/rtsp_publisher.py
import subprocess
import threading
import queue
import logging
from app.core.publisher.packet import Packet

logger = logging.getLogger(__name__)


class RtspPublisher:
    def __init__(self, stream_key: str, target_rtsp_url: str):
        self.stream_key = stream_key
        # Dynamically set target URL passed from the user/Android client handshake
        self.rtsp_url = target_rtsp_url

        self.running = False
        self.thread = None
        self.process = None
        self.packet_queue = queue.Queue(maxsize=120)
        self.lock = threading.Lock()

    def start(self):
        with self.lock:
            if self.running:
                return

            ffmpeg_cmd = [
                'ffmpeg',
                '-y',
                '-f', 'h264',
                '-avoid_negative_ts', 'make_zero',
                '-i', 'pipe:0',  # Ingest raw H264 bytes from stdin
                '-c:v', 'copy',  # Zero re-encoding overhead
                '-f', 'rtsp',
                '-rtsp_transport', 'tcp',  # Enforce clean TCP transmission
                self.rtsp_url  # Dynamic target URL applied here
            ]

            try:
                self.process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                self.running = True
                self.thread = threading.Thread(target=self._run, daemon=True, name=f"RtspPub-{self.stream_key}")
                self.thread.start()
                logger.info(f"Dynamic RTSP Proxy initiated for stream [{self.stream_key}] -> {self.rtsp_url}")
            except Exception as e:
                logger.error(f"Failed to kick off dynamic publisher process instance: {e}")

    def _run(self):
        while self.running:
            try:
                packet: Packet = self.packet_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                if self.process and self.process.stdin:
                    self.process.stdin.write(packet.data)
                    self.process.stdin.flush()
            except IOError as e:
                logger.error(f"Broken pipe error on RTSP FFmpeg instance for [{self.stream_key}]: {e}")
                break

    def write_packet(self, packet: Packet):
        if not self.running:
            return
        try:
            self.packet_queue.put_nowait(packet)
        except queue.Full:
            logger.warning(f"RTSP proxy queue saturated for [{self.stream_key}]. Dropping packet.")

    def stop(self):
        with self.lock:
            if not self.running:
                return
            self.running = False

        if self.thread:
            self.thread.join(timeout=2.0)

        if self.process:
            try:
                if self.process.stdin:
                    self.process.stdin.close()
                self.process.wait(timeout=3.0)
            except Exception as e:
                logger.error(f"Error closing down dynamic publisher process: {e}")
        logger.info(f"RTSP transmission disconnected for stream: {self.stream_key}")
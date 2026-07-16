from __future__ import annotations

import subprocess
import threading
import logging
import queue
import os
import time
from typing import Optional

from .packet import Packet
from .worker import Consumer

logger = logging.getLogger(__name__)


class H264Recorder(Consumer):

    def __init__(
            self,
            stream_id: str,
            storage_dir: str = "storage/recordings"
    ):
        self.stream_id = stream_id
        self.storage_dir = os.path.join(storage_dir, stream_id)

        self._queue: queue.Queue[Optional[Packet]] = queue.Queue(maxsize=500)
        self._thread: Optional[threading.Thread] = None
        self._running = False

        self.process: Optional[subprocess.Popen] = None
        self.current_file_path: Optional[str] = None

        # State indicators
        self.sps: Optional[bytes] = None
        self.pps: Optional[bytes] = None
        self.recording_active = False

        self.total_packets = 0
        self.total_bytes = 0

    def start(self) -> None:
        """Starts the background recording thread loop."""
        if self._running:
            return

        os.makedirs(self.storage_dir, exist_ok=True)
        self._running = True
        self._thread = threading.Thread(
            target=self._recorder_loop,
            name=f"H264RecLoop-{self.stream_id}",
            daemon=True
        )
        self._thread.start()
        logger.info("H264 Recording infrastructure started for %s", self.stream_id)

    def on_packet(self, packet: Packet) -> None:
        """Fast, non-blocking handoff called by the main Worker thread."""
        if not self._running:
            return

        try:
            self._queue.put_nowait(packet)
        except queue.Full:
            logger.error("Recording queue full for stream %s. Dropping frame from disk cache.", self.stream_id)

    def _start_muxing_session(self) -> bool:
        """Spawns an optimized copy-muxer instance targeting an MP4 output container."""
        self._close_muxing_session()

        timestamp = int(time.time())
        filename = f"rec_{timestamp}.mp4"
        self.current_file_path = os.path.join(self.storage_dir, filename)

        # FFmpeg configuration for container muxing without frame transcoding
        command = [
            "ffmpeg",
            "-y",
            "-f", "h264",
            # Use source packet presentation timestamps strictly
            "-i", "pipe:0",
            "-c:v", "copy",
            "-movflags", "faststart",  # Repositions index details to the front for smooth web playback
            self.current_file_path
        ]

        try:
            self.process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                bufsize=0
            )
            threading.Thread(target=self._read_stderr, args=(self.process,), daemon=True).start()
            logger.info("Started new recording file segmentation: %s", self.current_file_path)
            return True
        except Exception as e:
            logger.error("Failed to spawn recording sub-process for %s: %s", self.stream_id, e)
            return False

    def _read_stderr(self, proc: subprocess.Popen) -> None:
        """Consumes logs to prevent deadlocks from full OS pipeline buffers."""
        if not proc.stderr:
            return
        try:
            for line in iter(proc.stderr.readline, b''):
                log_str = line.decode('utf-8', errors='ignore').strip()
                if "Error" in log_str:
                    logger.error("[Recorder-FFmpeg-%s] %s", self.stream_id, log_str)
        except Exception:
            pass

    def _recorder_loop(self) -> None:
        """Main internal loop handling segment lifecycles and physical frame emission."""
        while self._running:
            try:
                packet = self._queue.get(timeout=1.0)
                if packet is None:
                    continue

                # 1. Capture stream initialization context dynamically
                if packet.is_config:
                    self._extract_config(packet.payload)
                    continue

                # 2. Keyframe enforcement: Ensure recording starts cleanly on an IDR boundary
                if not self.recording_active:
                    if packet.is_keyframe and self.sps and self.pps:
                        if self._start_muxing_session():
                            self.recording_active = True
                        else:
                            continue
                    else:
                        # Skip predictive frames preceding the initial keyframe cluster
                        continue

                # 3. Detect process health states
                if self.process is None or self.process.poll() is not None:
                    logger.warning("Recording process severed for %s. Resetting state.", self.stream_id)
                    self.recording_active = False
                    continue

                # 4. Prepare stream payload packet data maps
                payload = self._prepare_payload(packet)

                # 5. Push payload to process pipe
                try:
                    self.process.stdin.write(payload)
                    self.process.stdin.flush()
                    self.total_packets += 1
                    self.total_bytes += len(payload)
                except (BrokenPipeError, OSError) as e:
                    logger.error("Recorder pipeline standard input dropped for %s: %s", self.stream_id, e)
                    self._close_muxing_session()
                    self.recording_active = False

            except queue.Empty:
                continue
            except Exception as ex:
                logger.exception("Unexpected error inside recording engine workflow loop for %s: %s", self.stream_id,
                                 ex)

    def _extract_config(self, payload: bytes) -> None:
        """Extracts configuration bytes."""
        if b'\x00\x00\x00\x01\x67' in payload or b'\x00\x00\x01\x67' in payload:
            self.sps = payload
        elif b'\x00\x00\x00\x01\x68' in payload or b'\x00\x00\x01\x68' in payload:
            self.pps = payload

    def _prepare_payload(self, packet: Packet) -> bytes:
        """Guarantees explicit start code formatting across packet boundaries."""
        out = bytearray()

        # Inject metadata initialization tokens directly into keyframe segments
        if packet.is_keyframe and self.sps and self.pps:
            if not self.sps.startswith(b"\x00\x00\x00\x01") and not self.sps.startswith(b"\x00\x00\x01"):
                out.extend(b"\x00\x00\x00\x01")
            out.extend(self.sps)

            if not self.pps.startswith(b"\x00\x00\x00\x01") and not self.pps.startswith(b"\x00\x00\x01"):
                out.extend(b"\x00\x00\x00\x01")
            out.extend(self.pps)

        if not packet.payload.startswith(b"\x00\x00\x00\x01") and not packet.payload.startswith(b"\x00\x00\x01"):
            out.extend(b"\x00\x00\x00\x01")

        out.extend(packet.payload)
        return bytes(out)

    def _close_muxing_session(self) -> None:
        """Closes files and clears process resources cleanly."""
        if self.process:
            try:
                if self.process.stdin:
                    self.process.stdin.close()
            except Exception:
                pass
            try:
                self.process.terminate()
                self.process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                try:
                    self.process.kill()
                except Exception:
                    pass
            except Exception:
                pass
            self.process = None
        self.current_file_path = None

    def stop(self) -> None:
        """Gracefully shuts down the recorder thread and saves the active recording."""
        if not self._running:
            return

        logger.info("Stopping H264 recorder consumer loop for %s", self.stream_id)
        self._running = False
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

        if self._thread:
            self._thread.join(timeout=3.0)
            self._thread = None

        self._close_muxing_session()
        self.recording_active = False

    def __repr__(self) -> str:
        return f"H264Recorder(stream={self.stream_id}, recording={self.recording_active}, bytes_written={self.total_bytes})"
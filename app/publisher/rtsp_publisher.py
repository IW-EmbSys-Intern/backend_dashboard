from __future__ import annotations

import subprocess
import threading
import logging
import queue
import time
from typing import Optional

from .packet import Packet
from .worker import Consumer

logger = logging.getLogger(__name__)


class RtspPublisher(Consumer):

    def __init__(
            self,
            stream_id: str,
            rtsp_url: str,
            width: int = 1280,
            height: int = 720,
            fps: int = 30,
    ):
        self.stream_id = stream_id
        self.rtsp_url = rtsp_url
        self.width = width
        self.height = height
        self.fps = fps

        self.process: Optional[subprocess.Popen] = None
        self._queue: queue.Queue[Optional[Packet]] = queue.Queue(maxsize=300)  # Isolated buffer (~10s of video max)
        self._thread: Optional[threading.Thread] = None
        self._running = False

        self.total_packets = 0
        self.total_bytes = 0

        # Codec State Management
        self.sps: Optional[bytes] = None
        self.pps: Optional[bytes] = None
        self.initialized_codec = False

    def start(self) -> None:
        """Initializes the background loop worker. Does not spawn FFmpeg yet."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._publish_loop,
            name=f"RtspPubLoop-{self.stream_id}",
            daemon=True
        )
        self._thread.start()
        logger.info("RTSP Consumer infrastructure started for %s", self.stream_id)

    def on_packet(self, packet: Packet) -> None:
        """
        Fast, non-blocking handoff executed by the main Worker thread.
        Never performs direct I/O writes or heavy parsing here.
        """
        if not self._running:
            return

        try:
            self._queue.put_nowait(packet)
        except queue.Full:
            logger.warning(
                "RTSP publisher queue full for stream %s. Dropping streaming frame.",
                self.stream_id
            )

    def _spawn_ffmpeg(self) -> bool:
        """Spawns an optimized, live low-overhead stream copy FFmpeg instance."""
        self._terminate_ffmpeg()

        # Production low-latency stream-copy parameter mapping
        command = [
            "ffmpeg",
            "-y",
            "-f", "h264",
            # Trust the incoming stream timestamps rather than internal Wallclock simulation
            "-i", "pipe:0",
            "-c:v", "copy",
            "-f", "rtsp",
            "-rtsp_transport", "tcp",  # Enforce TCP transport to eliminate UDP frame drops
            self.rtsp_url
        ]

        try:
            self.process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                bufsize=0  # Completely unbuffered pipeline input for instant data delivery
            )
            # Spawn a reader thread to prevent FFmpeg's stderr buffer from locking up the process
            threading.Thread(target=self._read_stderr, args=(self.process,), daemon=True).start()
            logger.info("Spawned new FFmpeg process instance for stream %s", self.stream_id)
            return True
        except Exception as e:
            logger.error("Failed to spawn FFmpeg engine process for %s: %s", self.stream_id, e)
            return False

    def _read_stderr(self, proc: subprocess.Popen) -> None:
        """Consumes FFmpeg logs to prevent sub-process deadlocks."""
        if not proc.stderr:
            return
        try:
            for line in iter(proc.stderr.readline, b''):
                log_str = line.decode('utf-8', errors='ignore').strip()
                if "Error" in log_str or "severe" in log_str:
                    logger.error("[FFmpeg-%s] %s", self.stream_id, log_str)
        except Exception:
            pass

    def _publish_loop(self) -> None:
        """Isolated working thread loop handling process mutations and I/O execution."""
        while self._running:
            try:
                packet = self._queue.get(timeout=1.0)
                if packet is None:
                    continue

                # 1. Update active configurations immediately
                if packet.is_config:
                    self._update_codec_config(packet.payload)
                    continue

                # 2. Block outbound transmission until format boundaries are fully known
                if not self.initialized_codec:
                    continue

                # 3. Handle live runtime process recreation
                if self.process is None or self.process.poll() is not None:
                    # Enforce that a new process must start cleanly from a Keyframe boundary
                    if not packet.is_keyframe:
                        continue

                    logger.warning("FFmpeg process is dead/missing for %s. Re-spawning pipeline.", self.stream_id)
                    if not self._spawn_ffmpeg():
                        time.sleep(2.0)  # Rate limit aggressive looping crashes
                        continue

                # 4. Construct the Annex-B safe frame data stream
                payload_to_write = self._prepare_payload(packet)

                # 5. Pipeline push and robust failure recovery
                try:
                    self.process.stdin.write(payload_to_write)
                    self.process.stdin.flush()

                    self.total_packets += 1
                    self.total_bytes += len(payload_to_write)
                except (BrokenPipeError, OSError) as pipe_err:
                    logger.error("FFmpeg stdin pipe severed for stream %s: %s", self.stream_id, pipe_err)
                    self._terminate_ffmpeg()

            except queue.Empty:
                continue
            except Exception as ex:
                logger.exception("Unexpected exception inside RTSP engine thread loop for %s: %s", self.stream_id, ex)

    def _update_codec_config(self, payload: bytes) -> None:
        """Parses and updates the active configuration maps dynamically."""
        # Detect if it's an SPS or PPS block based on NAL Unit type octet markers
        header_index = self._find_nal_header_index(payload)
        if header_index != -1 and len(payload) > header_index:
            nal_type = payload[header_index] & 0x1F
            if nal_type == 7:
                self.sps = payload
            elif nal_type == 8:
                self.pps = payload

        if self.sps is not None and self.pps is not None:
            self.initialized_codec = True

    def _find_nal_header_index(self, payload: bytes) -> int:
        """Finds where the NAL unit type byte begins after the start code prefix."""
        if payload.startswith(b"\x00\x00\x00\x01"):
            return 4
        elif payload.startswith(b"\x00\x00\x01"):
            return 3
        return -1

    def _prepare_payload(self, packet: Packet) -> bytes:
        """Ensures frame chunks carry valid start codes and appends SPS/PPS before IDR frames."""
        out = bytearray()

        # If it's a critical IDR keyframe, prepend cached SPS and PPS sequences
        if packet.is_keyframe and self.sps and self.pps:
            if self._find_nal_header_index(self.sps) == -1:
                out.extend(b"\x00\x00\x00\x01")
            out.extend(self.sps)

            if self._find_nal_header_index(self.pps) == -1:
                out.extend(b"\x00\x00\x00\x01")
            out.extend(self.pps)

        # Append the primary frame payload
        if self._find_nal_header_index(packet.payload) == -1:
            out.extend(b"\x00\x00\x00\x01")
        out.extend(packet.payload)

        return bytes(out)

    def _terminate_ffmpeg(self) -> None:
        """Safely cleans up process handles and open resources."""
        if self.process:
            logger.info("Terminating FFmpeg instance for %s", self.stream_id)
            try:
                if self.process.stdin:
                    self.process.stdin.close()
            except Exception:
                pass
            try:
                self.process.terminate()
                self.process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                try:
                    self.process.kill()
                except Exception:
                    pass
            except Exception:
                pass
            self.process = None

    def stop(self) -> None:
        """Gracefully tears down the worker pipeline loops and sub-processes."""
        if not self._running:
            return

        logger.info("Stopping RTSP publisher framework cleanly for %s", self.stream_id)
        self._running = False
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

        self._terminate_ffmpeg()

    def __repr__(self) -> str:
        return f"RtspPublisher(stream={self.stream_id}, running={self._running}, processed_packets={self.total_packets})"
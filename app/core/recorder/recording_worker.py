import os
import subprocess
import threading
import queue
import logging
from datetime import datetime, timezone
from app.core.publisher.packet import Packet
from app.models.recording import Recording
from app.core.recorder.recording_storage import RecordingStorage

logger = logging.getLogger(__name__)


class RecordingWorker:
    def __init__(self, camera_id: str, base_directory: str = "storage/recordings"):
        self.camera_id = camera_id

        self.running = False
        self.thread = None
        self.process = None
        self.packet_queue = queue.Queue(maxsize=120)

        self.recording_metadata = None
        self.first_pts = None
        self.lock = threading.Lock()

        self.storage = RecordingStorage(base_directory)

    def start(self):
        with self.lock:
            if self.running:
                return

            folder, filename, file_path = self.storage.create_recording(self.camera_id)

            # Metadata initialization bound to camera_id
            self.recording_metadata = Recording(
                camera_id=self.camera_id,
                base_directory="storage/recordings",
                filename=filename,
                file_path=file_path,
                status="recording",
                started_at=datetime.now(timezone.utc)
            )

            # Spawn non-blocking FFmpeg process configured for raw H264 stdin copying
            ffmpeg_cmd = [
                'ffmpeg',
                '-y',
                '-f', 'h264',
                '-use_wallclock_as_timestamps', '0',
                '-i', 'pipe:0',
                '-c:v', 'copy',
                '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
                # Write fragmented moov headers live                str(self.recording_metadata.file_path)
            ]

            self.process = subprocess.Popen(
                ffmpeg_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            self.running = True
            self.first_pts = None

            self.thread = threading.Thread(
                target=self._run,
                daemon=True,
                name=f"RecWorker-Cam-{self.camera_id}"
            )
            self.thread.start()

        logger.info(f"H264 recorder started for camera {self.camera_id} -> {filename}")

    def _run(self):
        while self.running or not self.packet_queue.empty():
            try:
                packet: Packet = self.packet_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            # Ensure frames wait until a keyframe / sync point before writing
            if self.first_pts is None:
                if not packet.is_keyframe:
                    continue
                self.first_pts = packet.pts

            try:
                if self.process.poll() is not None:
                    logger.error(f"FFmpeg process exited unexpectedly for camera {self.camera_id}")
                    break
                if self.process and self.process.stdin:
                    self.process.stdin.write(packet.data)
                    self.process.stdin.flush()
            except IOError as e:
                logger.error(f"FFmpeg stream pipe failure for camera {self.camera_id}: {e}")
                break

    def write_packet(self, packet: Packet):
        if not self.running:
            return
        try:
            self.packet_queue.put_nowait(packet)
        except queue.Full:
            logger.warning(f"Recording buffer full for camera {self.camera_id}. Frame dropped.")

    def consume(self, packet: Packet):
        self.write_packet(packet)

    def stop(self):
        with self.lock:
            if not self.running:
                return
            self.running = False

        if self.thread:
            self.thread.join(timeout=2.0)

        # Close stdin cleanly and calculate final metadata
        if self.process:
            try:
                if self.process.stdin:
                    self.process.stdin.flush()  # Push any buffered tail bytes
                    self.process.stdin.close()
                self.process.wait(timeout=5.0)
            except Exception as e:
                logger.error(f"Error wrapping up FFmpeg process for camera {self.camera_id}: {e}")
                if self.process.poll() is None:
                    self.process.kill()

        if self.recording_metadata:
            self.recording_metadata.status = "completed"
            self.recording_metadata.ended_at = datetime.now(timezone.utc)

            if self.recording_metadata.started_at and self.recording_metadata.ended_at:
                self.recording_metadata.duration = (
                        self.recording_metadata.ended_at - self.recording_metadata.started_at
                ).total_seconds()

            if os.path.exists(self.recording_metadata.file_path):
                self.recording_metadata.file_size = os.path.getsize(self.recording_metadata.file_path)

        logger.info(f"H264 recorder stopped cleanly for camera {self.camera_id}")

    def get_metadata(self) -> Recording:
        return self.recording_metadata
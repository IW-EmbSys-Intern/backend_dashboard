# app/core/publisher/publisher_manager.py
import threading
import logging
from typing import Dict, Optional
from app.core.publisher.publisher_worker import PublisherWorker
# from app.core.publisher.recording_worker import PublisherRecordingWorker
from app.core.publisher.rtsp_publisher import RtspPublisher

logger = logging.getLogger(__name__)


class PublisherManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.workers: Dict[str, PublisherWorker] = {}
        # self.active_recordings: Dict[str, PublisherRecordingWorker] = {}
        self.active_rtsp_publishers: Dict[str, RtspPublisher] = {}

    def get_or_create_worker(self, stream_key: str, session_id: str, target_rtsp_url: str) -> PublisherWorker:
        with self.lock:
            if stream_key in self.workers:
                logger.info(f"Re-attaching stream connection to worker: {stream_key}")
                return self.workers[stream_key]

            worker = PublisherWorker(session_id=session_id, stream_key=stream_key)
            self.workers[stream_key] = worker
            worker.start()

            # Instantiating RTSP publisher using the dynamic runtime destination string
            rtsp_pub = RtspPublisher(stream_key=stream_key, target_rtsp_url=target_rtsp_url)
            rtsp_pub.start()
            self.active_rtsp_publishers[stream_key] = rtsp_pub
            worker.add_consumer(rtsp_pub)

            return worker

    def start_stream_recording(self, stream_key: str) -> bool:
        with self.lock:
            worker = self.workers.get(stream_key)
            if not worker:
                logger.warning(f"Cannot record nonexistent active stream feed: {stream_key}")
                return False

            if stream_key in self.active_recordings:
                return True

            # recorder = PublisherRecordingWorker(stream_key=stream_key)
            # recorder.start()

            # self.active_recordings[stream_key] = recorder
            # worker.add_consumer(recorder)
            return True

    def stop_stream_recording(self, stream_key: str) -> Optional[object]:
        with self.lock:
            recorder = self.active_recordings.pop(stream_key, None)
            worker = self.workers.get(stream_key)

            if not recorder:
                return None

            if worker:
                worker.remove_consumer(recorder)

            recorder.stop()
            return recorder.get_metadata()

    def remove_worker(self, stream_key: str):
        self.stop_stream_recording(stream_key)

        with self.lock:
            rtsp_pub = self.active_rtsp_publishers.pop(stream_key, None)
            worker = self.workers.pop(stream_key, None)

            if worker and rtsp_pub:
                worker.remove_consumer(rtsp_pub)
            if rtsp_pub:
                rtsp_pub.stop()
            if worker:
                worker.stop()


publisher_manager = PublisherManager()
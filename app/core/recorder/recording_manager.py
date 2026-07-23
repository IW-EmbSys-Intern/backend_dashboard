import threading
from app.core.recorder.recording_worker import RecordingWorker


class RecordingManager:

    def __init__(self):
        self.lock = threading.Lock()
        self.active_recordings = {}
        self.completed_recordings = {}

    def start_recording(self, camera_id: str, publisher_worker):
        with self.lock:
            if camera_id in self.active_recordings:
                return self.active_recordings[camera_id].get_metadata()

            recorder = RecordingWorker(camera_id=camera_id)
            recorder.start()

            self.active_recordings[camera_id] = recorder

            # Attach recorder as a non-blocking consumer to PublisherWorker
            publisher_worker.add_consumer(recorder)

            return recorder.get_metadata()

    def get_recording(self, camera_id: str):
        recorder = self.active_recordings.get(camera_id)
        if recorder is None:
            return None
        return recorder.get_metadata()

    def list_recordings(self, camera_id: str):
        recordings = []

        recorder = self.active_recordings.get(camera_id)
        if recorder:
            recordings.append(recorder.get_metadata())

        for recording in self.completed_recordings.values():
            if getattr(recording, 'camera_id', None) == camera_id:
                recordings.append(recording)

        return recordings

    def stop_recording(self, camera_id: str, publisher_worker):
        with self.lock:
            recorder = self.active_recordings.pop(camera_id, None)

            if not recorder:
                return None

            publisher_worker.remove_consumer(recorder)
            recorder.stop()

            metadata = recorder.get_metadata()
            if metadata and hasattr(metadata, 'id'):
                self.completed_recordings[metadata.id] = metadata

            return metadata

    def delete_recording(self, recording_id: str):
        recording = self.completed_recordings.pop(recording_id, None)

        if recording is None:
            return False

        if recording.file_path and recording.file_path.exists():
            recording.file_path.unlink()

        return True


recording_manager = RecordingManager()
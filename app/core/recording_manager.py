import threading

from app.core.recording_worker import RecordingWorker


class RecordingManager:

    def __init__(self):

        # camera_id -> RecordingWorker
        self.recordings = {}

        self.lock = threading.Lock()



    def start_recording(
        self,
        camera_id,
        video_worker
    ):

        with self.lock:

            # Already recording
            if camera_id in self.recordings:

                return self.recordings[camera_id]


            worker = RecordingWorker(
                camera_id
            )


            self.recordings[camera_id] = worker


            # Subscribe recorder to VideoWorker frames
            video_worker.add_frame_consumer(
                worker
            )


            worker.start()


            return worker



    def stop_recording(
        self,
        camera_id,
        video_worker
    ):

        with self.lock:

            worker = self.recordings.get(
                camera_id
            )


            if worker is None:

                return False


            # Remove frame subscription
            video_worker.remove_frame_consumer(
                worker
            )


            worker.stop()


            del self.recordings[camera_id]


            return True



    def get_recording(
        self,
        camera_id
    ):

        with self.lock:

            worker = self.recordings.get(
                camera_id
            )


            if worker:

                return worker.get_recording()


            return None



    def is_recording(
        self,
        camera_id
    ):

        with self.lock:

            worker = self.recordings.get(
                camera_id
            )


            if worker:

                return worker.is_recording()


            return False


recording_manager = RecordingManager()
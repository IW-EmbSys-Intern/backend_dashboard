import cv2
import threading
import queue
import os
from datetime import datetime

from app.models.recording import Recording


class RecordingWorker:

    def __init__(
        self,
        camera_id,
        width=1280,
        height=720,
        fps=30
    ):

        self.camera_id = camera_id

        self.width = width
        self.height = height
        self.fps = fps


        # Current recording metadata
        self.recording = None


        # Thread control
        self.running = False
        self.thread = None


        # Frame queue between VideoWorker and recorder thread
        self.frame_queue = queue.Queue(
            maxsize=60
        )


        # OpenCV writer
        self.writer = None


        self.lock = threading.Lock()



    def start(self):

        with self.lock:

            if self.running:
                return


            self.recording = Recording(
                self.camera_id
            )


            folder = os.path.join(
                "storage",
                "recordings",
                f"camera_{self.camera_id}"
            )


            os.makedirs(
                folder,
                exist_ok=True
            )


            timestamp = datetime.utcnow().strftime(
                "%Y%m%d_%H%M%S"
            )


            filename = (
                f"camera_{self.camera_id}_"
                f"{timestamp}.mp4"
            )


            self.recording.filename = filename


            self.recording.file_path = os.path.join(
                folder,
                filename
            )


            self.recording.status = "recording"


            self.recording.started_at = (
                datetime.utcnow()
            )


            self.running = True


            self.thread = threading.Thread(
                target=self.run,
                daemon=True
            )


            self.thread.start()



    def run(self):

        fourcc = cv2.VideoWriter_fourcc(
            *"mp4v"
        )


        self.writer = cv2.VideoWriter(
            self.recording.file_path,
            fourcc,
            self.fps,
            (
                self.width,
                self.height
            )
        )


        while self.running:

            try:

                frame = self.frame_queue.get(
                    timeout=1
                )


            except queue.Empty:

                continue


            if self.writer:

                self.writer.write(
                    frame
                )
        print(
            "Frame written to MP4:",
            self.recording.file_path
        )


        if self.writer:

            self.writer.release()

            self.writer = None



    def write_frame(
        self,
        frame
    ):
        # print(
        #     "Recorder received frame:",
        #     frame.shape
        # )
        if not self.running:
            return


        try:

            self.frame_queue.put_nowait(
                frame.copy()
            )


        except queue.Full:

            # Drop frames instead of blocking VideoWorker
            pass



    def stop(self):

        with self.lock:

            if not self.running:
                return


            self.running = False


        if self.thread:

            self.thread.join(timeout=2)


        if self.recording:

            self.recording.status = "completed"


            self.recording.ended_at = (
                datetime.utcnow()
            )


            if os.path.exists(
                self.recording.file_path
            ):

                self.recording.file_size = (
                    os.path.getsize(
                        self.recording.file_path
                    )
                )



    def get_recording(self):

        return self.recording



    def is_recording(self):

        return self.running
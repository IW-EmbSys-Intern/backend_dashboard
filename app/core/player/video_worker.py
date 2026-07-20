import cv2
import threading
import time


class VideoWorker:

    def __init__(self, url, camera_id, camera):
        self.url = url
        self.camera_id = camera_id
        self.camera = camera

        self.running = False

        # frame storage
        self.frame = None
        self.jpeg = None

        self.lock = threading.Lock()

        # OpenCV capture
        self.cap = None
        self.cap_lock = threading.Lock()

        self.thread = None

        # viewer count
        self.viewer_count = 0
        self.viewer_lock = threading.Lock()

        # RecordingWorker
        self.frame_consumers = []
        self.consumer_lock = threading.Lock()

        self.frame_counter = 0
        self.last_fps_time = time.time()

    def start(self):
        self.running = True
        self.thread = threading.Thread(
            target=self.run,
            daemon=True
        )
        self.thread.start()

    def run(self):
        print(f"Starting camera {self.camera_id}: {self.url}")

        # Staggered startup to avoid hammering the RTSP server
        time.sleep(0.5 * self.camera_id)

        while self.running:
            self.camera.status = "connecting"

            # Open RTSP connection
            with self.cap_lock:

                print("Opening RTSP:", self.url)

                self.cap = cv2.VideoCapture(
                    self.url,
                    cv2.CAP_FFMPEG,
                )

                print(
                    "Opened:",
                    self.cap.isOpened()
                )
                if self.cap:
                    self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

            if self.cap is None or not self.cap.isOpened():
                print(f"Camera {self.camera_id} failed to connect")
                self.camera.status = "failed"

                with self.cap_lock:
                    if self.cap:
                        self.cap.release()
                        self.cap = None

                time.sleep(3)
                continue

            print(f"Camera {self.camera_id} connected")
            self.camera.status = "connected"

            for _ in range(5):
                with self.cap_lock:
                    if self.cap:
                        self.cap.grab()
            time.sleep(0.1)

            # Initialize the tracking counter for frame drop resilience
            consecutive_failures = 0

            while self.running:
                with self.cap_lock:
                    if not self.running or self.cap is None:
                        break

                    # Drop old frames to combat decoding lag
                    for _ in range(3):
                        self.cap.grab()

                    ret, frame = self.cap.retrieve()

                if not ret or frame is None:
                    consecutive_failures += 1
                    print(f"Camera {self.camera_id} missed a frame ({consecutive_failures}/3)")

                    # If it's a minor network hitch, try again immediately without killing the socket
                    if consecutive_failures < 3:
                        time.sleep(0.01)
                        continue
                    else:
                        # 3 consecutive misses means the stream link is dead or frozen
                        print(f"Camera {self.camera_id} link timed out. Forcing hard reset.")
                        self.camera.status = "disconnected"
                        break

                # Reset failure counter on a completely successful decode
                consecutive_failures = 0

                # Process the clean frame (Resize & Encode ONCE)
                frame = cv2.resize(frame, (1280, 720))
                self.frame_counter += 1

                now = time.time()

                if now - self.last_fps_time >= 1:
                    print(f"Camera {self.camera_id} incoming FPS: {self.frame_counter}")

                    self.frame_counter = 0
                    self.last_fps_time = now
                self.notify_frame_consumers(
                    frame
                )

                ret, jpg = cv2.imencode(
                    ".jpg",
                    frame,
                    [cv2.IMWRITE_JPEG_QUALITY, 80]
                )

                if ret:
                    with self.lock:
                        self.frame = frame
                        self.jpeg = jpg.tobytes()

            with self.cap_lock:
                if self.cap:
                    try:
                        self.cap.release()
                    except Exception as e:
                        print(f"Release error during reset: {e}")
                    self.cap = None

            if self.running:
                print(f"Reconnecting camera {self.camera_id} in 3 seconds...")
                time.sleep(3)

        print(f"Camera {self.camera_id} stopped")

    def get_frame(self):
        with self.lock:
            if self.frame is None:
                return None
            return self.frame.copy()

    def get_jpeg(self):
        with self.lock:
            return self.jpeg

    def stop(self):
        print(f"Stopping camera {self.camera_id}")
        self.running = False
        self.camera.status = "stopped"

        with self.cap_lock:
            if self.cap:
                try:
                    self.cap.release()
                except Exception as e:
                    print(f"Release error: {e}")
                self.cap = None

        with self.lock:
            self.frame = None
            self.jpeg = None

        # Frame consumers
    def add_frame_consumer(
            self,
            consumer
    ):

        with self.consumer_lock:
            if consumer not in self.frame_consumers:
                self.frame_consumers.append(
                    consumer
                )

    def remove_frame_consumer(
            self,
            consumer
    ):

        with self.consumer_lock:
            if consumer in self.frame_consumers:
                self.frame_consumers.remove(
                    consumer
                )

    def notify_frame_consumers(
            self,
            frame
    ):

        with self.consumer_lock:
            consumers = list(
                self.frame_consumers
            )

        for consumer in consumers:
            print(
                "Sending frame to recorder:",
                self.camera_id,
                frame.shape
            )
            consumer.write_frame(
                frame
            )

    # Viewer count

    def add_viewer(self):
        with self.viewer_lock:
            self.viewer_count += 1

    def remove_viewer(self):
        with self.viewer_lock:
            if self.viewer_count > 0:
                self.viewer_count -= 1

    def get_viewer_count(self):
        with self.viewer_lock:
            return self.viewer_count
from app.models.camera import Camera
from app.core.player.video_worker import VideoWorker
import threading
from app.core.player.recording_manager import recording_manager

class CameraManager:

    def __init__(self):

        self.lock = threading.Lock()

        # camera_id -> Camera object
        self.cameras = {}

        # url -> camera_id mapping
        self.url_map = {}

        # unique id generator
        self.next_id = 0




    def add_camera(self, url):

        with self.lock:

            # Check duplicate RTSP URL
            if url in self.url_map:

                existing_id = self.url_map[url]

                print(
                    f"Camera already exists. Returning camera {existing_id}"
                )

                return self.cameras[existing_id]


            # Create new camera
            camera_id = self.next_id

            self.next_id += 1


            camera = Camera(
                camera_id,
                url
            )


            worker = VideoWorker(
                url,
                camera_id,
                camera
            )


            camera.worker = worker


            # Store references
            self.cameras[camera_id] = camera

            self.url_map[url] = camera_id


        # IMPORTANT:
        # Start worker outside lock
        worker.start()


        print(
            f"Created new camera {camera_id}"
        )


        return camera




    def get_camera(self, camera_id):

        with self.lock:

            return self.cameras.get(
                camera_id
            )

    def remove_camera(self, camera_id):

        camera = None

        # Remove from manager first
        with self.lock:

            camera = self.cameras.get(camera_id)

            # Camera does not exist
            if camera is None:
                print(
                    f"Camera {camera_id} not found"
                )

                return False

            # Remove camera reference
            del self.cameras[camera_id]

            # Remove URL mapping
            if camera.url in self.url_map:
                del self.url_map[camera.url]

        # Stop worker outside lock
        recording_manager.stop_recording(
            camera_id,
            camera.worker
        )

        camera.worker.stop()

        print(
            f"Camera {camera_id} removed"
        )

        return True


camera_manager = CameraManager()
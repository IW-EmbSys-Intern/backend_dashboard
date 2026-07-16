from datetime import datetime
from pathlib import Path
from uuid import uuid4


class Recording:


    def __init__(self, camera_id: int, base_directory: str = "storage/recordings"):

        self.id = str(uuid4())
        self.camera_id = camera_id
        self.status = "idle"
        self.started_at = None
        self.ended_at = None

        self.filename = None
        self.directory = Path(base_directory) / f"camera_{camera_id}"
        self.file_path = None

        self.duration = 0
        self.file_size = 0

        self.uploaded = False
        self.s3_key = None
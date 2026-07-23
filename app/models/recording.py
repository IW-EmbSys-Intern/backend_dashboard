# app/models/recording.py
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional


class Recording:
    def __init__(
        self,
        camera_id: str,
        base_directory: str = "storage/recordings",
        filename: Optional[str] = None,
        file_path: Optional[Path] = None,
        status: str = "idle",
        started_at: Optional[datetime] = None
    ):
        # Unique recording identifier
        self.id = str(uuid4())

        # Primary identity bound to camera_id
        self.camera_id = camera_id

        # Recording lifecycle state
        self.status = status

        self.created_at = datetime.now(timezone.utc)
        self.started_at = started_at
        self.ended_at = None

        # File information
        self.filename = filename
        self.directory = Path(base_directory) / f"camera_{camera_id}"
        self.file_path = file_path if file_path else (self.directory / filename if filename else None)

        # Recording statistics
        self.duration = 0.0
        self.file_size = 0

        # Storage/upload information
        self.uploaded = False
        self.s3_key = None

        # Reason for stopping (user_stopped, disconnect, error, etc.)
        self.end_reason = None
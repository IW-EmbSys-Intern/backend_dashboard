from pathlib import Path
from datetime import datetime, timezone


class RecordingStorage:

    def __init__(self, base_directory="storage/recordings"):
        self.base_directory = Path(base_directory)

    def create_recording(self, camera_id: str):
        folder = self.base_directory / f"camera_{camera_id}"
        folder.mkdir(parents=True, exist_ok=True)

        now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"indocaster_{camera_id}_{now_str}.mp4"

        return folder, filename, folder / filename
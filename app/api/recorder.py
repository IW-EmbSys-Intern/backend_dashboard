import logging
from fastapi import APIRouter, HTTPException
from app.core.publisher.publisher_manager import publisher_manager
from app.core.recorder.recording_manager import recording_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/recorder", tags=["Recorder"])


def recording_to_dict(recording):
    if recording is None:
        return None

    return {
        "id": getattr(recording, "id", None),
        "camera_id": getattr(recording, "camera_id", None),
        "status": getattr(recording, "status", None),
        "filename": getattr(recording, "filename", None),
        "file_path": str(recording.file_path) if getattr(recording, "file_path", None) else None,
        "created_at": getattr(recording, "created_at", None),
        "started_at": getattr(recording, "started_at", None),
        "ended_at": getattr(recording, "ended_at", None),
        "duration": getattr(recording, "duration", None),
        "file_size": getattr(recording, "file_size", None),
        "uploaded": getattr(recording, "uploaded", False),
        "s3_key": getattr(recording, "s3_key", None),
        "end_reason": getattr(recording, "end_reason", None)
    }


# ---------------------------------------------------------
# Start Recording
# ---------------------------------------------------------

@router.post("/start/{camera_id}")
def start_recording(camera_id: str):
    """
    Start recording an active live stream for a given camera.
    """
    # Look up the worker by camera_id
    print("CURRENT ACTIVE WORKERS IN MEMORY:", list(publisher_manager.workers.keys()))
    worker = publisher_manager.workers.get(camera_id)

    if worker is None:
        raise HTTPException(
            status_code=404,
            detail=f"Active stream for camera '{camera_id}' not found"
        )

    recording = recording_manager.start_recording(
        camera_id=camera_id,
        publisher_worker=worker
    )

    return {
        "message": "Recording started",
        "recording": recording_to_dict(recording)
    }


# ---------------------------------------------------------
# Stop Recording
# ---------------------------------------------------------

@router.post("/stop/{camera_id}")
def stop_recording(camera_id: str):
    """
    Stop active recording for a camera.
    """
    worker = publisher_manager.workers.get(camera_id)

    if worker is None:
        raise HTTPException(
            status_code=404,
            detail=f"Active stream for camera '{camera_id}' not found"
        )

    recording = recording_manager.stop_recording(
        camera_id=camera_id,
        publisher_worker=worker
    )

    if recording is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active recording found for camera '{camera_id}'"
        )

    return {
        "message": "Recording stopped",
        "recording": recording_to_dict(recording)
    }


# ---------------------------------------------------------
# Get Active Recording Status
# ---------------------------------------------------------

@router.get("/{camera_id}")
def get_recording(camera_id: str):
    """
    Get current recording status for a specific camera.
    """
    recording = recording_manager.get_recording(camera_id)

    if recording is None:
        return {
            "recording": False,
            "camera_id": camera_id
        }

    return {
        "recording": True,
        "data": recording_to_dict(recording)
    }


# ---------------------------------------------------------
# List Camera Recordings
# ---------------------------------------------------------

@router.get("/{camera_id}/list")
def list_recordings(camera_id: str):
    """
    Get all recordings associated with a camera_id.
    """
    recordings = recording_manager.list_recordings(camera_id)

    return {
        "camera_id": camera_id,
        "recordings": [
            recording_to_dict(r) for r in recordings
        ]
    }


# ---------------------------------------------------------
# Delete Recording
# ---------------------------------------------------------

@router.delete("/{recording_id}")
def delete_recording(recording_id: str):
    """
    Delete a completed recording by its unique recording ID.
    """
    deleted = recording_manager.delete_recording(recording_id)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail="Recording not found"
        )

    return {
        "message": "Recording deleted successfully",
        "recording_id": recording_id
    }
from fastapi import APIRouter, HTTPException

from app.core.camera_manager import camera_manager
from app.core.recording_manager import recording_manager


router = APIRouter()

def recording_to_dict(recording):

    if recording is None:
        return None


    return {

        "id": recording.id,

        "camera_id": recording.camera_id,

        "status": recording.status,

        "filename": recording.filename,

        "file_path": str(recording.file_path)
        if recording.file_path
        else None,

        "started_at": recording.started_at,

        "ended_at": recording.ended_at,

        "duration": recording.duration,

        "file_size": recording.file_size,

        "uploaded": recording.uploaded,

        "s3_key": recording.s3_key

    }



@router.post("/cameras/{camera_id}/recording")
def start_recording(
        camera_id: int
):

    camera = camera_manager.get_camera(
        camera_id
    )


    if camera is None:

        raise HTTPException(
            status_code=404,
            detail="Camera not found"
        )


    recording_manager.start_recording(
        camera_id,
        camera.worker
    )


    recording = recording_manager.get_recording(
        camera_id
    )


    return {

        "message": "recording started",

        "recording": recording_to_dict(recording)
        if recording else None

    }





@router.delete("/cameras/{camera_id}/recording")
def stop_recording(
        camera_id: int
):

    camera = camera_manager.get_camera(
        camera_id
    )


    if camera is None:

        raise HTTPException(
            status_code=404,
            detail="Camera not found"
        )


    stopped = recording_manager.stop_recording(
        camera_id,
        camera.worker
    )


    if not stopped:

        raise HTTPException(
            status_code=404,
            detail="No active recording found"
        )



    return {

        "message": "recording stopped"

    }





@router.get("/cameras/{camera_id}/recording")
def get_active_recording(
        camera_id: int
):

    camera = camera_manager.get_camera(
        camera_id
    )


    if camera is None:

        raise HTTPException(
            status_code=404,
            detail="Camera not found"
        )



    recording = recording_manager.get_recording(
        camera_id
    )


    if recording is None:

        return {

            "recording": False

        }



    return {

        "recording": True,

        "data": recording_to_dict(recording)

    }
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.camera_manager import camera_manager

router = APIRouter()

manager = camera_manager
class CameraCreate(BaseModel):

    url: str



@router.post("/")
def add_camera(
        camera: CameraCreate
):

    created_camera = manager.add_camera(
        camera.url
    )


    return {

        "camera_id": created_camera.id,

        "status": created_camera.status

    }


@router.get("/")
def get_cameras():

    cameras = []

    for camera in manager.cameras.values():
        worker = camera.worker

        # 1. Thread-safely read the current number of active browser clients
        viewer_count = worker.get_viewer_count()

        # 2. Check if the camera is actively writing decoded images to the buffer
        has_buffer = worker.get_jpeg() is not None
        cameras.append({
            "id": camera.id,
            "url": camera.url,
            "status": camera.status,
            "viewer_count": viewer_count,
            "has_buffer": has_buffer
        })

    return cameras




@router.get("/{camera_id}")
def get_camera(
        camera_id:int
):

    camera = manager.get_camera(
        camera_id
    )


    if camera is None:

        raise HTTPException(
            status_code=404,
            detail="Camera not found"
        )

    worker = camera.worker
    return {
        "id": camera.id,
        "url": camera.url,
        "status": camera.status,
        "viewer_count": worker.get_viewer_count(),
        "has_buffer": worker.get_jpeg() is not None
    }



@router.delete("/{camera_id}")
def remove_camera(
        camera_id:int
):

    deleted = manager.remove_camera(
        camera_id
    )


    if not deleted:

        raise HTTPException(
            status_code=404,
            detail="Camera not found"
        )


    return {

        "message":"camera stopped"

    }
# app/api/stream.py
import asyncio
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from app.core.camera_manager import camera_manager

router = APIRouter()


async def generate_frames(camera_id: int, request: Request):
    camera = camera_manager.get_camera(camera_id)
    if camera is None:
        return

    # Safely track viewer count
    camera.worker.add_viewer()

    try:
        while True:
            # 1. Break loop immediately if the client disconnects their browser tab
            if await request.is_disconnected():
                print(f"Client disconnected from camera {camera_id}")
                break

            if camera.status == "stopped":
                break

            # 2. Fetch the pre-encoded JPEG buffer (non-blocking reference copy)
            jpg = camera.worker.get_jpeg()

            if jpg is None:
                await asyncio.sleep(0.05)
                continue

            # 3. Yield the frame chunk
            yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Content-Length: " + str(len(jpg)).encode() + b"\r\n\r\n"
                    + jpg +
                    b"\r\n"
            )

            # 4. Use non-blocking async sleep to let other viewers process frames
            await asyncio.sleep(0.033)  # Targets ~30 FPS cleanly

    finally:
        # Guarantees cleanup of viewer count even if exceptions occur or tab closes
        camera.worker.remove_viewer()


@router.get("/cameras/{camera_id}/stream")
async def camera_stream(camera_id: int, request: Request):
    camera = camera_manager.get_camera(camera_id)
    if camera is None:
        raise HTTPException(status_code=404, detail="Camera not found")

    return StreamingResponse(
        generate_frames(camera_id, request),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )
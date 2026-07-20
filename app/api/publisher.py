# app/api/publisher.py
import json
import logging
import subprocess
import uuid
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from app.core.publisher.publisher_manager import publisher_manager
logger = logging.getLogger(__name__)

router = APIRouter()

# Keep track of active web streaming processes
# maps: session_id -> { "process": Popen, "rtsp_url": str }
active_web_sessions = {}


class StreamRequest(BaseModel):
    rtsp_url: str


@router.post("/start-streaming")
async def start_streaming(request: StreamRequest):
    """
    HTTP API: Called when the user types an RTSP URL on the website and clicks 'Start'.
    """
    if not request.rtsp_url:
        raise HTTPException(status_code=400, detail="Target RTSP URL is required")

    # Generate a tracking ID for this dynamic session
    session_id = str(uuid.uuid4())
    stream_key = f"web_{session_id[:8]}"

    # Configure an internal FFmpeg session to accept the browser's video layout
    # and immediately throw it to the user's specific target RTSP URL
    ffmpeg_cmd = [
        'ffmpeg', '-y',
        '-i', 'pipe:0',  # Read the byte chunk stream straight from Python memory
        '-c:v', 'libx264',  # Convert browser video layouts to standard H264
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-f', 'rtsp',
        '-rtsp_transport', 'tcp',
        request.rtsp_url  # The dynamic AWS destination URL Indoplayer reads from
    ]

    try:
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        active_web_sessions[session_id] = {
            "process": process,
            "rtsp_url": request.rtsp_url,
            "stream_key": stream_key
        }

        return {"status": "success", "session_id": session_id, "stream_key": stream_key}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to provision backend media loop: {str(e)}")


# app/api/publisher.py (Update the WebSocket endpoint section)

@router.websocket("/ws/{session_id}")
async def websocket_stream_endpoint(websocket: WebSocket, session_id: str):
    """
    WebSocket API: The pipeline where browser screen capture buffers are continuously sent.
    Ensures absolute lifecycle safety against zombie FFmpeg child processes.
    """
    await websocket.accept()

    if session_id not in active_web_sessions:
        await websocket.close(code=4004, reason="Session expired or not found")
        return

    session = active_web_sessions[session_id]
    process = session["process"]

    try:
        while True:
            # Capture continuous video bytes coming live from the browser screen
            video_chunk = await websocket.receive_bytes()

            # Pipe the data buffer straight down into the running FFmpeg instance
            if process.poll() is None:
                process.stdin.write(video_chunk)
                process.stdin.flush()
            else:
                # Process died unexpectedly on its own
                break
    except WebSocketDisconnect as e:
                logger.info(
                    f"Websocket disconnected for session {session_id}, code={e.code}"
                )
    except Exception as e:
        logger.error(f"Unexpected error during streaming session {session_id}: {e}")
    finally:
        # --- CRITICAL ZOMBIE CLEANUP IMPLEMENTATION ---
        # 1. Remove the session map reference to free up memory tracking
        active_web_sessions.pop(session_id, None)

        # 2. Terminate the OS subprocess cleanly if it is still running
        if process.poll() is None:
            try:
                if process.stdin:
                    process.stdin.close()  # Flush and close the stream pipe
            except Exception:
                pass

            try:
                process.terminate()  # Ask FFmpeg to exit gracefully
                # Give it up to 2 seconds to write remaining frame structures out
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()  # Force kill if it completely hangs
                process.wait()  # Reclaim process descriptor from OS table
            except Exception as e:
                logger.error(f"Error purging dynamic process loop for {session_id}: {e}")

        # 3. Cleanly close the server side socket boundary
        try:
            await websocket.close()
        except Exception:
            # Socket might already be closed by the client
            pass


@router.post("/stop-streaming/{session_id}")
async def stop_streaming(session_id: str):
    """
    HTTP API: Called when the user clicks 'Stop Streaming'.
    """
    session = active_web_sessions.pop(session_id, None)
    if not session:
        raise HTTPException(status_code=404, detail="Active session not found")

    process = session["process"]
    try:
        if process.stdin:
            process.stdin.close()
        process.terminate()  # Instantly stop the internal media bridge
        process.wait(timeout=2)
        return {"status": "success", "message": "Stream stopped successfully"}
    except Exception as e:
        return {"status": "partial_success", "message": f"Stream cleared with note: {str(e)}"}
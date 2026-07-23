# app/api/publisher.py
import json
import logging
import subprocess
import uuid
from typing import Optional
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from app.core.publisher.publisher_manager import publisher_manager
from app.core.publisher.publisher_worker import PublisherWorker
from app.core.publisher.packet import Packet

logger = logging.getLogger(__name__)

router = APIRouter()

# Keep track of active web streaming processes
# maps: session_id -> { "process": Popen, "rtsp_url": str, "worker": PublisherWorker, "camera_id": str }
active_web_sessions = {}


class StreamRequest(BaseModel):
    rtsp_url: str
    camera_id: Optional[str] = None  # Optional camera identifier, defaults to session_id


@router.post("/start-streaming")
async def start_streaming(request: StreamRequest):
    """
    HTTP API: Called when the user types an RTSP URL on the website and clicks 'Start'.
    Provisions FFmpeg bridge and registers a PublisherWorker for consumers (Recorder, etc.).
    """
    if not request.rtsp_url:
        raise HTTPException(status_code=400, detail="Target RTSP URL is required")

    # Generate tracking and camera identifiers
    session_id = str(uuid.uuid4())

    # Default to "0" if camera_id is not supplied or empty
    camera_id = request.camera_id if (hasattr(request, "camera_id") and request.camera_id) else "0"
    stream_key = f"web_{session_id[:8]}"

    # Configure an internal FFmpeg session to accept WebM binary input from stdin
    # and re-encode to standard low-latency H.264 over RTSP
    ffmpeg_cmd = [
        'ffmpeg', '-y',
        '-loglevel', 'warning',
        '-f', 'webm',  # Explicitly parse incoming WebM chunks from MediaRecorder
        '-i', 'pipe:0',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',  # Lowest CPU usage for live delivery
        '-tune', 'zerolatency',
        '-r', '30',  # Throttle/Force steady 30 FPS output
        '-g', '60',  # Keyframe interval every 2 seconds
        '-f', 'rtsp',
        '-rtsp_transport', 'tcp',  # Reliable TCP transport for AWS RTSP server
        request.rtsp_url
    ]

    try:
        # Spawn FFmpeg process with piped stdin
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )

        # 1. Instantiate the PublisherWorker for this active camera stream
        worker = PublisherWorker(camera_id=camera_id)
        if hasattr(worker, "start"):
            worker.start()

        # 2. Register inside publisher_manager under camera_id, session_id, and stream_key for lookup
        publisher_manager.workers[camera_id] = worker
        publisher_manager.workers[session_id] = worker
        publisher_manager.workers[stream_key] = worker

        # 3. Store reference in active web sessions map
        active_web_sessions[session_id] = {
            "process": process,
            "rtsp_url": request.rtsp_url,
            "stream_key": stream_key,
            "camera_id": camera_id,
            "worker": worker
        }

        return {
            "status": "success",
            "session_id": session_id,
            "camera_id": camera_id,
            "stream_key": stream_key
        }

    except Exception as e:
        # Cleanup registered entries in case of process allocation failure
        publisher_manager.workers.pop(camera_id, None)
        publisher_manager.workers.pop(session_id, None)
        publisher_manager.workers.pop(stream_key, None)

        raise HTTPException(
            status_code=500,
            detail=f"Failed to provision backend media loop: {str(e)}"
        )
    except Exception as e:
        # Cleanup in case of process allocation failure
        publisher_manager.workers.pop(camera_id, None)
        publisher_manager.workers.pop(session_id, None)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to provision backend media loop: {str(e)}"
        )


@router.websocket("/ws/{session_id}")
async def websocket_stream_endpoint(websocket: WebSocket, session_id: str):
    """
    WebSocket API: Receives binary video chunks from browser screen capture,
    pipes them into FFmpeg RTSP delivery, and broadcasts Packet objects to PublisherWorker consumers.
    """
    await websocket.accept()

    if session_id not in active_web_sessions:
        await websocket.close(code=4004, reason="Session expired or not found")
        return

    session = active_web_sessions[session_id]
    process = session["process"]
    worker = session["worker"]
    camera_id = session["camera_id"]

    try:
        while True:
            # Capture continuous video bytes coming live from the browser screen
            video_chunk = await websocket.receive_bytes()

            # 1. Pipe data into FFmpeg OS process
            if process.poll() is None:
                process.stdin.write(video_chunk)
                process.stdin.flush()

                # 2. Wrap incoming raw chunk into system Packet object and notify worker consumers
                # Simple heuristic to flag keyframes/headers or pass through to RecordingWorker
                is_keyframe = (b'\x00\x00\x00\x01' in video_chunk) or (b'\x00\x00\x01' in video_chunk)

                packet = Packet(
                    data=video_chunk,
                    pts=0,
                    dts=0,
                    is_keyframe=is_keyframe
                )

                # Broadcast to attached RecordingWorker (or future AI modules)
                if hasattr(worker, "broadcast"):
                    worker.broadcast(packet)
                elif hasattr(worker, "push_packet"):
                    worker.push_packet(packet)

            else:
                # FFmpeg process terminated unexpectedly
                break

    except WebSocketDisconnect as e:
        logger.info(f"Websocket disconnected for session {session_id}, code={e.code}")
    except Exception as e:
        logger.error(f"Unexpected error during streaming session {session_id}: {e}")
    finally:
        # --- CRITICAL CLEANUP IMPLEMENTATION ---

        # 1. Stop worker thread/queue if available
        if hasattr(worker, "stop"):
            try:
                worker.stop()
            except Exception:
                pass

        # 2. Remove references from memory managers
        publisher_manager.workers.pop(session_id, None)
        publisher_manager.workers.pop(camera_id, None)
        active_web_sessions.pop(session_id, None)

        # 3. Terminate FFmpeg subprocess cleanly
        if process.poll() is None:
            try:
                if process.stdin:
                    process.stdin.close()
            except Exception:
                pass

            try:
                process.terminate()
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            except Exception as e:
                logger.error(f"Error purging process loop for session {session_id}: {e}")

        # 4. Close websocket connection
        try:
            await websocket.close()
        except Exception:
            pass


@router.post("/stop-streaming/{session_id}")
async def stop_streaming(session_id: str):
    """
    HTTP API: Called when the user clicks 'Stop Streaming'.
    """
    session = active_web_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Active session not found")

    camera_id = session.get("camera_id")
    worker = session.get("worker")

    # Clean up publisher manager tracking
    publisher_manager.workers.pop(session_id, None)
    if camera_id:
        publisher_manager.workers.pop(camera_id, None)

    if worker and hasattr(worker, "stop"):
        try:
            worker.stop()
        except Exception:
            pass

    session_data = active_web_sessions.pop(session_id, None)
    process = session_data["process"] if session_data else None

    try:
        if process and process.poll() is None:
            if process.stdin:
                process.stdin.close()
            process.terminate()
            process.wait(timeout=2)
        return {"status": "success", "message": "Stream stopped successfully"}
    except Exception as e:
        return {"status": "partial_success", "message": f"Stream cleared with note: {str(e)}"}
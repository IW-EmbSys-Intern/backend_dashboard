import os
import logging
from contextlib import asynccontextmanager

from app.core.publisher.encoder_receiver import EncoderReceiver
from fastapi import FastAPI

from app.api.cameras import router
from app.api.stream import router as stream_router
# from app.api.recordings import router as recording_router
from fastapi.responses import HTMLResponse
from pathlib import Path
from app.api.publisher import router as publisher_router

logger = logging.getLogger(__name__)

# Enforce low-latency runtime bindings across OpenCV/FFmpeg bindings
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
    "rtsp_transport;tcp|"          
    "stimeout;3000000|"            
    "timeout;3000000|"             
    "rw_timeout;3000000|"          
    "buffer_size;102400|"          
    "max_delay;500000|"            
    "fflags;nobuffer+discardcorrupt|" 
    "flags;low_delay"
)

# Initialize TCP connection structure global tracking references
receiver = EncoderReceiver(host="0.0.0.0", port=4000)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Boot the critical asynchronous backend background listeners on system boot
    receiver.start()
    yield
    # Safely wind down listening endpoints on shutdown
    receiver.stop()

app = FastAPI(
    title="Indoplayer + Indocaster Integrated Streaming Platform",
    lifespan=lifespan
)
# app = FastAPI(
#     title="Indoplayer Camera Service",
# )


app.include_router(router, prefix="/cameras")
app.include_router(stream_router)
# app.include_router(recording_router)
app.include_router(publisher_router, prefix="/api/publisher", tags=["Publisher"])

@app.get("/")
def home():
    return {
        "service": "camera-service",
        "status": "running"
    }
@app.get("/test-caster", response_class=HTMLResponse)
async def get_test_page():
    # Read the file right from your templates file system location
    html_path = Path("templates/test_caster.html")
    if html_path.exists():
        return html_path.read_text()
    return "<h1>Template test_caster.html not found inside templates directory</h1>"
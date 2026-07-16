import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.api.cameras import router
from app.api import caster
from app.api.stream import router as stream_router
from app.api.recordings import router as recording_router

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Indoplayer Engine initializing core components...")
    yield
    logger.info("Application shutdown signal caught. Flushing publisher resources...")
    try:
        caster.manager.shutdown()
        logger.info("Global clean shutdown completed.")
    except Exception as e:
        logger.exception("Error executing clean manager teardown sequence: %s", e)

app = FastAPI(
    title="Indoplayer Camera Service",
    lifespan=lifespan
)

app.include_router(router, prefix="/cameras")
app.include_router(stream_router)
app.include_router(recording_router)
app.include_router(caster.router)

@app.get("/")
def home():
    return {
        "service": "camera-service",
        "status": "running"
    }
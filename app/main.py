import os

from fastapi import FastAPI

from app.api.cameras import router

from app.api.stream import router as stream_router


os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
    "rtsp_transport;tcp|"          # Enforce stable TCP transport over UDP
    "stimeout;3000000|"            # Connection timeout (3 seconds in microseconds)
    "timeout;3000000|"             # Data streaming timeout (3 seconds in microseconds)
    "rw_timeout;3000000|"          # Read/Write timeout (3 seconds in microseconds)
    "buffer_size;102400|"          # Minimize socket buffer size
    "max_delay;500000|"            # Max delay allowed (0.5 seconds)
    "fflags;nobuffer+discardcorrupt|" # Disable parsing buffers / drop bad frames immediately
    "flags;low_delay"              # Force ultra-low decoding delay
)
app = FastAPI(
    title="Indoplayer Camera Service"
)


app.include_router(
    router,
    prefix="/cameras"
)

app.include_router(
    stream_router
)

@app.get("/")
def home():

    return {
        "service": "camera-service",
        "status": "running"
    }
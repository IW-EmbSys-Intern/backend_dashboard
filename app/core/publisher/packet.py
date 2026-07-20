# app/core/publisher/packet.py
from dataclasses import dataclass, field
import time

@dataclass
class Packet:
    data: bytes
    pts: int             # Original MediaCodec presentationTimeUs
    dts: int             # Decode timestamp (often equals PTS for screen capture)
    is_keyframe: bool
    arrival_time: float = field(default_factory=time.time)
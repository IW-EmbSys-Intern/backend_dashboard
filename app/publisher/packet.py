# app/publisher/packet.py

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class Packet:

    payload: bytes

    # Media timestamps (from Android MediaCodec)

    pts: int                     # presentationTimeUs
    dts: Optional[int] = None    # decode timestamp (usually same as pts)

    # Packet information

    sequence: int = 0            # Incremented by Android
    duration: Optional[int] = None   # Duration in microseconds

    # Stream information

    codec: str = "h264"

    is_keyframe: bool = False

    is_config: bool = False      # SPS/PPS/VPS packet

    # Backend metadata

    arrival_time: float = field(default_factory=time.perf_counter)


    stream_id: Optional[str] = None

    def size(self) -> int:
        return len(self.payload)

    def __len__(self):
        return len(self.payload)

    def __repr__(self):
        return (
            f"Packet("
            f"seq={self.sequence}, "
            f"pts={self.pts}, "
            f"keyframe={self.is_keyframe}, "
            f"config={self.is_config}, "
            f"size={len(self.payload)} bytes)"
        )
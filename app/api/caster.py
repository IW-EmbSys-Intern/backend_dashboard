
# app/api/caster.py

from __future__ import annotations

import logging
import struct

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.publisher.manager import Manager
from app.publisher.packet import Packet


logger = logging.getLogger(__name__)


router = APIRouter()


#
# Global manager instance
#
# Later we can move this into app state
#
manager = Manager()



# ============================================================
# Binary Packet Format
# ============================================================
#
# Every WebSocket binary message:
#
# +----------------+
# | sequence       | 4 bytes
# +----------------+
# | pts            | 8 bytes
# +----------------+
# | flags          | 1 byte
# +----------------+
# | payload_size   | 4 bytes
# +----------------+
# | H264 payload   |
# +----------------+
#
#
# flags:
#
# bit 0 = keyframe
# bit 1 = config packet (SPS/PPS)
#
# ============================================================


HEADER_FORMAT = "!IQBI"

HEADER_SIZE = struct.calcsize(
    HEADER_FORMAT
)



def parse_packet(data: bytes) -> Packet:
    """
    Convert websocket binary message
    into internal Packet object.
    """


    if len(data) < HEADER_SIZE:

        raise ValueError(
            "Invalid packet size"
        )


    (
        sequence,
        pts,
        flags,
        payload_size

    ) = struct.unpack(
        HEADER_FORMAT,
        data[:HEADER_SIZE]
    )


    payload = data[
        HEADER_SIZE:
    ]


    if len(payload) != payload_size:

        raise ValueError(
            "Payload size mismatch"
        )


    packet = Packet(

        payload=payload,

        pts=pts,

        dts=pts,

        sequence=sequence,

        is_keyframe=bool(
            flags & 0x01
        ),

        is_config=bool(
            flags & 0x02
        )

    )


    return packet



@router.websocket(
    "/caster/{stream_id}"
)
async def caster_socket(
    websocket: WebSocket,
    stream_id: str,
):

    await websocket.accept()


    #
    # For now stream_id is enough.
    # Later authentication can provide device_id.
    #
    device_id = stream_id


    session = manager.create_session(
        stream_id=stream_id,
        device_id=device_id
    )


    logger.info(
        "Caster connected: %s",
        stream_id
    )


    try:

        while True:


            data = await websocket.receive_bytes()
            print("RAW DATA RECEIVED:", len(data))

            try:

                packet = parse_packet(
                    data
                )

            except Exception:

                logger.exception(
                    "Packet parsing failed"
                )

                continue



            manager.publish_packet(
                stream_id,
                packet
            )


    except WebSocketDisconnect:


        logger.info(
            "Caster disconnected: %s",
            stream_id
        )


    except Exception:


        logger.exception(
            "Caster error: %s",
            stream_id
        )


    finally:


        manager.remove_session(
            stream_id
        )
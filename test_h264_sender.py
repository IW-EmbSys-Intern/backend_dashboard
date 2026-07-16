import asyncio
import struct
import websockets

WS_URL = "ws://127.0.0.1:8000/caster/test_device"

HEADER_FORMAT = "!IQBI"


def split_h264_annexb(data: bytes):
    """
    Split Annex-B H264 stream into NAL units.
    Returns each NAL WITHOUT the start code.
    """
    nals = []

    i = 0
    start = None

    while i < len(data) - 3:

        if data[i:i+3] == b"\x00\x00\x01":
            if start is not None:
                nals.append(data[start:i])
            start = i + 3
            i += 3
            continue

        if data[i:i+4] == b"\x00\x00\x00\x01":
            if start is not None:
                nals.append(data[start:i])
            start = i + 4
            i += 4
            continue

        i += 1

    if start is not None:
        nals.append(data[start:])

    return [n for n in nals if len(n) > 0]


async def send_h264():

    async with websockets.connect(WS_URL) as ws:

        print("CONNECTED")

        with open("sample.h264", "rb") as f:
            data = f.read()

        nal_units = split_h264_annexb(data)

        print(f"Found {len(nal_units)} NAL units")

        sequence = 0
        pts = 0

        for nal in nal_units:

            nal_type = nal[0] & 0x1F

            flags = 0

            if nal_type == 5:
                flags |= 0x01      # keyframe

            if nal_type in (7, 8):
                flags |= 0x02      # SPS/PPS

            packet = (
                struct.pack(
                    HEADER_FORMAT,
                    sequence,
                    pts,
                    flags,
                    len(nal),
                )
                + nal
            )

            await ws.send(packet)

            print(
                f"SENT seq={sequence:4d} "
                f"type={nal_type} "
                f"config={bool(flags & 0x02)} "
                f"key={bool(flags & 0x01)} "
                f"size={len(nal)}"
            )

            sequence += 1
            pts += 33333

            await asyncio.sleep(1 / 30)

        print("DONE")


if __name__ == "__main__":
    asyncio.run(send_h264())
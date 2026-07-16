import asyncio
import struct
import random
import websockets


URL = "ws://127.0.0.1:8000/caster/test_device"


HEADER_FORMAT = "!IQBI"


async def main():

    async with websockets.connect(URL) as ws:

        print("CONNECTED TO BACKEND")


        sequence = 0
        pts = 0


        while True:

            sequence += 1


            # fake H264 bytes
            payload = (
                b"\x00\x00\x00\x01"
                +
                bytes(
                    random.getrandbits(8)
                    for _ in range(1000)
                )
            )


            flags = 0


            # every 30th packet is keyframe
            if sequence % 30 == 0:
                flags = 1


            header = struct.pack(
                HEADER_FORMAT,
                sequence,
                pts,
                flags,
                len(payload)
            )


            await ws.send(
                header + payload
            )


            print(
                "sent",
                sequence,
                pts
            )


            pts += 33333


            await asyncio.sleep(
                1/30
            )


asyncio.run(main())
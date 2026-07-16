# app/publisher/manager.py

from __future__ import annotations
from .h264_recorder import H264Recorder
import logging
import threading
from typing import Dict, Optional

from .stream_session import StreamSession
from .worker import Worker
from .rtsp_publisher import RtspPublisher
from app.config import RTSP_SERVER

logger = logging.getLogger(__name__)


class Manager:

    def __init__(self):

        # Active sessions
        self.sessions: Dict[str, StreamSession] = {}

        # Thread safety
        self.lock = threading.Lock()

    # Create / Get Session

    def create_session(
        self,
        stream_id: str,
        device_id: str,
    ) -> StreamSession:


        with self.lock:

            existing = self.sessions.get(stream_id)

            if existing:

                logger.info(
                    "Existing session found: %s",
                    stream_id
                )

                return existing

            # Create Worker
            worker = Worker(
                stream_id=stream_id
            )

            rtsp_url = f"{RTSP_SERVER}/live/{stream_id}"

            print("FINAL RTSP URL:", rtsp_url)

            rtsp = RtspPublisher(
                stream_id=stream_id,
                rtsp_url=rtsp_url,

            )

            worker.add_consumer(rtsp)
            # Attach recorder consumer

            recorder = H264Recorder(
                stream_id=stream_id
            )

            worker.add_consumer(
                recorder
            )
            # Create Session
            session = StreamSession(
                stream_id=stream_id,
                device_id=device_id,
            )

            # Connect Session -> Worker
            session.attach_worker(worker)

            # Stor
            self.sessions[stream_id] = session

            # Start Worker thread
            worker.start()


            logger.info(
                "Created new publisher session: %s",
                stream_id
            )


            return session


    # Get Session

    def get_session(
        self,
        stream_id: str
    ) -> Optional[StreamSession]:

        return self.sessions.get(stream_id)

    # Receive Packet Routing

    def publish_packet(
        self,
        stream_id: str,
        packet
    ) -> bool:

        session = self.get_session(stream_id)
        if session is None:

            logger.warning(
                "No session found for %s",
                stream_id
            )

            return False


        session.on_packet(packet)

        return True

    # Disconnect

    def remove_session(
        self,
        stream_id: str
    ):

        with self.lock:

            session = self.sessions.pop(
                stream_id,
                None
            )


            if session is None:
                return


            session.disconnect()


            logger.info(
                "Removed session: %s",
                stream_id
            )

    # Information

    def list_sessions(self):

        return list(
            self.sessions.values()
        )


    def count(self) -> int:

        return len(
            self.sessions
        )

    # Shutdown

    def shutdown(self):

        logger.info(
            "Shutting down publisher manager"
        )


        with self.lock:

            sessions = list(
                self.sessions.values()
            )


            self.sessions.clear()


        for session in sessions:

            session.disconnect()
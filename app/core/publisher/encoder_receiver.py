# app/core/publisher/encoder_receiver.py
import socket
import threading
import struct
import logging
from typing import Optional
from uuid import uuid4
from app.core.publisher.publisher_manager import publisher_manager
from app.core.publisher.packet import Packet

logger = logging.getLogger(__name__)


class EncoderReceiver:
    def __init__(self, host: str = "0.0.0.0", port: int = 4000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

        self.thread = threading.Thread(target=self._accept_loop, daemon=True, name="TCP-Receiver")
        self.thread.start()
        logger.info(f"Indocaster TCP Receiver listening on {self.host}:{self.port}")

    def _accept_loop(self):
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                logger.info(f"New client connection established from {addr}")
                t = threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True)
                t.start()
            except Exception as e:
                if self.running:
                    logger.error(f"Accept loop error: {e}")

    def _handle_client(self, sock: socket.socket):
        sock.settimeout(10.0)
        stream_key = None
        session_id = str(uuid4())

        try:
            # Read dynamic registration metadata from Android handshake (Maximum 512 bytes for full safe URL paths)
            header_buffer = b""
            while b"\n" not in header_buffer and len(header_buffer) < 512:
                chunk = sock.recv(1)
                if not chunk:
                    break
                header_buffer += chunk

            if not header_buffer.endswith(b"\n"):
                logger.error("Invalid streaming handshake initialization sequence.")
                return

            # Parse registration payload: "STREAM_KEY|TARGET_RTSP_URL"
            handshake_str = header_buffer.decode('utf-8').strip()
            if "|" not in handshake_str:
                logger.error(f"Malformed handshake string structure: {handshake_str}. Expected 'KEY|URL'.")
                return

            stream_key, target_rtsp_url = handshake_str.split("|", 1)
            stream_key = stream_key.strip()
            target_rtsp_url = target_rtsp_url.strip()

            logger.info(f"Stream registration accepted. Key: {stream_key} -> Target: {target_rtsp_url}")
            logger.info(
                f"ACTIVE WORKERS: {publisher_manager.workers.keys()}"
            )
            # Ingest target parameters right into our active session registry mapping pipelines
            worker = publisher_manager.get_or_create_worker(stream_key, session_id, target_rtsp_url)

            header_format = ">2sBqI"
            header_size = struct.calcsize(header_format)

            while self.running:
                header_data = self._recv_all(sock, header_size)
                if not header_data:
                    break

                magic, flags, pts, length = struct.unpack(header_format, header_data)

                if magic != b"IN":
                    logger.error(f"Malformed stream alignment signature found on stream {stream_key}.")
                    break

                is_keyframe = bool(flags & 0x01)
                payload = self._recv_all(sock, length)
                if not payload:
                    break

                packet = Packet(
                    data=payload,
                    pts=pts,
                    dts=pts,
                    is_keyframe=is_keyframe
                )
                worker.push_packet(packet)

        except socket.timeout:
            logger.warning(f"Stream interface timed out for {stream_key}")
        except Exception as e:
            logger.error(f"Error handling streaming data logic for {stream_key}: {e}")
        finally:
            sock.close()
            if stream_key:
                logger.info(f"Stream disconnected: {stream_key}")
                publisher_manager.remove_worker(stream_key)

    def _recv_all(self, sock: socket.socket, size: int) -> Optional[bytes]:
        buffer = b""
        while len(buffer) < size:
            packet = sock.recv(size - len(buffer))
            if not packet:
                return None
            buffer += packet
        return buffer

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
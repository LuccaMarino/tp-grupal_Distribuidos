import logging
import os
import socket
import threading

DEFAULT_HEARTBEAT_PORT = int(os.environ.get("HEARTBEAT_PORT", 9000))
PING_MSG = b"PING"
PONG_MSG = b"PONG"
DEFAULT_HOST = "0.0.0.0"

class HeartbeatResponder:
    """
    Listens for UDP PING heartbeat messages and responds with a PONG message. This is used to indicate that the service is alive and responsive.
    """

    def __init__(self, port=DEFAULT_HEARTBEAT_PORT):
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((DEFAULT_HOST, self._port))
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._listen_for_heartbeat,
            name="heartbeat-responder",
            daemon=True
        )

    def start(self):
        self._thread.start()
        logging.info(f"Heartbeat responder started on port {self._port}")

    def stop(self):
        self._stop_event.set()
        try:
            self._socket.close()
        except Exception as e:
            logging.error(f"Error closing heartbeat socket: {e}")

    def _listen_for_heartbeat(self):
        while not self._stop_event.is_set():
            try:
                data, addr = self._socket.recvfrom(64)
                if data == PING_MSG:
                    logging.debug(f"Received PING from {addr}, sending PONG")
                    self._socket.sendto(PONG_MSG, addr)
            except OSError:
                if self._stop_event.is_set():
                    break
            except Exception as e:
                logging.error(f"Error in heartbeat listener: {e}")
                break
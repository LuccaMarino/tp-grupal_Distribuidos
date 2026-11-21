import logging
import socket
import threading
import time

from common.message_protocol import monitor as monitor_protocol
from common.message_protocol.monitor import BullyMessageType

LEADER_HEARTBEAT_INTERVAL = 1.0
LEADER_TIMEOUT = 5.0
ELECTION_TIMEOUT = 1.0

DEFAULT_HOST = "0.0.0.0"


class BullyElection:
    """
    Implements the Bully election algorithm for leader election among distributed monitors.
    Exposes `is_leader()` method to check if the current monitor is the active leader.
    """

    def __init__(self, monitor_id: int, peers: dict[int, str], election_port: int):
        self.id = monitor_id
        self.peers = peers
        self.port = election_port

        self._lock = threading.Lock()
        self._leader_id: int | None = None
        self._last_leader_heartbeat = time.monotonic()
        self._election_in_progress = False
        self._got_alive = False
        self._stop_event = threading.Event()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((DEFAULT_HOST, self.port))
        self._socket.settimeout(0.5)

    def is_leader(self) -> bool:
        with self._lock:
            return self._leader_id == self.id

    def start(self):
        threading.Thread(target=self._listen_loop, name="bully-listener", daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, name="bully-heartbeat", daemon=True).start()
        threading.Thread(target=self.start_election, name="bully-initial-election", daemon=True).start()

    def stop(self):
        self._stop_event.set()
        try:
            self._socket.close()
        except Exception:
            pass

    def _send(self, target_host: str, msg_type: BullyMessageType):
        def _send_async():
            data = monitor_protocol.serialize(msg_type, self.id)
            try:
                self._socket.sendto(data, (target_host, self.port))
                logging.debug(f"Sent {msg_type} to {target_host}")
            except OSError as e:
                logging.debug(f"Could not send {msg_type} to {target_host}: {e}")

        threading.Thread(target=_send_async, daemon=True).start()

    def _broadcast_coordinator(self):
        with self._lock:
            self._leader_id = self.id
            self._last_leader_heartbeat = time.monotonic()
            self._election_in_progress = False
        logging.info(f"Monitor {self.id} is the new leader. Broadcasting COORDINATOR message...")
        for peer_id, peer_host in self.peers.items():
            if peer_id != self.id:
                self._send(peer_host, BullyMessageType.COORDINATOR)

    def start_election(self):
        with self._lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True
            self._got_alive = False

        logging.info("Monitor %d starting election...", self.id)
        higher_peers = [host for peer_id, host in self.peers.items() if peer_id > self.id]

        if not higher_peers:
            self._broadcast_coordinator()
            return
        for host in higher_peers:
            self._send(host, BullyMessageType.ELECTION)
        time.sleep(ELECTION_TIMEOUT)
        with self._lock:
            received_alive = self._got_alive
        if not received_alive:
            self._broadcast_coordinator()
        else:
            time.sleep(ELECTION_TIMEOUT)
            with self._lock:
                self._election_in_progress = False
                if self._leader_id is None or self._leader_id <= self.id:
                    threading.Thread(target=self.start_election, daemon=True).start()

    def _handle_election(self, sender_id: int):
        if sender_id < self.id:
            sender_host = self.peers.get(sender_id)
            if sender_host:
                self._send(sender_host, BullyMessageType.ALIVE)
            if self.is_leader():
                if sender_host:
                    self._send(sender_host, BullyMessageType.COORDINATOR)
            else:
                threading.Thread(target=self.start_election, name="bully-election", daemon=True).start()

    def _handle_alive(self, _sender_id: int):
        with self._lock:
            self._got_alive = True

    def _handle_coordinator(self, sender_id: int):
        if sender_id < self.id:
            logging.info(
                "Monitor %d ignoring COORDINATOR from lower ID %d; starting election...",
                self.id,
                sender_id,
            )
            threading.Thread(target=self.start_election, name="bully-election", daemon=True).start()
            return

        with self._lock:
            if self._leader_id != sender_id:
                logging.info("Monitor %d recognized Monitor %d as leader", self.id, sender_id)
            self._leader_id = sender_id
            self._last_leader_heartbeat = time.monotonic()
            self._election_in_progress = False

    def _listen_loop(self):
        while not self._stop_event.is_set():
            try:
                data, addr = self._socket.recvfrom(256)
                msg_type, sender_id = monitor_protocol.deserialize(data)
                logging.debug(f"Received {msg_type} from {sender_id} at {addr}")

                if msg_type == BullyMessageType.ELECTION:
                    self._handle_election(sender_id)
                elif msg_type == BullyMessageType.ALIVE:
                    self._handle_alive(sender_id)
                elif msg_type == BullyMessageType.COORDINATOR:
                    self._handle_coordinator(sender_id)

            except socket.timeout:
                continue
            except OSError:
                if self._stop_event.is_set():
                    break
            except Exception as e:
                logging.error(f"Error in listen loop: {e}")

    def _heartbeat_loop(self):
        while not self._stop_event.is_set():
            with self._lock:
                is_leader = self._leader_id == self.id
                elapsed = time.monotonic() - self._last_leader_heartbeat
                in_election = self._election_in_progress

            if is_leader:
                for peer_id, host in self.peers.items():
                    if peer_id != self.id:
                        self._send(host, BullyMessageType.COORDINATOR)
                time.sleep(LEADER_HEARTBEAT_INTERVAL)
            else:
                if elapsed > LEADER_TIMEOUT and not in_election:
                    logging.warning("Leader heartbeat timeout. Starting election...")
                    threading.Thread(target=self.start_election, name="bully-election", daemon=True).start()
                time.sleep(0.5)
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.message_protocol import monitor as monitor_protocol
from common.message_protocol.monitor import BullyMessageType

LEADER_HEARTBEAT_INTERVAL = 1.0
LEADER_TIMEOUT = 5.0
ELECTION_TIMEOUT = 1.0

DEFAULT_HOST = "0.0.0.0"
MAX_WORKERS = 4
JOIN_TIMEOUT = 1.0

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
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="bully-worker")
        self._threads: list[threading.Thread] = []

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((DEFAULT_HOST, self.port))
        self._socket.settimeout(0.5)

    def is_leader(self) -> bool:
        """
        Returns True if the current monitor is the active leader, False otherwise.
        """
        with self._lock:
            return self._leader_id == self.id

    def start(self):
        """
        Starts the Bully election algorithm by launching listener and heartbeat threads. One of the threads will trigger an election if no leader is present or if the current leader fails to send heartbeats. The other thread will listen for incoming messages from other monitors and handle them accordingly.
        """
        self._threads = [
            threading.Thread(target=self._listen_loop, name="bully-listener", daemon=True),
            threading.Thread(target=self._heartbeat_loop, name="bully-heartbeat", daemon=True),
        ]
        for t in self._threads:
            t.start()
        self._trigger_election()

    def stop(self):
        self._stop_event.set()
        try:
            self._socket.close()
        except Exception:
            pass
        self._executor.shutdown(wait=False, cancel_futures=True)
        for t in self._threads:
            t.join(timeout=JOIN_TIMEOUT)

    def _trigger_election(self):
        """
        Triggers a new election if one is not already in progress. This method is called when the current monitor detects that the leader has failed or when it receives a COORDINATOR message from a lower-ID monitor.
        """
        if self._stop_event.is_set():
            return
        with self._lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True
        try:
            self._executor.submit(self._run_election)
        except RuntimeError:
            with self._lock:
                self._election_in_progress = False

    def _send(self, target_host: str, msg_type: BullyMessageType):
        """
        Sends a message of the specified type to the target monitor asynchronously. If the current monitor is stopping, it does not send any messages.
        """
        if self._stop_event.is_set():
            return

        def _send_async():
            if self._stop_event.is_set():
                return
            data = monitor_protocol.serialize(msg_type, self.id)
            try:
                self._socket.sendto(data, (target_host, self.port))
                logging.debug(f"Sent {msg_type} to {target_host}")
            except OSError as e:
                logging.debug(f"Could not send {msg_type} to {target_host}: {e}")

        try:
            self._executor.submit(_send_async)
        except RuntimeError:
            pass

    def _broadcast_coordinator(self):
        """
        Broadcasts a COORDINATOR message to all peers, indicating that the current monitor is the new leader. This method is called when the current monitor wins an election or when it detects that no higher-ID monitors are alive.
        """
        with self._lock:
            self._leader_id = self.id
            self._last_leader_heartbeat = time.monotonic()
            self._election_in_progress = False
        logging.info(f"Monitor {self.id} is the new leader. Broadcasting COORDINATOR message...")
        for peer_id, peer_host in self.peers.items():
            if peer_id != self.id:
                self._send(peer_host, BullyMessageType.COORDINATOR)

    def _run_election(self):
        """
        Runs the election process according to the Bully algorithm. It sends ELECTION messages to all higher-ID monitors and waits for ALIVE responses. If no ALIVE responses are received within the timeout, it declares itself the leader and broadcasts a COORDINATOR message. If an ALIVE response is received, it waits for a COORDINATOR message from the new leader.
        """
        if self._stop_event.is_set():
            with self._lock:
                self._election_in_progress = False
            return
        with self._lock:
            self._got_alive = False

        logging.info("Monitor %d starting election...", self.id)
        higher_peers = [host for peer_id, host in self.peers.items() if peer_id > self.id]

        if not higher_peers:
            self._broadcast_coordinator()
            return
        for host in higher_peers:
            self._send(host, BullyMessageType.ELECTION)
        if self._stop_event.wait(ELECTION_TIMEOUT):
            with self._lock:
                self._election_in_progress = False
            return
        with self._lock:
            received_alive = self._got_alive
        if not received_alive:
            self._broadcast_coordinator()
        else:
            coordinator_wait = ELECTION_TIMEOUT * (len(higher_peers) + 1)
            if self._stop_event.wait(coordinator_wait):
                with self._lock:
                    self._election_in_progress = False
                return
            with self._lock:
                self._election_in_progress = False
                should_retry = self._leader_id is None or self._leader_id <= self.id
            if should_retry:
                self._trigger_election()

    def _handle_election(self, sender_id: int):
        """
        Handles an incoming ELECTION message from another monitor. If the sender has a higher ID, it responds with an ALIVE message. If the sender has a lower ID, it responds with a COORDINATOR message if it is the leader, or triggers a new election if it is not.
        """
        if sender_id > self.id:
            with self._lock:
                if self._leader_id == self.id:
                    self._leader_id = None
                self._got_alive = True
                self._last_leader_heartbeat = time.monotonic()
            return
        if sender_id < self.id:
            sender_host = self.peers.get(sender_id)
            if sender_host:
                self._send(sender_host, BullyMessageType.ALIVE)
            if self.is_leader():
                if sender_host:
                    self._send(sender_host, BullyMessageType.COORDINATOR)
            else:
                self._trigger_election()

    def _handle_alive(self, _sender_id: int):
        """
        Handles an incoming ALIVE message from another monitor. This indicates that a higher-ID monitor is alive and will take over the election process. The current monitor will wait for a COORDINATOR message from the new leader.
        """
        with self._lock:
            self._got_alive = True

    def _handle_coordinator(self, sender_id: int):
        """
        Handles an incoming COORDINATOR message from another monitor.
        - If sender_id > self.id: recognizes the sender as the active leader and resets the heartbeat timer.
        - If sender_id < self.id: reaffirms leadership if this node is the leader, ignores the stale message
          if a higher-ID leader is already recognized, or triggers a new election if no valid leader is set
          and no election is currently in progress.
        """
        if sender_id < self.id:
            with self._lock:
                current_leader = self._leader_id
                already_electing = self._election_in_progress
            if current_leader == self.id:
                sender_host = self.peers.get(sender_id)
                if sender_host:
                    self._send(sender_host, BullyMessageType.COORDINATOR)
                return
            if current_leader is not None and current_leader > self.id:
                logging.debug(
                    "Monitor %d ignoring stale COORDINATOR from %d (current leader is %d)",
                    self.id,
                    sender_id,
                    current_leader,
                )
                return
            if not already_electing:
                logging.info(
                    "Monitor %d ignoring COORDINATOR from lower ID %d; starting election...",
                    self.id,
                    sender_id,
                )
                self._trigger_election()
            return
        with self._lock:
            if self._leader_id != sender_id:
                logging.info("Monitor %d recognized Monitor %d as leader", self.id, sender_id)
            self._leader_id = sender_id
            self._last_leader_heartbeat = time.monotonic()
            self._election_in_progress = False

    def _listen_loop(self):
        """
        Main loop that listens for incoming UDP messages from other monitors. It handles ELECTION, ALIVE, and COORDINATOR messages according to the Bully algorithm. If the current monitor is stopping, it exits the loop.
        """
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
        """
        Main loop that sends periodic COORDINATOR messages if the current monitor is the leader. If the current monitor is not the leader, it checks for leader heartbeats and triggers an election if the leader is unresponsive. If the current monitor is stopping, it exits the loop.
        """
        while not self._stop_event.is_set():
            with self._lock:
                is_leader = self._leader_id == self.id
                elapsed = time.monotonic() - self._last_leader_heartbeat
                in_election = self._election_in_progress

            if is_leader:
                for peer_id, host in self.peers.items():
                    if peer_id != self.id:
                        self._send(host, BullyMessageType.COORDINATOR)
                self._stop_event.wait(LEADER_HEARTBEAT_INTERVAL)
            else:
                if elapsed > LEADER_TIMEOUT and not in_election:
                    logging.warning("Leader heartbeat timeout. Starting election...")
                    self._trigger_election()
                self._stop_event.wait(0.5)
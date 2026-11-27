import logging
import os
import signal
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from bully import BullyElection
from common.heartbeat import HeartbeatResponder, PING_MSG, PONG_MSG
from docker_client import DockerClient

MONITOR_ID = int(os.environ["MONITOR_ID"])
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
PEERS_RAW = os.environ["MONITOR_PEERS"] # e.g. "0:monitor_0,1:monitor_1,2:monitor_2"

HEARTBEAT_PORT = int(os.environ["HEARTBEAT_PORT"])
ELECTION_PORT = int(os.environ["ELECTION_PORT"])
CHECK_INTERVAL = float(os.environ["CHECK_INTERVAL"])
PING_TIMEOUT = float(os.environ["PING_TIMEOUT"])
MAX_MISSED_PINGS = int(os.environ["MAX_MISSED_PINGS"])

def parse_peers(peers_raw):
    peers = {}
    for peer in peers_raw.split(","):
        peer_id, peer_name = peer.split(":")
        peers[int(peer_id)] = peer_name
    return peers

class Monitor:
    def __init__(self):
        self.id = MONITOR_ID
        self.container_name = CONTAINER_NAME
        self.peers = parse_peers(PEERS_RAW)
        self.docker_client = DockerClient()
        self.election = BullyElection(self.id, self.peers, ELECTION_PORT)

        self.stop_event = threading.Event()
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)
        self.missed_pings: dict[str, int] = {}
        self._missed_lock = threading.Lock() # Lock to protect access to missed_pings
        self._executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="healthcheck")
        self.targets = [
            name
            for name in self.docker_client.get_monitored_container_names()
            if name != self.container_name
        ]

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM. Stopping monitor...")
        self.stop_event.set()

    def _ping_target(self, target_name: str) -> bool:
        """
        Pings a target monitor by sending a UDP message and waiting for a response.
        Returns True if the target responds with a PONG message, False otherwise.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(PING_TIMEOUT)
            try:
                sock.sendto(PING_MSG, (target_name, HEARTBEAT_PORT))
                data, _ = sock.recvfrom(64)
                return data == PONG_MSG
            except (socket.timeout, socket.gaierror, OSError):
                return False

    def _check_target(self, target_name: str):
        """
        Checks the health of a target monitor by pinging it. If the target is unresponsive for a certain number of consecutive pings, it restarts the corresponding container.
        """
        if self.stop_event.is_set():
            return
        if self._ping_target(target_name):
            with self._missed_lock:
                self.missed_pings[target_name] = 0
            return
        with self._missed_lock:
            missed = self.missed_pings.get(target_name, 0) + 1
            self.missed_pings[target_name] = missed
            should_restart = missed >= MAX_MISSED_PINGS
            if should_restart:
                self.missed_pings[target_name] = 0
        if should_restart:
            logging.warning(f"Container {target_name} is unresponsive. Restarting...")
            self.docker_client.restart_container(target_name)

    def _healthcheck_loop(self):
        """
        Main loop that checks the health of peer monitors. If the current monitor is the leader, it pings each target monitor and restarts unresponsive containers.
        """
        while not self.stop_event.is_set():
            if self.election.is_leader():
                futures = [
                    self._executor.submit(self._check_target, target)
                    for target in self.targets
                ]
                for f in futures:
                    f.result()
            else:
                with self._missed_lock:
                    self.missed_pings.clear()
            self.stop_event.wait(CHECK_INTERVAL)

    def start(self):
        self.election.start()
        try:
            self._healthcheck_loop()
        finally:
            self.stop_event.set()
            self._executor.shutdown(wait=False, cancel_futures=True)
            self.election.stop()

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Starting monitor {MONITOR_ID} for container {CONTAINER_NAME}")
    heartbeat_responder = HeartbeatResponder()
    heartbeat_responder.start()
    monitor = Monitor()
    try:
        monitor.start()
    finally:
        heartbeat_responder.stop()

if __name__ == "__main__":
    main()
import logging
import os
import socket
import threading
import time

from bully import BullyElection
from common.heartbeat import HeartbeatResponder, PING_MSG, PONG_MSG
from docker_client import DockerClient

MONITOR_ID = int(os.environ["MONITOR_ID"])
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
PEERS_RAW = os.environ["MONITOR_PEERS"] # e.g. "0:monitor_0,1:monitor_1,2:monitor_2"

HEARTBEAT_PORT = int(os.environ.get("HEARTBEAT_PORT", 9000))
ELECTION_PORT = int(os.environ.get("ELECTION_PORT", 9001))
CHECK_INTERVAL = float(os.environ.get("CHECK_INTERVAL", 5.0))
PING_TIMEOUT = float(os.environ.get("PING_TIMEOUT", 2.0))
MAX_MISSED_PINGS = int(os.environ.get("MAX_MISSED_PINGS", 3))

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
        self.ping_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ping_socket.settimeout(PING_TIMEOUT)
        self.missed_pings: dict[str, int] = {}

    def _ping_target(self, target_name: str) -> bool:
        try:
            self.ping_socket.sendto(PING_MSG, (target_name, HEARTBEAT_PORT))
            data, _ = self.ping_socket.recvfrom(64)
            return data == PONG_MSG
        except (socket.timeout, socket.gaierror, OSError) as e:
            return False

    def _healthcheck_loop(self):
        while not self.stop_event.is_set():
            if self.election.is_leader():
                targets = self.docker_client.get_monitored_container_names()
                for target in targets:
                    if target == self.container_name:
                        continue
                    if self._ping_target(target):
                        self.missed_pings[target] = 0
                    else:
                        self.missed_pings[target] = self.missed_pings.get(target, 0) + 1
                        if self.missed_pings[target] >= MAX_MISSED_PINGS:
                            logging.warning(f"Container {target} is unresponsive. Restarting...")
                            self.docker_client.restart_container(target)
                            self.missed_pings[target] = 0
            else:
                self.missed_pings.clear()
            time.sleep(CHECK_INTERVAL)

    def start(self):
        self.election.start()
        try:
            self._healthcheck_loop()
        finally:
            self.stop_event.set()
            self.election.stop()
            self.ping_socket.close()

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
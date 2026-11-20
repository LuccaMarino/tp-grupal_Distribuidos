import json
import os
import random
import time
from docker_client import DockerClient
import logging

INITIAL_DELAY_SECONDS = int(os.environ.get("INITIAL_DELAY_SECONDS", 10))
MIN_INTERVAL_SECONDS = int(os.environ.get("MIN_INTERVAL_SECONDS", 10))
MAX_INTERVAL_SECONDS = int(os.environ.get("MAX_INTERVAL_SECONDS", 90))

class ChaosMonkey:
    def __init__(self, docker_client):
        self.docker_client = docker_client

    def run(self):
        time.sleep(INITIAL_DELAY_SECONDS)
        while True:
            targets = self.docker_client.get_targets()
            if targets:
                target = random.choice(targets)
                logging.info( "Killing container: %s", target.name)
                self.docker_client.kill(target)
            else:
                logging.info( "No targets found to kill.")
            interval = random.randint(MIN_INTERVAL_SECONDS, MAX_INTERVAL_SECONDS)
            logging.info( "Next kill in %d seconds", interval)
            time.sleep(interval)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info( "Chaos Monkey started. Initial delay: %d seconds", INITIAL_DELAY_SECONDS)
    docker_client = DockerClient()
    chaos_monkey = ChaosMonkey(docker_client)
    chaos_monkey.run()

   
if __name__ == "__main__":
    main()
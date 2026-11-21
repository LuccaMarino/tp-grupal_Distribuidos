import logging
import docker
from docker.errors import NotFound, APIError

class DockerClient:
    def __init__(self):
        self.client = docker.from_env()

    def get_monitored_container_names(self):
        """
        Discovers all containers (running or stopped) that have the label 'chaos.monkey.enabled=true' and returns their names.
        """
        containers = self.client.containers.list(
            all=True, filters={"label": "chaos.monkey.enabled=true"}
        )
        return [c.name for c in containers]

    def restart_container(self, container_name):
        """
        Restarts a container by its name. If the container is not found or an API error occurs, it logs the error.
        """
        try:
            container = self.client.containers.get(container_name)
            container.start()
            logging.info(f"Container '{container_name}' restarted successfully.")
        except NotFound:
            logging.error(f"Container '{container_name}' not found.")
        except APIError as e:
            logging.error(f"Failed to restart container '{container_name}': {e}")
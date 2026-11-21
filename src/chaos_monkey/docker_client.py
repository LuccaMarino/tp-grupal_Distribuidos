import docker


class DockerClient:
    def __init__(self):
        self.client = docker.from_env()

    def get_targets(self):
        containers = self.client.containers.list(
            filters={"label": "chaos.monkey.enabled=true"})
        return containers

    def kill(self, container):
        container.kill()
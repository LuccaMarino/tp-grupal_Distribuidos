import os
import signal
import socket
import time

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

class Client:
    def __init__(self):
        self.closed = False
        self.server_socket = None
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        print("SIGTERM received. Closing client...")
        self.closed = True
        self.disconnect()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)
            self.server_socket.close()
            self.server_socket = None

def main():
    client = Client()
    try:
        for i in range(1, 6):
            if client.closed:
                break
            print(f"Processing step {i}/5...", flush=True)
            time.sleep(1)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.disconnect()
        print("Client disconnected.")

if __name__ == "__main__":
    main()
import pyarrow.parquet as pq
import logging
import os
import signal
import socket
import time

from common import message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
RETRY_COUNT = int(os.environ["RETRY_COUNT"])
DELAY_BETWEEN_RETRIES = int(os.environ["DELAY_BETWEEN_RETRIES"])

class Client:
    def __init__(self):
        self.closed = False
        self.server_socket = None
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received. Closing client...")
        self.closed = True
        self.disconnect()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        for attempt in range(RETRY_COUNT):
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.connect((server_host, server_port))
                logging.info(f"Successfully connected to {server_host}:{server_port} in attempt {attempt + 1}")
                return
            except ConnectionRefusedError:
                logging.error(f"Connection refused. Retrying in {DELAY_BETWEEN_RETRIES} seconds...")
                time.sleep(DELAY_BETWEEN_RETRIES)
        raise ConnectionError(f"Failed to connect to {server_host}:{server_port} after {RETRY_COUNT} attempts.")

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)
            self.server_socket.close()
            self.server_socket = None

    def send_wikipedia_articles(self, input_file):
        parquet_file = pq.ParquetFile(input_file)
        total_articles = parquet_file.metadata.num_rows
        print(f"Total articles to send: {total_articles}")
        ack_count = 0
        for batch in parquet_file.iter_batches(batch_size=500):
            for article in batch.to_pylist():
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.WIKI_ARTICLE,
                    article
                )
                message_protocol.external.recv_msg(self.server_socket)
                ack_count += 1
                if ack_count % 10000 == 0:
                    print(f"ACK received from gateway for {ack_count} / {total_articles} articles.")
        if ack_count % 10000 != 0:
            print(f"ACK received from gateway for {ack_count} / {total_articles} articles.")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_OF_RECORDS
        )
        logging.info("All articles sent. Waiting for final acknowledgment...")
        message_protocol.external.recv_msg(self.server_socket)

def main():
    client = Client()
    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_wikipedia_articles(INPUT_FILE)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        client.disconnect()
        logging.info("Client disconnected.")

if __name__ == "__main__":
    main()
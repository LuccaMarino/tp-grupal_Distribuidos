import pyarrow.parquet as pq
import logging
import os
import signal
import socket
import time

from common import message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_DIR = os.environ["INPUT_DIR"]
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

    def send_wikipedia_articles(self, input_dir):
        parquet_files = [
            os.path.join(input_dir, f)
            for f in os.listdir(input_dir)
            if f.endswith(".parquet")
        ]
        total_articles_sent = 0
        for parquet_file_path in parquet_files:
            filename = os.path.basename(parquet_file_path)
            parquet_file = pq.ParquetFile(parquet_file_path)
            total_articles = parquet_file.metadata.num_rows
            logging.info(f"Sending {total_articles} articles from {filename}...")
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
                    total_articles_sent += 1
                    if ack_count % 10000 == 0:
                        print(f"({filename}) ACK received from gateway for {ack_count} / {total_articles} articles.")
            if ack_count % 10000 != 0:
                print(f"({filename}) ACK received from gateway for {ack_count} / {total_articles} articles.")
        print(f"All {len(parquet_files)} parquet files processed. Total articles sent: {total_articles_sent}.")
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
        client.send_wikipedia_articles(INPUT_DIR)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        client.disconnect()
        logging.info("Client disconnected.")

if __name__ == "__main__":
    main()
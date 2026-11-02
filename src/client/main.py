import pyarrow.parquet as pq
import logging
import os
import signal
import socket
import time
import threading
import csv
from common import message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_DIR = os.environ["INPUT_DIR"]
OUTPUT_FILE_Q1 = os.environ["OUTPUT_FILE_Q1"]
OUTPUT_FILE_Q2 = os.environ["OUTPUT_FILE_Q2"]
OUTPUT_FILE_Q3 = os.environ["OUTPUT_FILE_Q3"]
OUTPUT_FILE_Q4 = os.environ["OUTPUT_FILE_Q4"]
OUTPUT_FILE_Q5 = os.environ["OUTPUT_FILE_Q5"]
RETRY_COUNT = int(os.environ["RETRY_COUNT"])
DELAY_BETWEEN_RETRIES = int(os.environ["DELAY_BETWEEN_RETRIES"])
QUERY_OUTPUTS = {
    message_protocol.internal.Query.Q1: {"path": OUTPUT_FILE_Q1,"fields": ["url", "date_modified"]},
    message_protocol.internal.Query.Q2: {"path": OUTPUT_FILE_Q2,"fields": ["name", "url", "image"]},
    message_protocol.internal.Query.Q3: {"path": OUTPUT_FILE_Q3,"fields": ["word"]},
    message_protocol.internal.Query.Q4: {"path": OUTPUT_FILE_Q4,"fields": ["word", "count"]},
    message_protocol.internal.Query.Q5: {"path": OUTPUT_FILE_Q5,"fields": ["url", "word_count"]}
    }

class Client:
    def __init__(self):
        self.closed = threading.Event()
        self.server_socket = None
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.output_files = {}
        self.csv_writers = {}
        for query, config in QUERY_OUTPUTS.items():
            output_file = open(config["path"],"w",encoding="utf-8",newline="",)
            writer = csv.writer(output_file)
            writer.writerow(config["fields"])
            self.output_files[query] = output_file
            self.csv_writers[query] = writer
        self.finished_queries = set()

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received. Closing client...")
        self.closed.set()
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
        try:
            if self.server_socket:
                self.server_socket.shutdown(socket.SHUT_RDWR)
                self.server_socket.close()
                self.server_socket = None
        except:
            self.server_socket.close()
            self.server_socket = None

    def write_query_result(self, query, result):
        writer = self.csv_writers.get(query)
        if writer is None:
            raise ValueError(f"Unknown query: {query}")
        writer.writerows(result)

    def receive_messages(self):
        ack_count = 0
        try:
            while not self.closed.is_set():
                msg_type, data = message_protocol.external.recv_msg(self.server_socket)
                if msg_type == message_protocol.external.MsgType.ACK:
                    ack_count += 1
                elif msg_type == message_protocol.external.MsgType.QUERY_RESULT:
                    query, result = data
                    self.write_query_result(query, result)
                elif msg_type == message_protocol.external.MsgType.END_OF_QUERY_RESULTS:
                    self.finished_queries.add(data)
                    logging.info(f"Finished query: {data}")
                    if len(self.finished_queries) == 5:
                        logging.info("All queries finished.")
                        break
        except Exception as e:
            if not self.closed.is_set():
                logging.error(f"Error receiving messages: {e}")
        finally:
            self.closed.set()
            logging.info(f"ACKs received: {ack_count}")

    def send_wikipedia_articles(self, input_dir):
        parquet_files = [
            os.path.join(input_dir, f)
            for f in os.listdir(input_dir)
            if f.endswith(".parquet")
        ]
        sent_count = 0
        for parquet_file_path in parquet_files:
            filename = os.path.basename(parquet_file_path)
            parquet_file = pq.ParquetFile(parquet_file_path)
            total_articles = parquet_file.metadata.num_rows
            logging.info(f"Sending {total_articles} articles from {filename}...")
            sent_count = 0
            for batch in parquet_file.iter_batches(batch_size=500):
                for article in batch.to_pylist():
                    message_protocol.external.send_msg(
                        self.server_socket,
                        message_protocol.external.MsgType.WIKI_ARTICLE,
                        article
                    )
                    sent_count += 1
                    if sent_count % 10000 == 0:
                            print(f"(Articles sent to gateway {sent_count} / {total_articles} .")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_OF_RECORDS
        )
        logging.info("All articles sent. Waiting for final acknowledgment...")

    def close_output_files(self):
        for output_file in self.output_files.values():
            output_file.close()

def main():
    client = Client()
    receiver = None
    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        receiver = threading.Thread(target=client.receive_messages,daemon=True)
        receiver.start()
        client.send_wikipedia_articles(INPUT_DIR)
        receiver.join()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        client.disconnect()
        if receiver is not None:
            receiver.join()
        client.close_output_files()
        logging.info("Client disconnected.")

if __name__ == "__main__":
    main()
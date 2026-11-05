import logging
import os
import signal
import threading

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
NEW_ARTICLES_GROUP = os.environ["NEW_ARTICLES_GROUP"]
SUM_PARTIAL_LENGTH_GROUP = os.environ["SUM_PARTIAL_LENGTH_GROUP"]
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
EXPECTED_WORDSUM_EOFS = int(os.environ["EXPECTED_WORDSUM_EOFS"])
EXPECTED_WORDCOUNTER_EOFS = int(os.environ["EXPECTED_WORDCOUNTER_EOFS"])
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]
BATCH_SIZE = int(os.environ["BATCH_SIZE"])

class Q5Joiner:
    """
    Joiner for the Q5 query. It receives messages from the word sum and word counter nodes, accumulates the new articles and their lengths, and sends the final results to the gateway when all EOFs have been received.
    """
    def __init__(self):
        """
        Initializes the Q5Joiner by setting up the input group members for new articles and sum partial length, buffers for global statistics, and EOF tracking for each client. It also sets up a lock for thread-safe operations and creates the storage directory if it doesn't exist.
        """
        self._new_articles = GroupMember(MOM_HOST, NEW_ARTICLES_GROUP, ID)
        self._sum_partial_length = GroupMember(MOM_HOST, SUM_PARTIAL_LENGTH_GROUP, ID)
        self._global_stats = {}  # client_id -> [total_words, total_articles]
        self._received_word_sum_eofs = {}
        self._received_word_counter_eofs = {}
        self._lock = threading.Lock()
        os.makedirs(STORAGE_PATH, exist_ok=True)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer threads for both new articles and sum partial length. This allows the joiner to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._new_articles.stop_consuming_threadsafe()
        self._sum_partial_length.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group members for new articles and sum partial length, handling any exceptions that may occur during the close operation.
        """
        for connection in (self._new_articles, self._sum_partial_length):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _client_file_path(self, client_id):
        """
        Returns the file path for the new articles of a given client.
        """
        return os.path.join(STORAGE_PATH, f"new_articles_client_{client_id}.txt")

    def _process_new_articles(self, client_id, records):
        """
        Processes incoming new articles messages by appending the records to a file specific to the client. This allows persisting the new articles for each client in a separate file, which will be used later to compute the final results.
        """
        file_path = self._client_file_path(client_id)
        with self._lock:
            with open(file_path, "a") as f:
                for record in records:
                    url, length = record
                    f.write(f"{url}\t{length}\n")

    def _process_articles_eof(self, client_id, sender_id):
        """
        Processes incoming EOF messages for new articles by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it checks if the processing is complete and finalizes the query if so.
        """
        logging.info(f"Received EOF from word sum {sender_id} for client {client_id}")
        with self._lock:
            if client_id not in self._received_word_sum_eofs:
                self._received_word_sum_eofs[client_id] = set()
            self._received_word_sum_eofs[client_id].add(sender_id)
            self._check_processing_completion(client_id)

    def _check_processing_completion(self, client_id):
        """
        Checks if all expected EOFs have been received for both new articles and sum partial length for a given client. If so, it computes the query finalization.
        """
        new_articles_done = len(self._received_word_sum_eofs.get(client_id, set())) >= EXPECTED_WORDSUM_EOFS
        sum_partial_length_done = len(self._received_word_counter_eofs.get(client_id, set())) >= EXPECTED_WORDCOUNTER_EOFS
        if new_articles_done and sum_partial_length_done:
            self._finalize_query(client_id)

    def _process_sum_partial_length(self, client_id, totals):
        """
        Processes incoming sum partial length messages by updating the global statistics for the specified client with the total words and total articles received from a word counter. This allows accumulating the overall statistics for each client, which will be used later to calculate the mean length of abstracts for old articles.
        """
        words, articles = totals
        with self._lock:
            if client_id not in self._global_stats:
                self._global_stats[client_id] = [0, 0]
            self._global_stats[client_id][0] += words
            self._global_stats[client_id][1] += articles

    def _process_sum_partial_length_eof(self, client_id, sender_id):
        """
        Processes incoming EOF messages for sum partial length by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it checks if the processing is complete and finalizes the query if so.
        """
        logging.info(f"Received EOF from {sender_id} for client {client_id}")
        with self._lock:
            if client_id not in self._received_word_counter_eofs:
                self._received_word_counter_eofs[client_id] = set()
            self._received_word_counter_eofs[client_id].add(sender_id)
            self._check_processing_completion(client_id)

    def _finalize_query(self, client_id):
        """
        Finalizes the query for a given client by computing the mean length of abstracts for old articles and sending the new articles (in batches) that exceed this mean to the gateway. It also sends an EOF message to indicate the end of results for this client and cleans up the state associated with the client.
        """
        total_words, total_articles = self._global_stats.get(client_id, [0, 0])
        old_articles_average = (total_words / total_articles) if total_articles > 0 else 0.0
        file_path = self._client_file_path(client_id)
        results_sender = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                batch = []
                for line in f:
                    url, length_str = line.strip().split("\t")
                    length = int(length_str)
                    if length > old_articles_average:
                        batch.append([url, length])
                        if len(batch) >= BATCH_SIZE:
                            result_message = message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q5, batch)
                            results_sender.send(result_message)
                            batch = []
                if batch:
                    result_message = message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q5, batch)
                    results_sender.send(result_message)
            os.remove(file_path)
        eof_message = message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q5, ID)
        results_sender.broadcast(eof_message)
        try:
            results_sender.close()
        except MessageMiddlewareCloseError as e:
            logging.error(e)
        self._global_stats.pop(client_id, None)
        self._received_word_sum_eofs.pop(client_id, None)
        self._received_word_counter_eofs.pop(client_id, None)

    def _process_new_articles_message(self, message, ack, nack):
        """
        Processes incoming messages for new articles by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_new_articles(client_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_articles_eof(client_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type: {msg_type}")
        except (ValueError, TypeError, AttributeError) as e:
            logging.error(f"Discarding malformed message: {e!r}")
        ack()

    def _process_sum_partial_length_message(self, message, ack, nack):
        """
        Processes incoming messages for sum partial length by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_sum_partial_length(client_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_sum_partial_length_eof(client_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type: {msg_type}")
        except (ValueError, TypeError, AttributeError) as e:
            logging.error(f"Discarding malformed message: {e!r}")
        ack()

    def start(self):
        """
        Starts the Q5Joiner service by creating and starting two threads for consuming messages from the new articles and sum partial length group members. It waits for both threads to finish and ensures proper cleanup by closing the connections.
        """
        t1 = threading.Thread(
            target=self._new_articles.start_consuming,
            args=(self._process_new_articles_message,)
        )
        t2 = threading.Thread(
            target=self._sum_partial_length.start_consuming,
            args=(self._process_sum_partial_length_message,)
        )

        t1.start()
        t2.start()

        try:
            t1.join()
            t2.join()
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Q5 joiner service...")
    q5_joiner = Q5Joiner()
    q5_joiner.start()
    return 0

if __name__ == "__main__":
    main()
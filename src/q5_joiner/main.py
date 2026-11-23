import logging
import os
import signal
import threading

from common import message_protocol
from common.heartbeat import HeartbeatResponder
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
from common.state_storage import ClientStateStorage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
NEW_ARTICLES_GROUP = os.environ["NEW_ARTICLES_GROUP"]
SUM_PARTIAL_LENGTH_GROUP = os.environ["SUM_PARTIAL_LENGTH_GROUP"]
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
EXPECTED_WORDSUM_EOFS = int(os.environ["EXPECTED_WORDSUM_EOFS"])
EXPECTED_WORDCOUNTER_EOFS = int(os.environ["EXPECTED_WORDCOUNTER_EOFS"])
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]
DATA_PATH = os.environ["DATA_PATH"]
BATCH_SIZE = int(os.environ["BATCH_SIZE"])

class Q5Joiner:
    """
    Joiner for the Q5 query. It receives messages from the word sum and word counter nodes, accumulates the new articles and their lengths, and sends the final results to the gateway when all EOFs have been received.
    """
    def __init__(self):
        """
        Initializes the Q5Joiner by setting up the input group members for new articles and sum partial length, the results sender to the gateway, buffers for global statistics, and EOF tracking for each client. It also sets up a lock for thread-safe operations and creates the storage directory if it doesn't exist.
        """
        self._new_articles = GroupMember(MOM_HOST, NEW_ARTICLES_GROUP, ID)
        self._sum_partial_length = GroupMember(MOM_HOST, SUM_PARTIAL_LENGTH_GROUP, ID)
        self._results_sender = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._lock = threading.Lock()
        os.makedirs(STORAGE_PATH, exist_ok=True)
        os.makedirs(DATA_PATH, exist_ok=True)
        self._storage = ClientStateStorage(STORAGE_PATH, lambda: {"total_words": 0,"total_articles": 0,"word_sum_eofs": [],"word_counter_eofs": [],})
        self._storage.load(on_recover=self._on_recover)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _on_recover(self, client_id, state, eofs, pending_sends):
        state.setdefault("total_words", 0)
        state.setdefault("total_articles", 0)
        state.setdefault("word_sum_eofs", [])
        state.setdefault("word_counter_eofs", [])
        logging.info("Recovered state for client %s: stats=%s/%s, eofs=%s",client_id,state["total_words"],state["total_articles"],eofs,)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer threads for both new articles and sum partial length. This allows the joiner to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._new_articles.stop_consuming_threadsafe()
        self._sum_partial_length.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group members for new articles and sum partial length, and the results sender, handling any exceptions that may occur during the close operation.
        """
        for connection in (self._new_articles, self._sum_partial_length, self._results_sender):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _client_file_path(self, client_id):
        """
        Returns the file path for the new articles of a given client.
        """
        return os.path.join(DATA_PATH, f"new_articles_client_{client_id}.txt")

    def _process_new_articles(self, client_id, message_id, records):
        """
        Processes incoming new articles messages by appending the records to a file specific to the client. This allows persisting the new articles for each client in a separate file, which will be used later to compute the final results.
        """
        with self._lock:
            if self._storage.is_completed(client_id):
                return
            if self._storage.is_duplicate_msg(client_id, message_id):
                logging.debug("Duplicate new-articles message %s for client %s, ignoring.", message_id, client_id)
                return

            file_path = self._client_file_path(client_id)
            with open(file_path, "a", encoding="utf-8") as f:
                for record in records:
                    url, length = record
                    f.write(f"{message_id}\t{url}\t{length}\n")
                f.flush()

            self._storage.mark_msg_seen(client_id, message_id)
            self._storage.persist(client_id)

    def _process_articles_eof(self, client_id, message_id, sender_id):
        """
        Processes incoming EOF messages for new articles by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it checks if the processing is complete and finalizes the query if so.
        """
        logging.info(f"Received EOF from word sum {sender_id} for client {client_id}")
        with self._lock:
            if self._storage.is_completed(client_id):
                return
            if self._storage.is_duplicate_eof(client_id, f"word_sum:{sender_id}"):
                return

            state = self._ensure_state(client_id)
            if sender_id not in state["word_sum_eofs"]:
                state["word_sum_eofs"].append(sender_id)
            self._storage.add_eof(client_id, message_id, f"word_sum:{sender_id}")
            self._storage.persist(client_id)
            self._check_processing_completion(client_id)

    def _ensure_state(self, client_id):
        state = self._storage.get_state(client_id)
        state.setdefault("total_words", 0)
        state.setdefault("total_articles", 0)
        state.setdefault("word_sum_eofs", [])
        state.setdefault("word_counter_eofs", [])
        return state

    def _check_processing_completion(self, client_id):
        """
        Checks if all expected EOFs have been received for both new articles and sum partial length for a given client. If so, it computes the query finalization.
        """
        state = self._ensure_state(client_id)
        new_articles_done = len(state["word_sum_eofs"]) >= EXPECTED_WORDSUM_EOFS
        sum_partial_length_done = len(state["word_counter_eofs"]) >= EXPECTED_WORDCOUNTER_EOFS
        if new_articles_done and sum_partial_length_done:
            self._finalize_query(client_id)

    def _process_sum_partial_length(self, client_id, message_id, totals):
        """
        Processes incoming sum partial length messages by updating the global statistics for the specified client with the total words and total articles received from a word counter. This allows accumulating the overall statistics for each client, which will be used later to calculate the mean length of abstracts for old articles.
        """
        words, articles = totals
        with self._lock:
            if self._storage.is_completed(client_id):
                return
            if self._storage.is_duplicate_msg(client_id, message_id):
                logging.debug("Duplicate sum-partial message %s for client %s, ignoring.", message_id, client_id)
                return
            state = self._ensure_state(client_id)
            state["total_words"] += words
            state["total_articles"] += articles
            self._storage.mark_msg_seen(client_id, message_id)
            self._storage.persist(client_id)

    def _process_sum_partial_length_eof(self, client_id, message_id, sender_id):
        """
        Processes incoming EOF messages for sum partial length by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it checks if the processing is complete and finalizes the query if so.
        """
        logging.info(f"Received EOF from {sender_id} for client {client_id}")
        with self._lock:
            if self._storage.is_completed(client_id):
                return
            if self._storage.is_duplicate_eof(client_id, f"word_counter:{sender_id}"):
                return

            state = self._ensure_state(client_id)
            if sender_id not in state["word_counter_eofs"]:
                state["word_counter_eofs"].append(sender_id)
            self._storage.add_eof(client_id, message_id, f"word_counter:{sender_id}")
            self._storage.persist(client_id)
            self._check_processing_completion(client_id)

    def _finalize_query(self, client_id):
        """
        Finalizes the query for a given client by computing the mean length of abstracts for old articles and sending the new articles (in batches) that exceed this mean to the gateway. It also sends an EOF message to indicate the end of results for this client and cleans up the state associated with the client.
        """
        if self._storage.is_completed(client_id):
            return

        logging.info("Finalizing query for client %s", client_id)

        state = self._ensure_state(client_id)
        total_words = state["total_words"]
        total_articles = state["total_articles"]
        old_articles_average = (total_words / total_articles) if total_articles > 0 else 0.0
        file_path = self._client_file_path(client_id)
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                batch = []
                batch_idx = 0
                for line in f:
                    fields = line.rstrip("\n").split("\t")
                    if len(fields) == 3:
                        _article_message_id, url, length_str = fields
                    elif len(fields) == 2:
                        url, length_str = fields
                    else:
                        logging.error("Discarding malformed article record for client %s: %r", client_id, line)
                        continue
                    length = int(length_str)
                    if length > old_articles_average:
                        batch.append([url, length])
                        if len(batch) >= BATCH_SIZE:
                            result_message = message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q5, batch, message_id=f"q5_join_{ID}.{batch_idx}")
                            self._results_sender.send(result_message)
                            batch = []
                            batch_idx += 1
                if batch:
                    result_message = message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q5, batch, message_id=f"q5_join_{ID}.{batch_idx}")
                    self._results_sender.send(result_message)
            os.remove(file_path)
        eof_message = message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q5, ID, message_id=f"q5_join_{ID}.eof")
        self._results_sender.broadcast(eof_message)
        self._storage.mark_completed(client_id)

    def _process_new_articles_message(self, message, ack, nack):
        """
        Processes incoming messages for new articles by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, message_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_new_articles(client_id, message_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_articles_eof(client_id, message_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type: {msg_type}")
            ack()
        except (ValueError, TypeError, AttributeError) as e:
            logging.error(f"Discarding malformed message: {e!r}")
            ack()
        except Exception as e:
            logging.error(f"Error processing new-articles message: {e!r}")
            nack()

    def _process_sum_partial_length_message(self, message, ack, nack):
        """
        Processes incoming messages for sum partial length by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, message_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_sum_partial_length(client_id, message_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_sum_partial_length_eof(client_id, message_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type: {msg_type}")
            ack()
        except (ValueError, TypeError, AttributeError) as e:
            logging.error(f"Discarding malformed message: {e!r}")
            ack()
        except Exception as e:
            logging.error(f"Error processing sum-partial message: {e!r}")
            nack()

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
    heartbeat_responder = HeartbeatResponder()
    heartbeat_responder.start()
    q5_joiner = Q5Joiner()
    try:
        q5_joiner.start()
    finally:
        heartbeat_responder.stop()
    return 0

if __name__ == "__main__":
    main()
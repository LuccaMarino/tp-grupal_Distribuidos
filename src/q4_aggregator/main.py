import logging
import os
import signal

from common import message_protocol
from common.bucket_storage import BucketStorage
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
JOINER_GROUP = os.environ["JOINER_GROUP"]
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
WORD_SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]
BUCKET_AMOUNT = int(os.environ["BUCKET_AMOUNT"])

# Quantity of words to accumulate in memory before flushing to disk. This is a tradeoff between memory usage and disk I/O.
FLUSH_WORDS = int(os.environ["FLUSH_WORDS"])

JOINER_AMOUNT = 1
TOP_SIZE = 20

EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT

def _top_order(word_total):
    """
    Returns a tuple that can be used to sort words by total count in descending order, and by word in ascending order for ties.
    """
    word, total = word_total
    return (-total, word)

def _add_totals(stored_values, new_values):
    """
    Combines the total stored on disk with the total in the buffer. The values are [total].
    """
    return [stored_values[0] + new_values[0]]

class Q4Aggregator:
    """
    Aggregator for the Q4 query. It receives messages from the word sum nodes, accumulates the totals in memory, and flushes them to disk when the buffer reaches a certain size. When it receives all EOFs for a client, it computes the partial top 20 words and sends them to the joiner.
    """
    def __init__(self):
        """
        Initializes the Q4Aggregator by setting up the input group member, buffers, storages, and EOF tracking for each client. It also sets up a signal handler for SIGTERM to allow graceful shutdown.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._buffers_by_client = {}   # client_id -> {word: [total]} with the totals accumulated in memory
        self._storages_by_client = {}  # client_id -> BucketStorage with the totals stored on disk
        self._eofs_by_client = {}      # client_id -> set with the sender_id of the received EOFs
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer thread. This allows the aggregator to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group member, handling any exceptions that may occur during the close operation. This is called when the aggregator is shutting down to ensure that resources are released properly.
        """
        try:
            self._input.close()
        except MessageMiddlewareCloseError as e:
            logging.error(e)

    def _process_data(self, client_id, records):
        """
        Processes incoming data messages by accumulating word totals in the buffer for the specified client. If the buffer reaches the defined threshold (FLUSH_WORDS), it flushes the buffer to disk using the associated BucketStorage instance.
        """
        if client_id not in self._buffers_by_client:
            self._buffers_by_client[client_id] = {}
            self._storages_by_client[client_id] = BucketStorage(STORAGE_PATH, client_id, BUCKET_AMOUNT)
        buffer = self._buffers_by_client[client_id]
        for word, total in records:
            if word not in buffer:
                buffer[word] = [0]
            buffer[word][0] += total
        if len(buffer) >= FLUSH_WORDS:
            self._flush_buffer(client_id)

    def _flush_buffer(self, client_id):
        """
        Flushes the buffer for the specified client by merging the accumulated word totals in memory with the totals stored on disk. After merging, it clears the buffer to free up memory.
        """
        buffer = self._buffers_by_client[client_id]
        if len(buffer) == 0:
            return
        self._storages_by_client[client_id].merge(buffer, _add_totals)
        self._buffers_by_client[client_id] = {}

    def _process_eof(self, client_id, sender_id):
        """
        Processes incoming EOF messages by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it triggers the flush operation to compute and send the top 20 words to the joiner.
        """
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == EXPECTED_EOFS:
            self._flush_client(client_id)

    def _top_words(self, storage):
        """
        Computes the local top 20 words by iterating through each bucket in the BucketStorage. For each bucket, it combines the current top candidates with the words in the bucket, sorts them according to the defined order, and retains only the top 20 words.
        """
        top = []
        for bucket_index in range(storage.bucket_amount()):
            candidates = top
            for word, values in storage.read_bucket(bucket_index).items():
                candidates.append([word, values[0]])
            sorted_candidates = sorted(candidates, key=_top_order)
            top = sorted_candidates[:TOP_SIZE]
        return top

    def _flush_client(self, client_id):
        """
        Flushes the remaining buffer for the specified client, computes the top 20 words from the stored data, and sends the results along with an EOF message to the joiner. After sending, it cleans up by deleting the storage files for that client.
        """
        self._eofs_by_client.pop(client_id)
        top = []
        storage = None
        if client_id in self._buffers_by_client:
            self._flush_buffer(client_id)
            self._buffers_by_client.pop(client_id)
            storage = self._storages_by_client.pop(client_id)
            top = self._top_words(storage)
        joiner = GroupSender(MOM_HOST, JOINER_GROUP, JOINER_AMOUNT)
        try:
            joiner.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q4, top))
            joiner.send(message_protocol.internal.serialize_eof(client_id, ID))
        finally:
            try:
                joiner.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)
        if storage is not None:
            storage.delete()
        logging.info(f"Client {client_id} finished: sent top {len(top)}")

    def _process_message(self, message, ack, nack):
        """
        Processes incoming messages by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_data(client_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_eof(client_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type {msg_type}")
        except (ValueError, TypeError, AttributeError) as e:
            logging.error(f"Discarding malformed message: {e!r}")
        ack()

    def start(self):
        try:
            self._input.start_consuming(self._process_message)
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting q4 aggregator service...")
    q4_aggregator = Q4Aggregator()
    q4_aggregator.start()
    return 0

if __name__ == "__main__":
    main()
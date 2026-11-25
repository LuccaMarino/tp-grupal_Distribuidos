import logging
import os
import signal
import shutil

from common import message_protocol
from common.bucket_storage import BucketStorage
from common.heartbeat import HeartbeatResponder
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
from common.state_storage import ClientStateStorage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
JOINER_GROUP = os.environ["JOINER_GROUP"]
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
WORD_SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]
BUCKET_AMOUNT = int(os.environ["BUCKET_AMOUNT"])

# Quantity of words to accumulate in memory before flushing to disk. 
# This is a tradeoff between memory usage and disk I/O.
FLUSH_WORDS = int(os.environ["FLUSH_WORDS"])

# Maximum number of messages processed without ack. A checkpoint is made 
# when it is reached, and it is also used as the prefetch count, so 
# RabbitMQ never stops delivering before a checkpoint.
CHECKPOINT_MESSAGES = int(os.environ["CHECKPOINT_MESSAGES"])

JOINER_AMOUNT = 1
TOP_SIZE = 20

EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT

RESULT_SEND = "QUERY_RESULT"
EOF_SEND = "EOF"

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

def _new_client_state():
    """
    Returns the initial persisted state of a client. The generations of its buckets are None until the first checkpoint, which means that no bucket has a file yet.
    """
    return {"generations": None}

class Q4Aggregator:
    """
    Aggregator for the Q4 query. It receives messages from the word sum nodes, accumulates the totals in memory, and flushes them to disk when the buffer reaches a certain size. When it receives all EOFs for a client, it computes the partial top 20 words and sends them to the joiner.
    Messages are acked only after a checkpoint, which flushes the buffers to disk and persists the state of each client (the bucket generations, the processed message ids and the received EOFs) in a single atomic write. If the node crashes, RabbitMQ redelivers the messages without ack, and the ones already included in a checkpoint are discarded as duplicates.
    """
    def __init__(self):
        """
        Initializes the Q4Aggregator by setting up the input group member, the sender to the joiner, buffers, storages, and the persisted state of each client. It recovers the clients stored on disk, resends their pending results and sets up a signal handler for SIGTERM to allow graceful shutdown.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID, prefetch_count=CHECKPOINT_MESSAGES)
        self._joiner = GroupSender(MOM_HOST, JOINER_GROUP, JOINER_AMOUNT)
        self._buffers_by_client = {}   # client_id -> {word: [total]} with the totals accumulated in memory
        self._storages_by_client = {}  # client_id -> BucketStorage with the totals stored on disk
        self._dirty_clients = set()    # client_id of the clients with changes since the last checkpoint
        self._pending_acks = []        # ack functions of the messages processed since the last checkpoint
        self._store = ClientStateStorage(STORAGE_PATH, _new_client_state)
        self._store.load(on_recover=self._on_recover, send_fn=self._dispatch_send)
        self._remove_completed_clients()
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer thread. This allows the aggregator to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group member and the joiner sender, handling any exceptions that may occur during the close operation. This is called when the aggregator is shutting down to ensure that resources are released properly.
        """
        for connection in (self._input, self._joiner):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _on_recover(self, client_id, state, eofs, pending_sends):
        """
        Restores the bucket storage of a client recovered from disk, using the bucket generations of its last checkpoint.
        """
        logging.info(f"Recovered client {client_id}: {len(eofs)} EOFs received, {len(pending_sends)} pending sends")
        self._storages_by_client[client_id] = BucketStorage(STORAGE_PATH, client_id, BUCKET_AMOUNT, state["generations"])

    def _remove_completed_clients(self):
        """
        Deletes the bucket storages of the clients that are already completed: the ones finished while resending pending results on recovery, and the directories left on disk by a crash right after a client was completed.
        """
        for client_id in list(self._storages_by_client):
            if self._store.is_completed(client_id):
                self._storages_by_client.pop(client_id).delete()
        for filename in os.listdir(STORAGE_PATH):
            path = os.path.join(STORAGE_PATH, filename)
            if os.path.isdir(path) and self._store.is_completed(filename):
                shutil.rmtree(path, ignore_errors=True)

    def _dispatch_send(self, client_id, item):
        """
        Sends a pending item to the joiner: the partial top 20 words or the EOF. Returns True when the EOF is sent, which tells the state storage that the client is completed.
        """
        if item["type"] == RESULT_SEND:
            self._joiner.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q4, item["records"], message_id=item["message_id"]))
            return False
        self._joiner.send(message_protocol.internal.serialize_eof(client_id, ID, message_id=item["message_id"]))
        return True

    def _get_storage(self, client_id):
        """
        Returns the bucket storage of the specified client, creating it if the client has none yet.
        """
        if client_id not in self._storages_by_client:
            self._storages_by_client[client_id] = BucketStorage(STORAGE_PATH, client_id, BUCKET_AMOUNT)
        return self._storages_by_client[client_id]

    def _process_data(self, client_id, message_id, records, ack):
        """
        Processes incoming data messages by accumulating word totals in the buffer for the specified client. Duplicated messages and messages of completed clients are acked and discarded. The ack of the other messages is delayed until the next checkpoint, which is made when the buffer reaches FLUSH_WORDS words or CHECKPOINT_MESSAGES messages are waiting for ack.
        """
        if self._store.is_completed(client_id) or self._store.is_duplicate_msg(client_id, message_id):
            ack()
            return
        self._store.mark_msg_seen(client_id, message_id)
        if client_id not in self._buffers_by_client:
            self._buffers_by_client[client_id] = {}
        buffer = self._buffers_by_client[client_id]
        for word, total in records:
            if word not in buffer:
                buffer[word] = [0]
            buffer[word][0] += total
        self._dirty_clients.add(client_id)
        self._pending_acks.append(ack)
        if len(buffer) >= FLUSH_WORDS or len(self._pending_acks) >= CHECKPOINT_MESSAGES:
            self._checkpoint()

    def _process_eof(self, client_id, message_id, sender_id, ack):
        """
        Processes incoming EOF messages by tracking the sender IDs for each client. Every new EOF triggers a checkpoint, so that the received EOFs are persisted. If all expected EOFs have been received for a client, it computes the top 20 words and sends them to the joiner.
        """
        if self._store.is_completed(client_id) or self._store.is_duplicate_eof(client_id, sender_id):
            ack()
            return
        eof_count = self._store.add_eof(client_id, message_id, sender_id)
        self._dirty_clients.add(client_id)
        self._pending_acks.append(ack)
        if eof_count == EXPECTED_EOFS:
            self._flush_client(client_id)
        else:
            self._checkpoint()

    def _flush_buffer(self, client_id):
        """
        Flushes the buffer for the specified client by merging the accumulated word totals in memory with the totals stored on disk, and records the new bucket generations in the client's state. After merging, it clears the buffer to free up memory.
        """
        storage = self._get_storage(client_id)
        buffer = self._buffers_by_client.pop(client_id, {})
        if len(buffer) > 0:
            storage.merge(buffer, _add_totals)
        self._store.get_state(client_id)["generations"] = storage.generations()

    def _checkpoint(self):
        """
        Flushes the buffers of the clients with changes, persists their state, deletes the replaced bucket files and acks all the messages processed since the last checkpoint.
        """
        for client_id in self._dirty_clients:
            self._flush_buffer(client_id)
            self._store.persist(client_id)
            self._storages_by_client[client_id].remove_stale()
        self._dirty_clients = set()
        for ack in self._pending_acks:
            ack()
        self._pending_acks = []

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
        Flushes the remaining buffer for the specified client, computes the top 20 words from the stored data and enqueues them along with an EOF message as pending sends. After a checkpoint, it sends them to the joiner and deletes the storage files for that client.
        """
        self._flush_buffer(client_id)
        top = self._top_words(self._storages_by_client[client_id])
        self._store.enqueue_sends(client_id, [
            {"type": RESULT_SEND, "message_id": f"q4_agg_{ID}.0", "records": top},
            {"type": EOF_SEND, "message_id": f"q4_agg_{ID}.eof"},
        ])
        self._checkpoint()
        self._store.flush_pending(client_id, self._dispatch_send)
        self._storages_by_client.pop(client_id).delete()
        logging.info(f"Client {client_id} finished: sent top {len(top)}")

    def _process_message(self, message, ack, nack):
        """
        Processes incoming messages by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method, which is responsible for acking the message.
        """
        try:
            msg_type, client_id, message_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_data(client_id, message_id, *args, ack)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_eof(client_id, message_id, *args, ack)
            else:
                logging.error(f"Discarding message with unexpected type {msg_type}")
                ack()
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
    heartbeat_responder = HeartbeatResponder()
    heartbeat_responder.start()
    q4_aggregator = Q4Aggregator()
    try:
        q4_aggregator.start()
    finally:
        heartbeat_responder.stop()
    return 0

if __name__ == "__main__":
    main()
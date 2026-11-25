import logging
import os
import shutil
import signal

from common import message_protocol
from common.bucket_storage import BucketStorage
from common.heartbeat import HeartbeatResponder
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
from common.state_storage import ClientStateStorage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
WORD_SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]
BUCKET_AMOUNT = int(os.environ["BUCKET_AMOUNT"])

# Quantity of words to accumulate in memory before flushing to disk. 
# This is a tradeoff between memory usage and disk I/O.
FLUSH_WORDS = int(os.environ["FLUSH_WORDS"])
# Maximum number of messages processed without ack. A checkpoint is made 
# when it is reached, and it is also used as the prefetch count, so that 
# RabbitMQ never stops delivering before a checkpoint. The shared words 
# found are sent at every checkpoint.
CHECKPOINT_MESSAGES = int(os.environ["CHECKPOINT_MESSAGES"])

SHARED_BATCH_SIZE = 100
EN_INDEX = 0
FR_INDEX = 1

EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT

RESULT_SEND = "QUERY_RESULT"
EOF_SEND = "EOF"

def _combine_languages(stored_languages, new_languages):
    """
    Combines the language flags stored on disk with the new language flags from the buffer. The result is a list of two binary flags (0 or 1) indicating whether the word has appeared in any abstract of English and French, respectively. Each flag is set to 1 if the word has appeared in that language at least once, either in the stored data or in the new data.
    """
    return [max(stored_languages[EN_INDEX], new_languages[EN_INDEX]),
            max(stored_languages[FR_INDEX], new_languages[FR_INDEX])]

def _is_shared(languages):
    """
    Determines whether a word is shared between English and French by checking if both language flags are set to 1. A word is considered shared if it has appeared in at least one abstract of both languages.
    """
    return languages[EN_INDEX] == 1 and languages[FR_INDEX] == 1

def _new_client_state():
    """
    Returns the initial persisted state of a client. The generations of its buckets are None until the first checkpoint, which means that no bucket has a file yet, and out_seq is the number of the next batch of shared words to send.
    """
    return {"generations": None, "out_seq": 0}

class Q3Aggregator:
    """
    The Q3Aggregator class is responsible for consuming messages from the input group, processing the data to extract shared words between English and French, and sending the results to the results group. It maintains buffers and storage for each client to track the language flags of words.
    Messages are acked only after a checkpoint, which flushes the buffers to disk, enqueues the new shared words as pending sends and persists the state of each client (the bucket generations, the processed message ids, the received EOFs, the pending sends and the batch counter) in a single atomic write. The pending sends are sent after the checkpoint, so a crash can only repeat a batch with the same message_id, never lose it or send it with a different one.
    """
    def __init__(self):
        """
        Initializes the Q3Aggregator by setting up the input group member, the results group sender, buffers, storages, and the persisted state of each client. It recovers the clients stored on disk, resends their pending batches and sets up a signal handler for SIGTERM to allow graceful shutdown.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID, prefetch_count=CHECKPOINT_MESSAGES)
        self._results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._buffers_by_client = {}   # client_id -> {word -> [en_flag, fr_flag]} with the flags accumulated in memory
        self._storages_by_client = {}  # client_id -> BucketStorage with the language flags stored on disk
        self._dirty_clients = set()    # client_id of the clients with changes since the last checkpoint
        self._pending_acks = []        # ack functions of the messages processed since the last checkpoint
        self._store = ClientStateStorage(STORAGE_PATH, _new_client_state)
        self._store.load(on_recover=self._on_recover, send_fn=self._dispatch_send)
        self._remove_completed_clients()
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input group member from consuming messages.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the resources used by the Q3Aggregator instance, including the input group member and the results group sender.
        """
        for connection in (self._input, self._results):
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
        Deletes the bucket storages of the clients that are already completed: the ones finished while resending pending batches on recovery, and the directories left on disk by a crash right after a client was completed.
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
        Sends a pending item to the results group: a batch of shared words or the EOF. Returns True when the EOF is sent, which tells the state storage that the client is completed.
        """
        if item["type"] == RESULT_SEND:
            self._results.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q3, item["records"], message_id=item["message_id"]))
            return False
        self._results.send(message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q3, ID, message_id=item["message_id"]))
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
        Marks in the buffer which languages each word appeared in. Each record is a list containing the word and two binary flags indicating whether the word appeared in any abstract of English and French, respectively. Duplicated messages and messages of completed clients are acked and discarded. The ack of the other messages is delayed until the next checkpoint, which is made when the buffer reaches FLUSH_WORDS words or CHECKPOINT_MESSAGES messages are waiting for ack.
        """
        if self._store.is_completed(client_id) or self._store.is_duplicate_msg(client_id, message_id):
            ack()
            return
        self._store.mark_msg_seen(client_id, message_id)
        if client_id not in self._buffers_by_client:
            self._buffers_by_client[client_id] = {}
        buffer = self._buffers_by_client[client_id]
        for word, in_en, in_fr in records:
            if word not in buffer:
                buffer[word] = [0, 0]
            if in_en:
                buffer[word][EN_INDEX] = 1
            if in_fr:
                buffer[word][FR_INDEX] = 1
        self._dirty_clients.add(client_id)
        self._pending_acks.append(ack)
        if len(buffer) >= FLUSH_WORDS or len(self._pending_acks) >= CHECKPOINT_MESSAGES:
            self._checkpoint()

    def _process_eof(self, client_id, message_id, sender_id, ack):
        """
        Processes the EOF message received from a word sum and tracks the sender_id of the EOF. Every new EOF triggers a checkpoint, so that the received EOFs are persisted. If all expected EOFs for the client have been received, it sends the remaining shared words and the EOF to the results group.
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
        Flushes the buffer for a given client by merging it with the stored language flags on disk. It identifies the words that have just become shared between English and French and enqueues them as pending sends. It also records the new bucket generations in the client's state. After merging, the buffer is removed to free up memory.
        """
        storage = self._get_storage(client_id)
        buffer = self._buffers_by_client.pop(client_id, {})
        if len(buffer) > 0:
            changes = storage.merge(buffer, _combine_languages)
            new_shared_words = []
            for word, before, after in changes:
                was_shared = before is not None and _is_shared(before)
                if _is_shared(after) and not was_shared:
                    new_shared_words.append([word])
            self._enqueue_shared_words(client_id, new_shared_words)
        self._store.get_state(client_id)["generations"] = storage.generations()

    # El número de lote se guarda en el estado del cliente junto con los envíos pendientes, así
    # cada lote tiene un message_id distinto y un lote reenviado después de una caída conserva el suyo
    def _enqueue_shared_words(self, client_id, shared_words):
        """
        Enqueues the shared words as pending sends in batches of SHARED_BATCH_SIZE. Each batch gets the message_id q3_agg_{ID}.{out_seq}, where out_seq is a counter stored in the client's state that is incremented for every batch.
        """
        state = self._store.get_state(client_id)
        batches = []
        for start in range(0, len(shared_words), SHARED_BATCH_SIZE):
            batches.append({
                "type": RESULT_SEND,
                "message_id": f"q3_agg_{ID}.{state['out_seq']}",
                "records": shared_words[start:start + SHARED_BATCH_SIZE],
            })
            state["out_seq"] += 1
        self._store.enqueue_sends(client_id, batches)
        
    def _checkpoint(self):
        """
        Flushes the buffers of the clients with changes, persists their state and deletes the replaced bucket files. Then it sends the pending batches of those clients and acks all the messages processed since the last checkpoint.
        """
        for client_id in self._dirty_clients:
            self._flush_buffer(client_id)
            self._store.persist(client_id)
            self._storages_by_client[client_id].remove_stale()
        for client_id in self._dirty_clients:
            # se flushean los pending aca porque q3 tiene lotes que enviar en todos los checkpoints
            self._store.flush_pending(client_id, self._dispatch_send)
        self._dirty_clients = set()
        for ack in self._pending_acks:
            ack()
        self._pending_acks = []
    
    def _flush_client(self, client_id):
        """
        Flushes the remaining data for a client by flushing the buffer, sending any remaining shared words to the results group, and deleting the stored language flags on disk. It also sends an EOF message to the results group indicating that the Q3Aggregator has finished processing for the client.
        """
        self._eofs_by_client.pop(client_id)
        storage = None
        if client_id in self._buffers_by_client:
            self._flush_buffer(client_id)
            self._buffers_by_client.pop(client_id)
            self._messages_since_flush_by_client.pop(client_id)
            storage = self._storages_by_client.pop(client_id)
        shared_amount = self._shared_amount_by_client.pop(client_id, 0)
        self._results.send(message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q3, ID, message_id=f"q3_agg_{ID}.eof"))
        if storage is not None:
            storage.delete()
        logging.info(f"Client {client_id} finished: sent {shared_amount} shared words")

    def _process_message(self, message, ack, nack):
        """
        Processes a message received from the input group. It deserializes the message, determines its type (DATA or EOF), and calls the appropriate processing method.
        """
        try:
            msg_type, client_id, _message_id, args = message_protocol.internal.deserialize(message)
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
    logging.info("Starting q3 aggregator service...")
    heartbeat_responder = HeartbeatResponder()
    heartbeat_responder.start()
    q3_aggregator = Q3Aggregator()
    try:
        q3_aggregator.start()
    finally:
        heartbeat_responder.stop()
    return 0


if __name__ == "__main__":
    main()
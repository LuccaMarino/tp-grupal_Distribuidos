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
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
WORD_SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]
BUCKET_AMOUNT = int(os.environ["BUCKET_AMOUNT"])

# Buffer is flushed to disk when it has FLUSH_WORDS distinct words or when FLUSH_MESSAGES messages have arrived since the last flush, whichever comes first. Flushing every so many messages ensures that shared words are sent frequently and the connection with the gateway does not remain idle, preventing disconnection due to heartbeat.
FLUSH_WORDS = int(os.environ["FLUSH_WORDS"])
FLUSH_MESSAGES = int(os.environ["FLUSH_MESSAGES"])

SHARED_BATCH_SIZE = 100
EN_INDEX = 0
FR_INDEX = 1

EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT

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

class Q3Aggregator:
    """
    The Q3Aggregator class is responsible for consuming messages from the input group, processing the data to extract shared words between English and French, and sending the results to the results group. It maintains buffers and storage for each client to track the language flags of words and determine when to flush the data to disk and send shared words to the results group.
    """
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._buffers_by_client = {}                # client_id -> {word -> [en_flag, fr_flag]} with the flags indicating appearances
        self._storages_by_client = {}               # client_id -> BucketStorage with the language flags stored on disk
        self._messages_since_flush_by_client = {}   # client_id -> messages since last flush
        self._shared_amount_by_client = {}          # client_id -> total shared words sent to the gateway
        self._eofs_by_client = {}                   # client_id -> set with the sender_id of the received EOFs
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

    def _process_data(self, client_id, records):
        """
        Marks in the buffer which languages each word appeared in. Each record is a list containing the word and two binary flags indicating whether the word appeared in any abstract of English and French, respectively. If the buffer reaches FLUSH_WORDS distinct words or FLUSH_MESSAGES messages have arrived since the last flush, it is flushed to disk.
        """
        if client_id not in self._buffers_by_client:
            self._buffers_by_client[client_id] = {}
            self._storages_by_client[client_id] = BucketStorage(STORAGE_PATH, client_id, BUCKET_AMOUNT)
            self._messages_since_flush_by_client[client_id] = 0
            self._shared_amount_by_client[client_id] = 0
        buffer = self._buffers_by_client[client_id]
        for word, in_en, in_fr in records:
            if word not in buffer:
                buffer[word] = [0, 0]
            if in_en:
                buffer[word][EN_INDEX] = 1
            if in_fr:
                buffer[word][FR_INDEX] = 1
        self._messages_since_flush_by_client[client_id] += 1
        if len(buffer) >= FLUSH_WORDS or self._messages_since_flush_by_client[client_id] >= FLUSH_MESSAGES:
            self._flush_buffer(client_id)

    def _flush_buffer(self, client_id):
        """
        Flushes the buffer for a given client by merging it with the stored language flags on disk. It identifies new shared words that have appeared in both English and French and sends them to the results group. The buffer is then cleared, and the message count since the last flush is reset.
        """
        self._messages_since_flush_by_client[client_id] = 0
        buffer = self._buffers_by_client[client_id]
        if len(buffer) == 0:
            return
        changes = self._storages_by_client[client_id].merge(buffer, _combine_languages)
        self._buffers_by_client[client_id] = {}
        new_shared_words = []
        for word, before, after in changes:
            was_shared = before is not None and _is_shared(before)
            if _is_shared(after) and not was_shared:
                new_shared_words.append([word])
        self._send_shared_words(client_id, new_shared_words)

    def _send_shared_words(self, client_id, shared_words):
        """
        Sends the shared words to the results group in batches of SHARED_BATCH_SIZE. Each batch is serialized as a query result message with the client_id and the query type (Q3). The total number of shared words sent for the client is updated accordingly.
        """
        for start in range(0, len(shared_words), SHARED_BATCH_SIZE):
            batch = shared_words[start:start + SHARED_BATCH_SIZE]
            self._results.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q3, batch))
        self._shared_amount_by_client[client_id] += len(shared_words)

    def _process_eof(self, client_id, sender_id):
        """
        Processes the EOF message received from a parser and tracks the sender_id of the EOF. If all expected EOFs for the client have been received, it flushes the client's buffer and sends any remaining shared words to the results group.
        """
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == EXPECTED_EOFS:
            self._flush_client(client_id)

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
        self._results.send(message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q3, ID))
        if storage is not None:
            storage.delete()
        logging.info(f"Client {client_id} finished: sent {shared_amount} shared words")

    def _process_message(self, message, ack, nack):
        """
        Processes a message received from the input group. It deserializes the message, determines its type (DATA or EOF), and calls the appropriate processing method.
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
    logging.info("Starting q3 aggregator service...")
    q3_aggregator = Q3Aggregator()
    q3_aggregator.start()
    return 0


if __name__ == "__main__":
    main()
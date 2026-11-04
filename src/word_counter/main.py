import logging
import os
import signal

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
SUM_PARTIAL_LENGTH_GROUP = os.environ["SUM_PARTIAL_LENGTH_GROUP"]
Q5_JOINER_AMOUNT = int(os.environ["Q5_JOINER_AMOUNT"])
EXPECTED_EOFS = int(os.environ["EXPECTED_EOFS"])

class WordCounter:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._sum_partial_length = GroupSender(MOM_HOST, SUM_PARTIAL_LENGTH_GROUP, Q5_JOINER_AMOUNT)
        self._stats = {} # client_id -> [total_words, total_articles]
        self._received_eofs = {} # client_id -> set of sender_ids that sent EOF
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        for connection in (self._input, self._sum_partial_length):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _process_data(self, client_id, records):
        if client_id not in self._stats:
            self._stats[client_id] = [0, 0]
        self._stats[client_id][0] += sum(records)
        self._stats[client_id][1] += len(records)

    def _process_eof(self, client_id, sender_id):
        logging.info(f"Received EOF from sender {sender_id} for client {client_id}")
        if client_id not in self._received_eofs:
            self._received_eofs[client_id] = set()
        self._received_eofs[client_id].add(sender_id)
        if len(self._received_eofs[client_id]) == EXPECTED_EOFS:
            total_words, total_articles = self._stats.get(client_id, [0, 0])
            data_message = message_protocol.internal.serialize_data(client_id, [total_words, total_articles])
            self._sum_partial_length.send(data_message)
            eof_message = message_protocol.internal.serialize_eof(client_id, f"word_counter_{ID}")
            self._sum_partial_length.broadcast(eof_message)
            self._stats.pop(client_id, None)
            self._received_eofs.pop(client_id, None)

    def _process_message(self, message, ack, nack):
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_data(client_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_eof(client_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type: {msg_type}")
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
    logging.info("Starting word counter service...")
    word_counter = WordCounter()
    word_counter.start()
    return 0

if __name__ == "__main__":
    main()
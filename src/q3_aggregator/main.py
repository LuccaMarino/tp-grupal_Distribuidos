import logging
import os
import signal

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
WORD_SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])

# Cantidad máxima de palabras compartidas por mensaje al gateway
SHARED_BATCH_SIZE = 50

# Posición de cada idioma en los flags [en, fr] que mandan los word_sum
EN_INDEX = 0
FR_INDEX = 1

# Cada word_sum reenvía el EOF de cada parser, así que llega uno por
# cada par parser-word_sum
EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT


class Q3Aggregator:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._languages_by_client = {}      # client_id -> {palabra: [visto_en, visto_fr]}
        self._shared_amount_by_client = {}  # client_id -> cantidad de palabras compartidas ya enviadas
        self._eofs_by_client = {}           # client_id -> set con los sender_id de los EOF recibidos
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        for connection in (self._input, self._results):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    # Marca en qué idiomas apareció cada palabra. Cada registro es [palabra, en, fr].
    # Las palabras que pasan a estar en los dos idiomas se envían al gateway
    def _process_data(self, client_id, records):
        if client_id not in self._languages_by_client:
            self._languages_by_client[client_id] = {}
            self._shared_amount_by_client[client_id] = 0
        languages_by_word = self._languages_by_client[client_id]
        new_shared_words = []
        for word, in_en, in_fr in records:
            if word not in languages_by_word:
                languages_by_word[word] = [0, 0]
            languages = languages_by_word[word]
            was_shared = languages[EN_INDEX] and languages[FR_INDEX]
            if in_en:
                languages[EN_INDEX] = 1
            if in_fr:
                languages[FR_INDEX] = 1
            is_shared = languages[EN_INDEX] and languages[FR_INDEX]
            if is_shared and not was_shared:
                new_shared_words.append([word])
        self._send_shared_words(client_id, new_shared_words)

    # Envía las palabras compartidas al gateway en batches
    def _send_shared_words(self, client_id, shared_words):
        for start in range(0, len(shared_words), SHARED_BATCH_SIZE):
            batch = shared_words[start:start + SHARED_BATCH_SIZE]
            self._results.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q3, batch))
        self._shared_amount_by_client[client_id] += len(shared_words)

    # Se guardan los sender_id por cada EOF en un set, así no se cuentan EOFs repetidos.
    # Si llegaron todos los EOFs esperados para este cliente, se flushea el cliente
    def _process_eof(self, client_id, sender_id):
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == EXPECTED_EOFS:
            self._flush_client(client_id)

    # Las palabras compartidas ya se enviaron mientras se procesaban los datos,
    # así que solo se avisa al gateway que este aggregator terminó Q3
    def _flush_client(self, client_id):
        self._languages_by_client.pop(client_id, None)
        shared_amount = self._shared_amount_by_client.pop(client_id, 0)
        self._eofs_by_client.pop(client_id)
        self._results.send(message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q3, ID))
        logging.info(f"Client {client_id} finished: sent {shared_amount} shared words")

    def _process_message(self, message, ack, nack):
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
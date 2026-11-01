import logging
import os
import signal

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
JOINER_GROUP = os.environ["JOINER_GROUP"]
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
WORD_SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])

JOINER_AMOUNT = 1
TOP_SIZE = 20

# Cantidad máxima de palabras compartidas (Q3) por mensaje
SHARED_BATCH_SIZE = 1000

# Cada word_sum reenvía el EOF de cada parser, así que llega uno por 
# cada par parser-word_sum
EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT

# Posiciones del conteo [total, n_en, n_fr] que mandan los word_sums
TOTAL_INDEX = 0
EN_INDEX = 1
FR_INDEX = 2


# Orden del top: mayor total primero y, si empatan, alfabético
def _top_order(word_counts):
    word, counts = word_counts
    return (-counts[TOTAL_INDEX], word)


class WordAggregator:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._joiner = GroupSender(MOM_HOST, JOINER_GROUP, JOINER_AMOUNT)
        self._counts_by_client = {} # client_id -> {palabra: [total, n_en, n_fr]}
        self._eofs_by_client = {}   # client_id -> set con los sender_id de los EOF recibidos
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        for connection in (self._input, self._joiner):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    # Actualiza los conteos. Cada registro es [palabra, total, n_en, n_fr]
    def _process_data(self, client_id, records):
        if client_id not in self._counts_by_client:
            self._counts_by_client[client_id] = {}
        counts_by_word = self._counts_by_client[client_id]
        for word, total, n_en, n_fr in records:
            if word not in counts_by_word:
                counts_by_word[word] = [0, 0, 0]
            counts = counts_by_word[word]
            counts[TOTAL_INDEX] += total
            counts[EN_INDEX] += n_en
            counts[FR_INDEX] += n_fr

    # Se guardan los sender_id por cada EOF en un set, así no se cuentan EOFs repetidos.
    # Si llegaron todos los EOFs esperados para este cliente, se flushea el cliente
    def _process_eof(self, client_id, sender_id):
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == EXPECTED_EOFS:
            self._flush_client(client_id)

    # Arma el top 20 local
    def _top_words(self, counts_by_word):
        sorted_words = sorted(counts_by_word.items(), key=_top_order)
        top = []
        for word, counts in sorted_words[:TOP_SIZE]:
            top.append([word, counts[TOTAL_INDEX]])
        return top

    # Envía al joiner las palabras que aparecen en algún abstract en inglés y en
    # alguno en francés, en mensajes de a SHARED_BATCH_SIZE palabras
    def _send_shared_words(self, client_id, counts_by_word):
        batch = []
        sent_amount = 0
        for word, counts in counts_by_word.items():
            if counts[EN_INDEX] > 0 and counts[FR_INDEX] > 0:
                batch.append(word)
                if len(batch) == SHARED_BATCH_SIZE:
                    self._send_shared_batch(client_id, batch)
                    sent_amount += len(batch)
                    batch = []
        if batch:   # si el ultimo batch es incompleto, se manda igual
            self._send_shared_batch(client_id, batch)
            sent_amount += len(batch)
        return sent_amount

    def _send_shared_batch(self, client_id, batch):
        self._joiner.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q3, batch))

    # Envía los resultados parciales del cliente al joiner y libera su estado
    def _flush_client(self, client_id):
        counts_by_word = self._counts_by_client.pop(client_id, {})
        self._eofs_by_client.pop(client_id)
        top = self._top_words(counts_by_word)
        shared_amount = self._send_shared_words(client_id, counts_by_word)
        self._joiner.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q4, top))
        self._joiner.send(message_protocol.internal.serialize_eof(client_id, ID))
        logging.info(f"Client {client_id} finished: sent top {len(top)} and {shared_amount} shared words")

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
    logging.info("Starting word aggregator service...")
    word_aggregator = WordAggregator()
    word_aggregator.start()
    return 0


if __name__ == "__main__":
    main()
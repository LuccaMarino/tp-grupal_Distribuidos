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

# Cada word_sum reenvía el EOF de cada parser, así que llega uno por 
# cada par parser-word_sum
EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT


# Orden del top: mayor total primero y, si empatan, alfabético
def _top_order(word_total):
    word, total = word_total
    return (-total, word)


class Q4Aggregator:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._totals_by_client = {} # client_id -> {palabra: total}
        self._eofs_by_client = {}   # client_id -> set con los sender_id de los EOF recibidos
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        try:
            self._input.close()
        except MessageMiddlewareCloseError as e:
            logging.error(e)

    # Suma los totales. Cada registro es [palabra, total]
    def _process_data(self, client_id, records):
        if client_id not in self._totals_by_client:
            self._totals_by_client[client_id] = {}
        totals_by_word = self._totals_by_client[client_id]
        for word, total in records:
            if word not in totals_by_word:
                totals_by_word[word] = 0
            totals_by_word[word] += total

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
    def _top_words(self, totals_by_word):
        sorted_words = sorted(totals_by_word.items(), key=_top_order)
        top = []
        for word, total in sorted_words[:TOP_SIZE]:
            top.append([word, total])
        return top

    # Envía el top 20 local y el EOF al joiner, y libera el estado del cliente.
    # La conexion de salida se abre recién aca y se cierra al terminar el flush, ya que
    # este nodo solo puede enviar cuando tiene todos los datos. Si se abre desde el
    # inicio quedaría ociosa y el broker podría cerrarla por heartbeat
    def _flush_client(self, client_id):
        totals_by_word = self._totals_by_client.pop(client_id, {})
        self._eofs_by_client.pop(client_id)
        top = self._top_words(totals_by_word)
        joiner = GroupSender(MOM_HOST, JOINER_GROUP, JOINER_AMOUNT)
        try:
            joiner.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q4, top))
            joiner.send(message_protocol.internal.serialize_eof(client_id, ID))
        finally:
            try:
                joiner.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)
        logging.info(f"Client {client_id} finished: sent top {len(top)}")

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
    logging.info("Starting q4 aggregator service...")
    q4_aggregator = Q4Aggregator()
    q4_aggregator.start()
    return 0


if __name__ == "__main__":
    main()
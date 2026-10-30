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
AGGREGATOR_AMOUNT = int(os.environ["AGGREGATOR_AMOUNT"])

TOP_SIZE = 20

# Posiciones de cada registro [palabra, total] del top que mandan los aggregators (Q4)
WORD_INDEX = 0
COUNT_INDEX = 1


# Mismo orden que en los aggregators: mayor total primero y, si empatan, alfabético
def _top_order(word_count):
    return (-word_count[COUNT_INDEX], word_count[WORD_INDEX])


class WordJoiner:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._tops_by_client = {}    # client_id -> lista de [palabra, total] de los tops locales
        self._shared_by_client = {}  # client_id -> lista de palabras compartidas
        self._eofs_by_client = {}    # client_id -> set con los sender_id de los EOF recibidos
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

    # Guarda el resultado parcial de un aggregator según la query que corresponde
    def _process_query_result(self, client_id, query, records):
        if query == message_protocol.internal.Query.Q4:
            if client_id not in self._tops_by_client:
                self._tops_by_client[client_id] = []
            self._tops_by_client[client_id].extend(records)
        elif query == message_protocol.internal.Query.Q3:
            if client_id not in self._shared_by_client:
                self._shared_by_client[client_id] = []
            self._shared_by_client[client_id].extend(records)
        else:
            logging.error(f"Discarding result of unexpected query {query}")

    # Guarda los sender_id por cada EOF recibido. Si llegaron de todos 
    # los aggregators para este cliente, se lo flushea
    def _process_eof(self, client_id, sender_id):
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == AGGREGATOR_AMOUNT:
            self._flush_client(client_id)
            
    # Arma el top 20 global
    def _top_words(self, candidates):
        sorted_words = sorted(candidates, key=_top_order)
        return sorted_words[:TOP_SIZE]

    # Envía los resultados finales del cliente al gateway y libera su estado
    def _flush_client(self, client_id):
        candidates = self._tops_by_client.pop(client_id, [])
        shared = sorted(self._shared_by_client.pop(client_id, []))
        self._eofs_by_client.pop(client_id)
        top = self._top_words(candidates)
        logging.info(f"Client {client_id} finished: sending top {len(top)} and {len(shared)} shared words")
        self._send_query(client_id, message_protocol.internal.Query.Q3, shared)
        self._send_query(client_id, message_protocol.internal.Query.Q4, top)

    # Envía el resultado de una query al gateway seguido de su QUERY_EOF
    def _send_query(self, client_id, query, records):
        self._results.send(message_protocol.internal.serialize_query_result(client_id, query, records))
        self._results.send(message_protocol.internal.serialize_query_eof(client_id, query, ID))

    def _process_message(self, message, ack, nack):
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.QUERY_RESULT:
                self._process_query_result(client_id, *args)
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
    logging.info("Starting word joiner service...")
    word_joiner = WordJoiner()
    word_joiner.start()
    return 0


if __name__ == "__main__":
    main()
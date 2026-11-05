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
# Cantidad de palabras distintas en memoria a partir de la cual se vuelca el buffer a disco
FLUSH_WORDS = int(os.environ["FLUSH_WORDS"])

JOINER_AMOUNT = 1
TOP_SIZE = 20

# Cada word_sum reenvía el EOF de cada parser, así que llega uno por 
# cada par parser-word_sum
EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT


# Orden del top: mayor total primero y, si empatan, alfabético
def _top_order(word_total):
    word, total = word_total
    return (-total, word)

# Combina el total guardado en disco con el del buffer. Los valores son [total]
def _add_totals(stored_values, new_values):
    return [stored_values[0] + new_values[0]]

class Q4Aggregator:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._buffers_by_client = {}   # client_id -> {palabra: [total]} con lo que todavía no se volcó a disco
        self._storages_by_client = {}  # client_id -> BucketStorage con los totales ya volcados
        self._eofs_by_client = {}      # client_id -> set con los sender_id de los EOF recibidos
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        try:
            self._input.close()
        except MessageMiddlewareCloseError as e:
            logging.error(e)

    # Suma los totales en el buffer. Cada registro es [palabra, total].
    # Si el buffer llega a FLUSH_WORDS palabras, se vuelca a disco
    def _process_data(self, client_id, records):
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

    # Suma el buffer a los totales guardados en disco y lo vacía
    def _flush_buffer(self, client_id):
        buffer = self._buffers_by_client[client_id]
        if len(buffer) == 0:
            return
        self._storages_by_client[client_id].merge(buffer, _add_totals)
        self._buffers_by_client[client_id] = {}

    # Se guardan los sender_id por cada EOF en un set, así no se cuentan EOFs repetidos.
    # Si llegaron todos los EOFs esperados para este cliente, se flushea el cliente
    def _process_eof(self, client_id, sender_id):
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == EXPECTED_EOFS:
            self._flush_client(client_id)

    # Arma el top 20 local recorriendo los buckets de a uno. A los candidatos de cada
    # bucket se les suma el top acumulado y se queda con los 20 mejores
    def _top_words(self, storage):
        top = []
        for bucket_index in range(storage.bucket_amount()):
            candidates = top
            for word, values in storage.read_bucket(bucket_index).items():
                candidates.append([word, values[0]])
            sorted_candidates = sorted(candidates, key=_top_order)
            top = sorted_candidates[:TOP_SIZE]
        return top

    # Vuelca lo que queda en el buffer, arma el top 20 local a partir de los buckets y
    # lo envía al joiner junto con el EOF. Recién después de enviarlo borra los archivos
    # del cliente. La conexión de salida se abre recién acá y se cierra al terminar el
    # flush, ya que este nodo solo puede enviar cuando tiene todos los datos. Si se abre
    # desde el inicio quedaría ociosa y el broker podría cerrarla por heartbeat
    def _flush_client(self, client_id):
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
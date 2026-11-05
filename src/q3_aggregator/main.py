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
# El buffer se vuelca a disco cuando tiene FLUSH_WORDS palabras distintas o cuando
# llegaron FLUSH_MESSAGES mensajes desde el último volcado, lo que pase primero.
# Volcar cada tantos mensajes hace que las palabras compartidas se envíen seguido
# y la conexión con el gateway no quede ociosa, evitando desconexion por heartbeat
FLUSH_WORDS = int(os.environ["FLUSH_WORDS"])
FLUSH_MESSAGES = int(os.environ["FLUSH_MESSAGES"])

# Cantidad máxima de palabras compartidas por mensaje al gateway
SHARED_BATCH_SIZE = 50

# Posición de cada idioma en los flags [en, fr] que mandan los word_sum
EN_INDEX = 0
FR_INDEX = 1

# Cada word_sum reenvía el EOF de cada parser, así que llega uno por
# cada par parser-word_sum
EXPECTED_EOFS = PARSER_AMOUNT * WORD_SUM_AMOUNT

# Combina los flags guardados en disco con los del buffer: queda 1 en cada idioma
# en el que la palabra apareció alguna vez
def _combine_languages(stored_languages, new_languages):
    return [max(stored_languages[EN_INDEX], new_languages[EN_INDEX]),
            max(stored_languages[FR_INDEX], new_languages[FR_INDEX])]


def _is_shared(languages):
    return languages[EN_INDEX] == 1 and languages[FR_INDEX] == 1


class Q3Aggregator:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._buffers_by_client = {}                # client_id -> {palabra: [visto_en, visto_fr]} con lo que todavía no se volcó a disco
        self._storages_by_client = {}               # client_id -> BucketStorage con los flags ya volcados
        self._messages_since_flush_by_client = {}   # client_id -> mensajes recibidos desde el último volcado
        self._shared_amount_by_client = {}          # client_id -> cantidad de palabras compartidas ya enviadas
        self._eofs_by_client = {}                   # client_id -> set con los sender_id de los EOF recibidos
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

    # Marca en el buffer en qué idiomas apareció cada palabra. Cada registro es [palabra, en, fr].
    # Si el buffer llega a FLUSH_WORDS palabras o pasaron FLUSH_MESSAGES mensajes, se vuelca a disco
    def _process_data(self, client_id, records):
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

    # Combina el buffer con lo guardado en disco y lo vacía. Las palabras que no eran
    # compartidas (o no estaban) y ahora sí lo son se envían al gateway. Las que ya eran
    # compartidas no se vuelven a enviar
    def _flush_buffer(self, client_id):
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

    # Vuelca lo que queda en el buffer (enviando las últimas palabras compartidas),
    # manda al gateway el eof de este aggregator y borra los archivos del cliente
    def _flush_client(self, client_id):
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
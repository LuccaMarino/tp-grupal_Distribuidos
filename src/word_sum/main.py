import logging
import os
import re
import signal

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
import stopwords

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
AGGREGATOR_GROUP = os.environ["AGGREGATOR_GROUP"]
AGGREGATOR_AMOUNT = int(os.environ["AGGREGATOR_AMOUNT"])
WORD_COUNTER_GROUP = os.environ["WORD_COUNTER_GROUP"]
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
Q5_JOINER_GROUP = os.environ["Q5_JOINER_GROUP"]


Q5_JOINER_AMOUNT = 1

# Para Q5. Hasta este año inclusive se usa para el promedio, y después se filtra
LAST_OLD_YEAR = 2020

# Posición de cada idioma en el conteo que se envía a los aggregators ([total, n_en, n_fr])
TOTAL_INDEX = 0
LANGUAGE_INDEX = {"en": 1, "fr": 2}

WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")


class WordSum:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._aggregators = GroupSender(MOM_HOST, AGGREGATOR_GROUP, AGGREGATOR_AMOUNT)
        self._word_counters = GroupSender(MOM_HOST, WORD_COUNTER_GROUP, WORD_COUNTER_AMOUNT)
        self._q5_joiner = GroupSender(MOM_HOST, Q5_JOINER_GROUP, Q5_JOINER_AMOUNT)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        for connection in (self._input, self._aggregators, self._word_counters, self._q5_joiner):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    # Palabras del abstract sin stopwords ni números, con repeticiones
    def _extract_words(self, abstract):
        words = []
        for word in WORD_REGEX.findall(abstract.lower()):
            if word not in stopwords.ALL and not NUMBER_REGEX.fullmatch(word):
                words.append(word)
        return words

    # Conteo para Q3 y Q4
    def _add_counts(self, counts_by_word, lang, words):
        language_index = LANGUAGE_INDEX.get(lang)
        for word in set(words):     # solo importa si la palabra aparece, no cuantas veces
            if word not in counts_by_word:
                counts_by_word[word] = [0, 0, 0]
            counts_by_word[word][TOTAL_INDEX] += 1
            if language_index is not None:
                counts_by_word[word][language_index] += 1

    def _process_data(self, client_id, records):
        counts_by_word = {}
        old_lengths = []
        new_articles = []
        for lang, year, url, abstract in records:
            words = self._extract_words(abstract)
            # para Q3 y Q4
            self._add_counts(counts_by_word, lang, words)
            # Para Q5, necesita el largo incluyendo repeticiones de palabras
            if year <= LAST_OLD_YEAR:
                old_lengths.append(len(words))
            else:
                new_articles.append([url, len(words)])

        def build_counts_message(member_counts):
            counts_records = []
            for word, counts in member_counts.items():
                # cada registro es [palabra, total, n_en, n_fr]
                counts_records.append([word, counts[0], counts[1], counts[2]])
            return message_protocol.internal.serialize_data(client_id, counts_records)

        self._aggregators.send_partitioned(counts_by_word, build_counts_message)
        if old_lengths:
            self._word_counters.send(message_protocol.internal.serialize_data(client_id, old_lengths))
        if new_articles:
            self._q5_joiner.send(message_protocol.internal.serialize_data(client_id, new_articles))

    # Reenvía el EOF de cada parser a todos los destinos. 
    # Se envia un sender_id que identifica al par parser-word_sum, ya que 
    # cada receptor espera un EOF por cada par
    def _process_eof(self, client_id, parser_id):
        logging.info(f"Received EOF from parser {parser_id} for client {client_id}")
        eof_message = message_protocol.internal.serialize_eof(client_id, f"{parser_id}.{ID}")
        self._aggregators.broadcast(eof_message)
        self._word_counters.broadcast(eof_message)
        self._q5_joiner.broadcast(eof_message)

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
            self._input.start_consuming(self._process_message, [self._aggregators, self._word_counters, self._q5_joiner])
        finally:
            self._close()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting word sum service...")
    word_sum = WordSum()
    word_sum.start()
    return 0


if __name__ == "__main__":
    main()
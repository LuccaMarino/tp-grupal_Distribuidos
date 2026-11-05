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
Q3_AGGREGATOR_GROUP = os.environ["Q3_AGGREGATOR_GROUP"]
Q3_AGGREGATOR_AMOUNT = int(os.environ["Q3_AGGREGATOR_AMOUNT"])
Q4_AGGREGATOR_GROUP = os.environ["Q4_AGGREGATOR_GROUP"]
Q4_AGGREGATOR_AMOUNT = int(os.environ["Q4_AGGREGATOR_AMOUNT"])
WORD_COUNTER_GROUP = os.environ["WORD_COUNTER_GROUP"]
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
Q5_JOINER_GROUP = os.environ["Q5_JOINER_GROUP"]


Q5_JOINER_AMOUNT = 1
LAST_OLD_YEAR = 2020

# Posición de cada idioma en los flags que se envían a los q3_aggregators ([en, fr])
LANGUAGE_INDEX = {"en": 0, "fr": 1}

WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")


class WordSum:
    def __init__(self):
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._q3_aggregators = GroupSender(MOM_HOST, Q3_AGGREGATOR_GROUP, Q3_AGGREGATOR_AMOUNT)
        self._q4_aggregators = GroupSender(MOM_HOST, Q4_AGGREGATOR_GROUP, Q4_AGGREGATOR_AMOUNT)
        self._word_counters = GroupSender(MOM_HOST, WORD_COUNTER_GROUP, WORD_COUNTER_AMOUNT)
        self._q5_joiner = GroupSender(MOM_HOST, Q5_JOINER_GROUP, Q5_JOINER_AMOUNT)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        for connection in (self._input, self._q3_aggregators, self._q4_aggregators, self._word_counters, self._q5_joiner):
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

    # Para Q4 cuenta en cuántos abstracts aparece cada palabra, y para Q3 marca
    # si aparece en algún abstract en inglés y/o en alguno en francés
    def _add_counts(self, totals_by_word, languages_by_word, lang, words):
        language_index = LANGUAGE_INDEX.get(lang)
        for word in set(words):     # solo importa si la palabra aparece, no cuantas veces
            if word not in totals_by_word:
                totals_by_word[word] = 0
            totals_by_word[word] += 1
            if language_index is not None:
                if word not in languages_by_word:
                    languages_by_word[word] = [0, 0]
                languages_by_word[word][language_index] = 1

    
    def _process_data(self, client_id, records):
        totals_by_word = {}
        languages_by_word = {}
        old_lengths = []
        new_articles = []
        for lang, year, url, abstract in records:
            words = self._extract_words(abstract)
            # para Q3 y Q4
            self._add_counts(totals_by_word, languages_by_word, lang, words)
            # Para Q5, necesita el largo incluyendo repeticiones de palabras
            if year <= LAST_OLD_YEAR:
                old_lengths.append(len(words))
            else:
                new_articles.append([url, len(words)])

        # cada registro es [palabra, en, fr]
        def build_languages_message(member_languages):
            languages_records = []
            for word, languages in member_languages.items():
                languages_records.append([word, languages[0], languages[1]])
            return message_protocol.internal.serialize_data(client_id, languages_records)

        # cada registro es [palabra, total]
        def build_totals_message(member_totals):
            totals_records = []
            for word, total in member_totals.items():
                totals_records.append([word, total])
            return message_protocol.internal.serialize_data(client_id, totals_records)

        self._q3_aggregators.send_partitioned(languages_by_word, build_languages_message)
        self._q4_aggregators.send_partitioned(totals_by_word, build_totals_message)
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
        self._q3_aggregators.broadcast(eof_message)
        self._q4_aggregators.broadcast(eof_message)
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
            self._input.start_consuming(self._process_message)
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
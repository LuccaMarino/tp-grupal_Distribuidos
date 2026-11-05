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

# Each language is represented by a binary flag (0 or 1) indicating whether the word appears in any abstract of that language. The index of each language in the list corresponds to its position in the LANGUAGE_INDEX dictionary.
LANGUAGE_INDEX = {"en": 0, "fr": 1}

WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")

class WordSum:
    """
    The WordSum class is responsible for consuming messages from the input group, processing the data to extract words and their counts, and sending the results to the appropriate output groups.
    """
    def __init__(self):
        """
        Initializes the WordSum instance by setting up the necessary group senders and group member for processing word sum requests.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._q3_aggregators = GroupSender(MOM_HOST, Q3_AGGREGATOR_GROUP, Q3_AGGREGATOR_AMOUNT)
        self._q4_aggregators = GroupSender(MOM_HOST, Q4_AGGREGATOR_GROUP, Q4_AGGREGATOR_AMOUNT)
        self._word_counters = GroupSender(MOM_HOST, WORD_COUNTER_GROUP, WORD_COUNTER_AMOUNT)
        self._q5_joiner = GroupSender(MOM_HOST, Q5_JOINER_GROUP, Q5_JOINER_AMOUNT)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input group member from consuming messages.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the resources used by the WordSum instance, including the input group member and the output group senders.
        """
        for connection in (self._input, self._q3_aggregators, self._q4_aggregators, self._word_counters, self._q5_joiner):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _extract_words(self, abstract):
        """
        Extracts words from the given abstract, filtering out stopwords and numbers. It returns a list of words in lowercase, preserving repetitions.
        """
        words = []
        for word in WORD_REGEX.findall(abstract.lower()):
            if word not in stopwords.ALL and not NUMBER_REGEX.fullmatch(word):
                words.append(word)
        return words

    def _add_counts(self, totals_by_word, languages_by_word, lang, words):
        """
        Updates the totals_by_word and languages_by_word dictionaries with the counts of words and their language appearances. It increments the count of each unique word in totals_by_word and updates the language flags in languages_by_word based on the provided language (lang) and the list of words. This is used for Q3 and Q4.
        """
        language_index = LANGUAGE_INDEX.get(lang)
        for word in set(words): # Only count unique words for Q3 and Q4
            if word not in totals_by_word:
                totals_by_word[word] = 0
            totals_by_word[word] += 1
            if language_index is not None:
                if word not in languages_by_word:
                    languages_by_word[word] = [0, 0]
                languages_by_word[word][language_index] = 1

    def _process_data(self, client_id, records):
        """
        Processes the data by extracting words from the abstracts, counting them, and sending the results to the appropriate aggregators. For Q3 and Q4, we only need to count unique words, so we use a set to avoid counting duplicates. For Q5, we need to count the total number of words in each abstract, so we preserve repetitions and store the length of the list of words. We also separate old articles (year <= LAST_OLD_YEAR) from new articles (year > LAST_OLD_YEAR) to send them to different consumers.
        """
        totals_by_word = {}
        languages_by_word = {}
        old_lengths = []
        new_articles = []
        for lang, year, url, abstract in records:
            words = self._extract_words(abstract)
            self._add_counts(totals_by_word, languages_by_word, lang, words)
            if year <= LAST_OLD_YEAR:
                old_lengths.append(len(words))
            else:
                new_articles.append([url, len(words)])

        def build_languages_message(member_languages):
            """
            Builds a message containing the languages in which each word appears. Each record is a list containing the word and two binary flags indicating whether the word appears in any abstract of English and French, respectively.
            """
            languages_records = []
            for word, languages in member_languages.items():
                languages_records.append([word, languages[0], languages[1]])
            return message_protocol.internal.serialize_data(client_id, languages_records)

        def build_totals_message(member_totals):
            """
            Builds a message containing the total counts of each word. Each record is a list containing the word and its total count.
            """
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
        
    def _process_eof(self, client_id, parser_id):
        """
        Processes the EOF message received from a parser and broadcasts it to all aggregators and counters. The EOF message is serialized with the client_id and a unique identifier for the parser-word_sum pair, allowing each receiver to expect an EOF for each pair.
        """
        logging.info(f"Received EOF from parser {parser_id} for client {client_id}")
        eof_message = message_protocol.internal.serialize_eof(client_id, f"{parser_id}.{ID}")
        self._q3_aggregators.broadcast(eof_message)
        self._q4_aggregators.broadcast(eof_message)
        self._word_counters.broadcast(eof_message)
        self._q5_joiner.broadcast(eof_message)

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
    logging.info("Starting word sum service...")
    word_sum = WordSum()
    word_sum.start()
    return 0


if __name__ == "__main__":
    main()
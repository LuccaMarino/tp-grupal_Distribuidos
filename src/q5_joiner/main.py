import logging
import os
import signal
import threading

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
NEW_ARTICLES_GROUP = os.environ["NEW_ARTICLES_GROUP"]
SUM_PARTIAL_LENGTH_GROUP = os.environ["SUM_PARTIAL_LENGTH_GROUP"]
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
EXPECTED_WORDSUM_EOFS = int(os.environ["EXPECTED_WORDSUM_EOFS"])
EXPECTED_WORDCOUNTER_EOFS = int(os.environ["EXPECTED_WORDCOUNTER_EOFS"])
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]

class Q5Joiner:
    def __init__(self):
        self._new_articles = GroupMember(MOM_HOST, NEW_ARTICLES_GROUP, ID)
        self._sum_partial_length = GroupMember(MOM_HOST, SUM_PARTIAL_LENGTH_GROUP, ID)
        self._results_sender = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._global_stats = {}  # client_id -> [total_words, total_articles]
        self._received_word_sum_eofs = set()
        self._received_word_counter_eofs = set()
        self._lock = threading.Lock()
        os.makedirs(STORAGE_PATH, exist_ok=True)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._new_articles.stop_consuming_threadsafe()
        self._sum_partial_length.stop_consuming_threadsafe()

    def _close(self):
        for connection in (self._new_articles, self._sum_partial_length, self._results_sender):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _process_new_articles_message(self, message, ack, nack):
        ...

    def _process_sum_partial_length_message(self, message, ack, nack):
        ...

    def start(self):
        t1 = threading.Thread(
            target=self._new_articles.start_consuming,
            args=(self._process_new_articles_message,)
        )
        t2 = threading.Thread(
            target=self._sum_partial_length.start_consuming,
            args=(self._process_sum_partial_length_message,)
        )

        t1.start()
        t2.start()

        try:
            t1.join()
            t2.join()
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Q5 joiner service...")
    q5_joiner = Q5Joiner()
    q5_joiner.start()
    return 0

if __name__ == "__main__":
    main()
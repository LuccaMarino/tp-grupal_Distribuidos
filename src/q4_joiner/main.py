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

WORD_INDEX = 0
COUNT_INDEX = 1

def _top_order(word_count):
    """
    Returns a tuple that can be used to sort words by total count in descending order, and by word in ascending order for ties.
    """
    return (-word_count[COUNT_INDEX], word_count[WORD_INDEX])

class Q4Joiner:
    """
    Joiner for the Q4 query. It receives messages from the aggregators, accumulates the top 20 words for each client, and sends the final top 20 to the gateway when all EOFs have been received.
    """
    def __init__(self):
        """
        Initializes the Q4Joiner by setting up the input group member, buffers for top words, and EOF tracking for each client.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._tops_by_client = {}    # client_id -> list of [word, total] with the top 20 words accumulated for this client
        self._eofs_by_client = {}    # client_id -> set with the sender_id of the received EOFs
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer thread. This allows the joiner to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group member, handling any exceptions that may occur during the close operation.
        """
        try:
            self._input.close()
        except MessageMiddlewareCloseError as e:
            logging.error(e)

    def _process_query_result(self, client_id, query, records):
        """
        Processes incoming query result messages by updating the global top 20 words for the specified client with the local top 20 words received from an aggregator. If the message is not of type Q4, it logs an error and discards the message.
        """
        if query != message_protocol.internal.Query.Q4:
            logging.error(f"Discarding result of unexpected query {query}")
            return
        if client_id not in self._tops_by_client:
            self._tops_by_client[client_id] = []
        current_top = self._tops_by_client[client_id]
        self._tops_by_client[client_id] = self._top_words(current_top + records)

    def _process_eof(self, client_id, sender_id):
        """
        Processes incoming EOF messages by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it triggers the flush operation to compute and send the top 20 words to the gateway.
        """
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = set()
        eofs = self._eofs_by_client[client_id]
        eofs.add(sender_id)
        if len(eofs) == AGGREGATOR_AMOUNT:
            self._flush_client(client_id)
            
    def _top_words(self, candidates):
        """
        Computes the global top 20 words by sorting the candidates according to the defined order and retaining only the top 20 words.
        """
        sorted_words = sorted(candidates, key=_top_order)
        return sorted_words[:TOP_SIZE]

    def _flush_client(self, client_id):
        """
        Flushes the remaining top words for the specified client and sends the results along with an EOF message to the gateway. After sending, it cleans up by deleting the state associated with that client. Output connection is opened here for the first time and closed after the flush to avoid being idle while waiting for the top words from the q4_aggregators.
        """
        top = self._tops_by_client.pop(client_id, [])
        self._eofs_by_client.pop(client_id)
        logging.info(f"Client {client_id} finished: sending top {len(top)}")
        results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        try:
            results.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q4, top))
            results.send(message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q4, ID))
        finally:
            try:
                results.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)
    
    def _process_message(self, message, ack, nack):
        """
        Processes incoming messages by deserializing them and determining their type. Depending on the message type (QUERY_RESULT or EOF), it calls the appropriate processing method.
        """
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
    logging.info("Starting q4 joiner service...")
    q4_joiner = Q4Joiner()
    q4_joiner.start()
    return 0


if __name__ == "__main__":
    main()
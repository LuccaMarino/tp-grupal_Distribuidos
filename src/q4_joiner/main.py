import logging
import os
import signal

from common import message_protocol
from common.heartbeat import HeartbeatResponder
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
from common.state_storage import ClientStateStorage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
RESULTS_GROUP = os.environ["RESULTS_GROUP"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
AGGREGATOR_AMOUNT = int(os.environ["AGGREGATOR_AMOUNT"])
STORAGE_PATH = os.environ["STORAGE_PATH"]

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
        Initializes the Q4Joiner by setting up the input group member, the results sender to the gateway, buffers for top words, and EOF tracking for each client.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._results = GroupSender(MOM_HOST, RESULTS_GROUP, GATEWAY_AMOUNT)
        self._store = ClientStateStorage(STORAGE_PATH, lambda: {"tops": []})
        self._store.load(on_recover=self._on_recover, send_fn=self._dispatch_send)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _on_recover(self, client_id, state, eofs, pending_sends):
        logging.info(
            "Recovered state for client %s: top_len=%d, eofs=%d/%d, pending_sends=%d",
            client_id,
            len(state["tops"]),
            len(eofs),
            AGGREGATOR_AMOUNT,
            len(pending_sends),
        )

    def _dispatch_send(self, client_id, item):
        if item["type"] == "QUERY_RESULT":
            msg = message_protocol.internal.serialize_query_result(
                client_id, message_protocol.internal.Query.Q4, item["records"], message_id=item["message_id"]
            )
            self._results.send(msg)
            return False
        if item["type"] == "QUERY_EOF":
            msg = message_protocol.internal.serialize_query_eof(
                client_id, message_protocol.internal.Query.Q4, ID, message_id=item["message_id"]
            )
            self._results.send(msg)
            return True
        return False

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer thread. This allows the joiner to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group member and the results sender, handling any exceptions that may occur during the close operation.
        """
        for connection in (self._input, self._results):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _process_query_result(self, client_id, message_id, query, records):
        """
        Processes incoming query result messages by updating the global top 20 words for the specified client with the local top 20 words received from an aggregator. If the message is not of type Q4, it logs an error and discards the message.
        """
        if query != message_protocol.internal.Query.Q4:
            logging.error(f"Discarding result of unexpected query {query}")
            return
        if self._store.is_completed(client_id):
            return
        if self._store.is_duplicate_msg(client_id, message_id):
            logging.debug(f"Duplicate message {message_id} for client {client_id}, ignoring.")
            self._store.flush_pending(client_id, self._dispatch_send)
            return
        self._store.mark_msg_seen(client_id, message_id)
        state = self._store.get_state(client_id)
        state["tops"] = self._top_words(state["tops"] + records)
        self._store.persist(client_id)

    def _process_eof(self, client_id, message_id, sender_id):
        """
        Processes incoming EOF messages by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it triggers the flush operation to compute and send the top 20 words to the gateway.
        """
        if self._store.is_completed(client_id):
            return
        if self._store.is_duplicate_eof(client_id, sender_id):
            logging.debug(f"Duplicate EOF from sender {sender_id} for client {client_id}, ignoring.")
            self._store.flush_pending(client_id, self._dispatch_send)
            return
        eof_count = self._store.add_eof(client_id, message_id, sender_id)
        if eof_count == AGGREGATOR_AMOUNT:
            self._flush_client(client_id)
        else:
            self._store.persist(client_id)
            
    def _top_words(self, candidates):
        """
        Computes the global top 20 words by sorting the candidates according to the defined order and retaining only the top 20 words.
        """
        sorted_words = sorted(candidates, key=_top_order)
        return sorted_words[:TOP_SIZE]

    def _flush_client(self, client_id):
        """
        Sends the global top 20 words for the specified client along with an EOF message to the gateway. After sending, it cleans up the state associated with that client.
        """
        top = self._store.get_state(client_id).get("tops", [])
        logging.info(f"Client {client_id} finished: sending top {len(top)}")
        self._store.enqueue_sends(client_id, [
            {"type": "QUERY_RESULT", "message_id": f"q4_join_{ID}.0", "records": top},
            {"type": "QUERY_EOF", "message_id": f"q4_join_{ID}.eof"},
        ])
        self._store.persist(client_id)
        self._store.flush_pending(client_id, self._dispatch_send)
    
    def _process_message(self, message, ack, nack):
        """
        Processes incoming messages by deserializing them and determining their type. Depending on the message type (QUERY_RESULT or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, message_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.QUERY_RESULT:
                self._process_query_result(client_id, message_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_eof(client_id, message_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type {msg_type}")
            ack()
        except (ValueError, TypeError, AttributeError) as e:
            logging.error(f"Discarding malformed message: {e!r}")
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e!r}")
            nack()

    def start(self):
        try:
            self._input.start_consuming(self._process_message)
        finally:
            self._close()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting q4 joiner service...")
    heartbeat_responder = HeartbeatResponder()
    heartbeat_responder.start()
    q4_joiner = Q4Joiner()
    try:
        q4_joiner.start()
    finally:
        heartbeat_responder.stop()
    return 0


if __name__ == "__main__":
    main()
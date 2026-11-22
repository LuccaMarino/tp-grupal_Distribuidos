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
SUM_PARTIAL_LENGTH_GROUP = os.environ["SUM_PARTIAL_LENGTH_GROUP"]
Q5_JOINER_AMOUNT = int(os.environ["Q5_JOINER_AMOUNT"])
EXPECTED_EOFS = int(os.environ["EXPECTED_EOFS"])
SUM_PARTIAL_LENGTH = int(os.environ["SUM_PARTIAL_LENGTH"])
STORAGE_PATH = os.environ["STORAGE_PATH"]

class WordCounter:
    """
    Word counter for the Q5 query. It receives messages from the parsers, accumulates the total words and total articles for each client, and sends the partial sums to the joiner when the accumulated articles reach a certain threshold. When it receives all EOFs for a client, it sends any remaining totals to the joiner and broadcasts an EOF message. It is a stateful node with atomic disk persistence and message deduplication, ensuring that it can recover from crashes without losing data or processing messages multiple times.
    """
    def __init__(self):
        """
        Initializes the WordCounter by setting up the input group member, the sum partial length sender, and buffers for statistics and EOF tracking for each client. It also sets up a signal handler for SIGTERM to allow graceful shutdown.
        """
        self._input = GroupMember(MOM_HOST, INPUT_GROUP, ID)
        self._sum_partial_length = GroupSender(MOM_HOST, SUM_PARTIAL_LENGTH_GROUP, Q5_JOINER_AMOUNT)
        self._store = ClientStateStorage(STORAGE_PATH, lambda: {"stats": [0, 0], "out_seq": 0})
        self._store.load(on_recover=self._on_recover, send_fn=self._dispatch_send)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _on_recover(self, client_id, state, eofs, pending):
        logging.info(
            "Recovered state for client %s: stats=%s, received_eofs=%s, out_seq=%s, pending_sends=%d",
            client_id,
            state["stats"],
            eofs,
            state["out_seq"],
            len(pending),
        )

    def _dispatch_send(self, client_id, item):
        if item["type"] == "DATA":
            msg = message_protocol.internal.serialize_data(client_id, item["records"], message_id=item["message_id"])
            self._sum_partial_length.send(msg)
            return False
        if item["type"] == "EOF":
            msg = message_protocol.internal.serialize_eof(client_id, item["sender_id"], message_id=item["message_id"])
            self._sum_partial_length.broadcast(msg)
            return True
        return False

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the input consumer thread. This allows the word counter to finish processing any remaining messages before shutting down.
        """
        logging.info("Received SIGTERM signal")
        self._input.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the input group member and the sum partial length sender, handling any exceptions that may occur during the close operations.
        """
        for connection in (self._input, self._sum_partial_length):
            try:
                connection.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def _process_data(self, client_id, message_id, records):
        """
        Processes incoming data messages by updating the total words and total articles for the specified client. If the accumulated articles reach the SUM_PARTIAL_LENGTH threshold, it sends the partial sums to the joiner and resets the counters for that client.
        """
        if self._store.is_completed(client_id):
            return
        if self._store.is_duplicate_msg(client_id, message_id):
            logging.debug(f"Duplicate message {message_id} for client {client_id}, ignoring.")
            self._store.flush_pending(client_id, self._dispatch_send)
            return
        self._store.mark_msg_seen(client_id, message_id)
        state = self._store.get_state(client_id)
        state["stats"][0] += sum(records)
        state["stats"][1] += len(records)

        if state["stats"][1] >= SUM_PARTIAL_LENGTH:
            total_words, total_articles = state["stats"]
            out_msg_id = f"wc_{ID}.{state['out_seq']}"
            state["out_seq"] += 1
            state["stats"] = [0, 0]
            self._store.enqueue_sends(client_id, [{
                "type": "DATA",
                "message_id": out_msg_id,
                "records": [total_words, total_articles],
            }])
            self._store.persist(client_id)
            self._store.flush_pending(client_id, self._dispatch_send)
        else:
            self._store.persist(client_id)

    def _process_eof(self, client_id, message_id, sender_id):
        """
        Processes incoming EOF messages by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it sends any remaining totals to the joiner and broadcasts an EOF message to indicate the end of results for that client.
        """
        if self._store.is_completed(client_id):
            return
        if self._store.is_duplicate_eof(client_id, sender_id):
            logging.debug(f"Duplicate EOF from sender {sender_id} for client {client_id}, ignoring.")
            self._store.flush_pending(client_id, self._dispatch_send)
            return

        logging.info(f"Received EOF from sender {sender_id} for client {client_id}")
        eof_count = self._store.add_eof(client_id, message_id, sender_id)

        if eof_count == EXPECTED_EOFS:
            state = self._store.get_state(client_id)
            total_words, total_articles = state["stats"]
            final_data_id = f"wc_{ID}.{state['out_seq']}"
            final_eof_id = f"wc_{ID}.eof"
            state["out_seq"] += 1
            state["stats"] = [0, 0]
            self._store.enqueue_sends(client_id, [
                {
                    "type": "DATA",
                    "message_id": final_data_id,
                    "records": [total_words, total_articles],
                },
                {
                    "type": "EOF",
                    "message_id": final_eof_id,
                    "sender_id": f"word_counter_{ID}",
                },
            ])
            self._store.persist(client_id)
            self._store.flush_pending(client_id, self._dispatch_send)
        else:
            self._store.persist(client_id)

    def _process_message(self, message, ack, nack):
        """
        Processes incoming messages by deserializing them and determining their type. Depending on the message type (DATA or EOF), it calls the appropriate processing method.
        """
        try:
            msg_type, client_id, message_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_data(client_id, message_id, *args)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                self._process_eof(client_id, message_id, *args)
            else:
                logging.error(f"Discarding message with unexpected type: {msg_type}")
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
    logging.info("Starting word counter service...")
    heartbeat_responder = HeartbeatResponder()
    heartbeat_responder.start()
    word_counter = WordCounter()
    try:
        word_counter.start()
    finally:
        heartbeat_responder.stop()
    return 0

if __name__ == "__main__":
    main()
import logging
import os
import signal

from common import message_protocol
from common.heartbeat import HeartbeatResponder
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
from common.state_storage import SequentialIdTracker, atomic_write_json, read_json

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
        self._stats = {} # client_id -> [total_words, total_articles]
        self._received_eofs = {} # client_id -> set of sender_ids that sent EOF
        self._seen_ids: dict[str, SequentialIdTracker] = {}
        self._out_seq: dict[str, int] = {}
        self._completed_clients = set()
        self._load_state()
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _client_state_path(self, client_id):
        """
        Returns the file path for storing the state of a specific client.
        """
        return os.path.join(STORAGE_PATH, f"client_{client_id}.json")

    def _completed_path(self):
        """
        Returns the file path for storing the list of completed clients.
        """
        return os.path.join(STORAGE_PATH, "completed_clients.json")

    def _load_state(self):
        """
        Loads the state of the word counter from disk, recovering statistics, received EOFs, seen message IDs, and output sequence numbers for each client. It also removes state files for clients that have already completed processing.
        """
        os.makedirs(STORAGE_PATH, exist_ok=True)
        completed = read_json(self._completed_path(), default=[])
        self._completed_clients = set(completed)

        for filename in os.listdir(STORAGE_PATH):
            if not filename.startswith("client_") or not filename.endswith(".json"):
                continue
            filepath = os.path.join(STORAGE_PATH, filename)
            data = read_json(filepath)
            if not data:
                continue
            client_id = data["client_id"]
            if client_id in self._completed_clients:
                try:
                    os.remove(filepath)
                except OSError:
                    pass
                continue
            self._stats[client_id] = data.get("stats", [0, 0])
            self._received_eofs[client_id] = set(data.get("received_eofs", []))
            self._seen_ids[client_id] = SequentialIdTracker.from_dict(data.get("seen_ids", {}))
            self._out_seq[client_id] = data.get("out_seq", 0)
            logging.info(
                "Recovered state for client %s: stats=%s, received_eofs=%s, seen_ids=%s, out_seq=%s",
                client_id,
                self._stats[client_id],
                self._received_eofs[client_id],
                self._seen_ids[client_id].to_dict(),
                self._out_seq[client_id],
            )

    def _persist_client(self, client_id):
        """
        Persists the state of a specific client to disk, including statistics, received EOFs, seen message IDs, and output sequence numbers.
        """
        self._ensure_client_state(client_id)
        data = {
            "client_id": client_id,
            "stats": self._stats.get(client_id, [0, 0]),
            "received_eofs": list(self._received_eofs.get(client_id, [])),
            "seen_ids": self._seen_ids.get(client_id, SequentialIdTracker()).to_dict(),
            "out_seq": self._out_seq.get(client_id, 0),
        }
        atomic_write_json(self._client_state_path(client_id), data)

    def _mark_client_completed(self, client_id):
        """
        Marks a client as completed by adding it to the completed clients set, persisting the updated list to disk, and removing any associated state files for that client.
        """
        self._completed_clients.add(client_id)
        atomic_write_json(self._completed_path(), sorted(self._completed_clients))
        self._stats.pop(client_id, None)
        self._received_eofs.pop(client_id, None)
        self._seen_ids.pop(client_id, None)
        self._out_seq.pop(client_id, None)
        state_path = self._client_state_path(client_id)
        if os.path.exists(state_path):
            try:
                os.remove(state_path)
            except OSError:
                pass

    def _ensure_client_state(self, client_id):
        """
        Ensures that the state for a specific client is initialized, creating default entries for statistics, received EOFs, seen message IDs, and output sequence numbers if they do not already exist.
        """
        if client_id not in self._stats:
            self._stats[client_id] = [0, 0]
        if client_id not in self._received_eofs:
            self._received_eofs[client_id] = set()
        if client_id not in self._seen_ids:
            self._seen_ids[client_id] = SequentialIdTracker()
        if client_id not in self._out_seq:
            self._out_seq[client_id] = 0

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
        if client_id in self._completed_clients:
            return
        self._ensure_client_state(client_id)
        if self._seen_ids[client_id].contains(message_id):
            logging.debug(f"Duplicate message {message_id} for client {client_id}, ignoring.")
            return
        self._seen_ids[client_id].add(message_id)
        self._stats[client_id][0] += sum(records)
        self._stats[client_id][1] += len(records)

        if self._stats[client_id][1] >= SUM_PARTIAL_LENGTH:
            total_words, total_articles = self._stats[client_id]
            out_msg_id = f"wc_{ID}.{self._out_seq[client_id]}"
            self._out_seq[client_id] += 1
            self._stats[client_id] = [0, 0]
            self._persist_client(client_id)
            data_message = message_protocol.internal.serialize_data(client_id, [total_words, total_articles], message_id=out_msg_id)
            self._sum_partial_length.send(data_message)
        else:
            self._persist_client(client_id)

    def _process_eof(self, client_id, message_id, sender_id):
        """
        Processes incoming EOF messages by tracking the sender IDs for each client. If all expected EOFs have been received for a client, it sends any remaining totals to the joiner and broadcasts an EOF message to indicate the end of results for that client.
        """
        if client_id in self._completed_clients:
            return
        self._ensure_client_state(client_id)
        if sender_id in self._received_eofs[client_id]:
            logging.debug(f"Duplicate EOF from sender {sender_id} for client {client_id}, ignoring.")
            return

        logging.info(f"Received EOF from sender {sender_id} for client {client_id}")
        self._seen_ids[client_id].add(message_id)
        self._received_eofs[client_id].add(sender_id)
        
        if len(self._received_eofs[client_id]) == EXPECTED_EOFS:
            total_words, total_articles = self._stats.get(client_id, [0, 0])
            final_data_id = f"wc_{ID}.{self._out_seq[client_id]}"
            final_eof_id = f"wc_{ID}.eof"
            self._out_seq[client_id] += 1
            self._stats[client_id] = [0, 0]
            self._persist_client(client_id)
            data_message = message_protocol.internal.serialize_data(client_id, [total_words, total_articles], message_id=final_data_id)
            self._sum_partial_length.send(data_message)
            eof_message = message_protocol.internal.serialize_eof(client_id, f"word_counter_{ID}", message_id=final_eof_id)
            self._sum_partial_length.broadcast(eof_message)
            self._mark_client_completed(client_id)
        else:
            self._persist_client(client_id)

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
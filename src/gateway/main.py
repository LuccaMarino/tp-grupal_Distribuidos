import logging
import os
import signal
import socket
import queue
import threading

from common.middleware import GroupSender, GroupMember
import message_handler
from common import message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
OUTPUT_GROUP = os.environ["OUTPUT_GROUP"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
PARSER_AMOUNT = int(os.environ["PARSER_AMOUNT"])
IMAGE_DOWNLOADER_AMOUNT = int(os.environ["IMAGE_DOWNLOADER_AMOUNT"])
Q3_AGGREGATOR_AMOUNT = int(os.environ["Q3_AGGREGATOR_AMOUNT"])
GATEWAY_ID = 0
EXPECTED_QUERY_EOFS = {
    message_protocol.internal.Query.Q1: PARSER_AMOUNT,
    message_protocol.internal.Query.Q2: IMAGE_DOWNLOADER_AMOUNT * PARSER_AMOUNT,
    message_protocol.internal.Query.Q3: Q3_AGGREGATOR_AMOUNT,
}

class ClientConnection:
    """
    Represents a connection with a client. It holds the socket, message handler, outgoing message queue, and a closed event to signal when the connection is closed.
    """
    def __init__(self, client_socket, message_handler):
        self.socket = client_socket
        self.message_handler = message_handler
        self.outgoing = queue.Queue()
        self.closed = threading.Event()

class ConsumerController:
    """
    Controls the registration of a GroupMember and allows for requesting a stop of message consumption in a thread-safe manner.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._member = None
        self._stop_requested = False

    """
    Registers a GroupMember to the controller. If a stop has already been requested, it will immediately stop the member from consuming messages.
    """
    def register(self, member):
        with self._lock:
            self._member = member
            should_stop = self._stop_requested
        if should_stop:
            member.stop_consuming_threadsafe()

    """
    Requests the registered GroupMember to stop consuming messages in a thread-safe manner. If no member is registered yet, it sets a flag to stop when a member is registered.
    """
    def request_stop(self):
        with self._lock:
            self._stop_requested = True
            member = self._member
        if member is not None:
            member.stop_consuming_threadsafe()

def handle_client_request(connection):
    """
    Handles incoming messages from the client. It receives Wikipedia articles and sends them to the output group. It also handles the end of records signal and sends an acknowledgment back to the client.
    """
    output_group = GroupSender(MOM_HOST, OUTPUT_GROUP, PARSER_AMOUNT)
    client_socket = connection.socket
    try:
        while True:
            msg_type, data = message_protocol.external.recv_msg(client_socket)
            if msg_type == message_protocol.external.MsgType.WIKI_ARTICLE:
                serialized_message = connection.message_handler.serialize_article_message(data)
                output_group.send(serialized_message)
                connection.outgoing.put((message_protocol.external.MsgType.ACK, None))
            elif msg_type == message_protocol.external.MsgType.END_OF_RECORDS:
                logging.info("Received end of records signal.")
                serialized_message = connection.message_handler.serialize_eof_message(data)
                output_group.broadcast(serialized_message)
                connection.outgoing.put((message_protocol.external.MsgType.ACK, None))
                return
    except socket.error:
        logging.error("The connection with the client was lost.")
    except Exception as e:
        logging.error(e)
    finally:
        ...
        output_group.close()


def delete_condition_ready(client_id, ended_querys):
    """
    Checks if all expected end-of-query results have been received for a given client. Returns True if all expected EOFs are present, otherwise returns False.
    """
    for query, expected_eofs in EXPECTED_QUERY_EOFS.items():
        if query not in ended_querys[client_id]:
            return False
        if len(ended_querys[client_id][query]) < expected_eofs:
            return False
    return True


def handle_query_eof(outgoing, ended_querys, client_id, query, sender_id):
    """
    Handles the end of a query for a specific client.
    """
    if query not in EXPECTED_QUERY_EOFS:
        outgoing.put((message_protocol.external.MsgType.END_OF_QUERY_RESULTS, query))
        return
    if client_id not in ended_querys:
        ended_querys[client_id] = {}
    if query not in ended_querys[client_id]:
        ended_querys[client_id][query] = set()
    ended_querys[client_id][query].add(sender_id)
    if len(ended_querys[client_id][query]) == EXPECTED_QUERY_EOFS[query]:
        outgoing.put((message_protocol.external.MsgType.END_OF_QUERY_RESULTS, query))
        if delete_condition_ready(client_id, ended_querys):
            del ended_querys[client_id]


def handle_client_response(client_list, client_lock, consumer_controller):
    """
    Handles incoming messages from the input group. It processes query results and end-of-query signals, delivering them to the appropriate client connections. It also manages acknowledgments and error handling.
    """
    input_group = GroupMember(MOM_HOST, INPUT_GROUP, GATEWAY_ID)
    ended_querys = {}
    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            with client_lock:
                for connection in client_list:
                    deserialized_message = connection.message_handler.deserialize_result_message(message)
                    if not deserialized_message:
                        client_index += 1
                        continue
                    type_message, client_id, fields = deserialized_message
                    if type_message == message_protocol.internal.MsgType.QUERY_EOF:
                        query, sender_id = fields
                        handle_query_eof(connection.outgoing, ended_querys, client_id, query, sender_id)
                    else:
                        query, records = fields
                        connection.outgoing.put((message_protocol.external.MsgType.QUERY_RESULT, (query, records)))
                    break
            ack()
        except socket.error:
            logging.error("The connection with the client was lost.")
            client_list.pop(client_index)
            nack()
        except Exception as e:
            logging.error(e)
            ack()
            input_group.stop_consuming()
    try:
        consumer_controller.register(input_group)
        input_group.start_consuming(_consume_result)
    except Exception:
        logging.exception("Error en Rabbit")
    finally:
        try:
            input_group.close()
        except Exception:
            logging.exception("Error closing Rabbit consumer")

def handle_client_send(connection):
    """
    Handles outgoing messages to the client. It retrieves messages from the outgoing queue and sends them to the client socket. It handles different message types, including query results and end-of-query notifications.
    """
    try:
        while True:
            message = connection.outgoing.get()
            if message is None:
                break
            msg_type, data = message
            if msg_type == message_protocol.external.MsgType.QUERY_RESULT:
                query, result = data
                message_protocol.external.send_msg(connection.socket, msg_type, query, result)
            elif msg_type == message_protocol.external.MsgType.END_OF_QUERY_RESULTS:
                message_protocol.external.send_msg(connection.socket, msg_type, data)
            else:
                message_protocol.external.send_msg(connection.socket, msg_type)
    except socket.error:
        logging.error("Connection with the client was lost")
    finally:
        connection.closed.set()

def handle_sigterm(server_socket, client_list, client_lock, sigterm_received):
    """
    Handles the SIGTERM signal. It shuts down the server socket and signals all client connections to close their sockets and stop processing.
    """
    try:
        server_socket.shutdown(socket.SHUT_RDWR)
    except:
        pass
    sigterm_received.set()
    with client_lock:
        for connection in client_list:
            try:
                connection.socket.shutdown(socket.SHUT_RDWR)
                connection.closed.set()
                connection.outgoing.put(None)
            except:
                pass

def main():
    client_list = []
    client_lock = threading.Lock()
    sigterm_received = threading.Event()
    consumer_controller = ConsumerController()
    response_handler_thread = threading.Thread(target=handle_client_response,args=(client_list,client_lock, consumer_controller,),daemon=True)
    response_handler_thread.start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((SERVER_HOST, SERVER_PORT))
        server_socket.listen()
        signal.signal(signal.SIGTERM, lambda signum, frame: handle_sigterm(server_socket, client_list, client_lock, sigterm_received))
        while True:
            try:
                client_socket, _ = server_socket.accept()
                message_handler_instance = message_handler.MessageHandler()
                connection = ClientConnection(client_socket, message_handler_instance)
                with client_lock:
                    client_list.append(connection)
                threading.Thread(target=handle_client_send, args=(connection,), daemon=True).start()
                threading.Thread(target=handle_client_request, args=(connection,), daemon=True).start()
            except socket.error:
                if sigterm_received.is_set():
                    logging.info("SIGTERM received. Shutting down the server...")
                    break                    
            except Exception as e:
                logging.error(f"An error occurred: {e}")
    consumer_controller.request_stop()
    response_handler_thread.join()
    return 0


if __name__ == "__main__":
    main()

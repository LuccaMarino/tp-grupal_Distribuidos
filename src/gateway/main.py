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
GATEWAY_ID = 0 #Por ahora solo tenemos un gateway

class ClientConnection:
    def __init__(self, client_socket, message_handler):
        self.socket = client_socket
        self.message_handler = message_handler
        self.outgoing = queue.Queue()
        self.closed = threading.Event()

def handle_client_request(connection):
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
    if not ended_querys[client_id].get(message_protocol.internal.Query.Q2) or not ended_querys[client_id].get(message_protocol.internal.Query.Q2):
        return False
    return (len(ended_querys[client_id][message_protocol.internal.Query.Q2]) == IMAGE_DOWNLOADER_AMOUNT
            and len(ended_querys[client_id][message_protocol.internal.Query.Q1]) == PARSER_AMOUNT)


def handle_query_eof(outgoing, ended_querys, client_id, query, sender_id):
    if client_id not in ended_querys:
        ended_querys[client_id] = {}
    if query == message_protocol.internal.Query.Q1 or query == message_protocol.internal.Query.Q2:
        if query not in ended_querys[client_id]:
            ended_querys[client_id][query] = set()
        ended_querys[client_id][query].add(sender_id)
        if query == message_protocol.internal.Query.Q1 and len(ended_querys[client_id][query]) == PARSER_AMOUNT:
            outgoing.put((message_protocol.external.MsgType.END_OF_RESULTS,query))
        if query == message_protocol.internal.Query.Q2 and len(ended_querys[client_id][query]) == IMAGE_DOWNLOADER_AMOUNT:
            outgoing.put((message_protocol.external.MsgType.END_OF_RESULTS,query))
        if delete_condition_ready(client_id, ended_querys):
            del ended_querys[client_id]
    else:
        outgoing.put((message_protocol.external.MsgType.END_OF_RESULTS,query))


def handle_client_response(client_list, client_lock):
    input_group = GroupMember(MOM_HOST, INPUT_GROUP, GATEWAY_ID)
    ended_querys = {}
    def _consume_result(message, ack, nack):
        # SHOULD DELIVER QUERY RESULTS TO CLIENTS ON THE GO, AND HANDLE END OF RESULTS.
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
                        connection.outgoing.put((message_protocol.external.MsgType.QUERY_RESULT, deserialized_message))
                    break
            ack()
        except socket.error:
            logging.error("The connection with the client was lost.")
            client_list.pop(client_index)
            nack()
        except Exception as e:
            logging.error(e)
            nack()
            input_group.stop_consuming()
    input_group.start_consuming(_consume_result)
    input_group.close()


def handle_client_send(connection):
    try:
        while True:
            message = connection.outgoing.get()
            if message is None:
                break
            msg_type, data = message
            message_protocol.external.send_msg(connection.socket,msg_type,data,)
    except socket.error:
        logging.error("Connection with the client was lost")
    finally:
        connection.closed.set()

def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for connection in list(client_list):
        connection.socket.shutdown(socket.SHUT_RDWR)
        connection.closed.set()
    sigterm_received.value = 1


def main():
    client_list = []
    client_lock = threading.Lock()
    sigterm_received = threading.Event()
    threading.Thread(target=handle_client_response,args=(client_list,client_lock),daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((SERVER_HOST, SERVER_PORT))
        server_socket.listen()
        signal.signal(signal.SIGTERM, lambda signum, frame: handle_sigterm(server_socket, client_list, sigterm_received))
        while True:
            try:
                client_socket, _ = server_socket.accept()
                message_handler_instance = message_handler.MessageHandler()
                with client_lock:
                    client_list[client_id] = connection
                client_id = message_handler_instance.client_id
                connection = ClientConnection(client_socket, message_handler_instance)
                threading.Thread(target=handle_client_send, args=(connection,), daemon=True).start()
                threading.Thread(target=handle_client_request, args=(connection,), daemon=True).start()
            except socket.error:
                if sigterm_received.value == 0:
                    logging.error("The connection with the client was lost.")
                    return 1
                else:
                    logging.info("SIGTERM received. Shutting down the server...")
                    return 0
            except Exception as e:
                logging.error(f"An error occurred: {e}")
    return 0


if __name__ == "__main__":
    main()

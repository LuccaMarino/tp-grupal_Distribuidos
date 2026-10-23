import multiprocessing
import os
import signal
import socket

from common import message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

def handle_client_request(client_socket):
    # Define output queue to use for sending articles to next node via MOM
    # ...

    try:
        while True:
            msg_type, data = message_protocol.external.recv_msg(client_socket)
            if msg_type == message_protocol.external.MsgType.WIKI_ARTICLE:
                # Serialize and send the article to the next node via MOM
                # ...
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK
                )
            elif msg_type == message_protocol.external.MsgType.END_OF_RECORDS:
                print("Received end of records signal.")
                # Send EOF to next node via MOM
                # ...
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK
                )
                break
    except Exception as e:
        print(f"An error occurred while handling client request: {e}")

def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for client_socket in list(client_list):
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1

def main():
    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value('c_short', 0)
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(signal.SIGTERM, lambda signum, frame: handle_sigterm(server_socket, client_list, sigterm_received))
                while True:
                    try:
                        client_socket, addr = server_socket.accept()
                        client_list.append(client_socket)
                        pool.apply_async(handle_client_request, args=(client_socket,))
                    except socket.error:
                        if sigterm_received.value == 0:
                            print("The connection with the client was lost.")
                            return 1
                        else:
                            print("SIGTERM received. Shutting down the server...")
                            return 0
                    except Exception as e:
                        print(f"An error occurred: {e}")
    return 0

if __name__ == "__main__":
    main()
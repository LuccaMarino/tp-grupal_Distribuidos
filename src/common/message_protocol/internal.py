import json

class MsgType:
    """
    Enumerates the different types of internal messages used in the protocol.
    Each message type is associated with a unique integer value.
    Attributes:
        DATA (int): Represents a data message containing records.
        EOF (int): Represents an end-of-file message indicating no more data will be sent.
        QUERY_RESULT (int): Represents a message containing the results of a query.
        QUERY_EOF (int): Represents a message indicating that all results for a query have been sent.
    Each internal message has the form `[msg_type, client_id, ...fields specific to the message type]`.
    """
    DATA = 1
    EOF = 2
    QUERY_RESULT = 3
    QUERY_EOF = 4

class Query:
    """
    Enumerates the different types of queries that can be performed.
    Each query type is associated with a unique integer value.
    """
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    Q5 = 5


def _serialize(fields):
    """
    Serializes the given fields into a JSON-encoded byte string.
    """
    return json.dumps(fields).encode("utf-8")

def serialize_data(client_id, records):
    """
    Serializes a data message containing records for a specific client.
    """
    return _serialize([MsgType.DATA, client_id, records])

def serialize_eof(client_id, sender_id):
    """
    Serializes an EOF (End of File) message for a specific client, indicating that no more data will be sent from the sender identified by `sender_id`.
    The receiver can use the `sender_id` to track which senders have completed sending data.
    """
    return _serialize([MsgType.EOF, client_id, sender_id])

def serialize_query_result(client_id, query, records):
    """
    Serializes a query result message for a specific client. The message includes the query type and the associated records.
    """
    return _serialize([MsgType.QUERY_RESULT, client_id, query, records])

def serialize_query_eof(client_id, query, sender_id):
    """
    Serializes a query EOF (End of File) message for a specific client, indicating that all results for the specified query have been sent from the sender identified by `sender_id`.
    """
    return _serialize([MsgType.QUERY_EOF, client_id, query, sender_id])

def deserialize(message):
    """
    Deserializes a message received from the gateway. It decodes the JSON-encoded byte string and extracts the message type, client ID, and any additional arguments.
    Returns:
        tuple: A tuple containing the message type, client ID, and a list of additional arguments.
    """
    [msg_type, client_id, *args] = json.loads(message.decode("utf-8"))
    return msg_type, client_id, args
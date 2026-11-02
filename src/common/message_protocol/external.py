from asyncio import IncompleteReadError
from datetime import datetime
from . import external_serializer
import json
class MsgType:
    """
    Enum-like class for message types.
    """
    WIKI_ARTICLE = 1
    END_OF_RECORDS = 2
    ACK = 3
    QUERY_RESULT = 4
    END_OF_QUERY_RESULTS = 5

def _serialize_wiki_article_batch(articles: list) -> bytes:
    batch_bytes = [external_serializer.serialize_uint32(len(articles))]
    for article in articles:
        batch_bytes.append(_serialize_wiki_article(article))
    return b"".join(batch_bytes)

def _recv_sized(socket, size: int) -> bytes:
    """
    Receive a specific number of bytes from the socket, preventing short reads. Raises `IncompleteReadError` if the connection is closed before the specified number of bytes is received.
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)

def _recv_wiki_articles(socket) -> dict:
    """
    Receive a WIKI_ARTICLE message from the socket and deserialize it into a dictionary containing the articles' details. The dictionary includes the following keys: `name`, `abstract`, `lang`, `url`, `sections`, and `date_modified`.
    """
    count = external_serializer.deserialize_uint32(_recv_sized(socket, external_serializer.UINT32_SIZE))
    articles = []
    for _ in range(count):
        name = external_serializer.deserialize_string(socket, _recv_sized)
        abstract = external_serializer.deserialize_string(socket, _recv_sized)
        lang = external_serializer.deserialize_string(socket, _recv_sized)
        url = external_serializer.deserialize_string(socket, _recv_sized)
        sections = external_serializer.deserialize_string(socket, _recv_sized)
        image = external_serializer.deserialize_string(socket, _recv_sized)
        date_modified_bytes = _recv_sized(socket, external_serializer.UINT64_SIZE)
        date_modified = external_serializer.deserialize_uint64(date_modified_bytes)
        articles.append({
            "name": name,
            "abstract": abstract,
            "lang": lang,
            "url": url,
            "sections": sections,
            "image": image,
            "date_modified": date_modified,
        })
    return articles

def _recv_empty(socket):
    """
    Receive an empty message from the socket. It's used for message types that do not carry any additional data, such as `END_OF_RECORDS` and `ACK`. It simply returns `None`.
    """
    return None

def _recv_query_result(sock):
    query = external_serializer.deserialize_uint32(
        _recv_sized(sock, external_serializer.UINT32_SIZE)
    )
    result_json = external_serializer.deserialize_string(sock, _recv_sized)
    return query, json.loads(result_json)


def _recv_end_of_query_results(socket):
    return external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )

RECV_MSG_HANDLERS = {
    MsgType.WIKI_ARTICLE: _recv_wiki_articles,
    MsgType.END_OF_RECORDS: _recv_empty,
    MsgType.ACK: _recv_empty,
    MsgType.QUERY_RESULT: _recv_query_result,
    MsgType.END_OF_QUERY_RESULTS: _recv_end_of_query_results
}
"""
    A dictionary mapping message types to their corresponding handler functions for receiving messages. Each message type is associated with a function that knows how to deserialize the message from the socket.
    - `MsgType.WIKI_ARTICLE`: Handled by `_recv_wiki_articles`, which deserializes a WIKI_ARTICLE message into a list of dictionaries containing the articles' details.
    - `MsgType.END_OF_RECORDS`: Handled by `_recv_empty`, which simply returns `None` since this message type does not carry any additional data.
    - `MsgType.ACK`: Handled by `_recv_empty`, which also returns `None` as this message type does not carry any additional data.
"""

def recv_msg(socket):
    """
    Receive a message from the socket, determine its type, and deserialize it using the appropriate handler function. Returns a tuple containing the message type and the deserialized data.
    """
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, handler(socket))

def _serialize_wiki_article(article: dict) -> bytes:
    """
    Serialize a WIKI_ARTICLE message into a bytes object.
    """
    name = article.get("name") or ""
    abstract = article.get("abstract") or ""
    lang = article.get("in_language").get("identifier", "")
    url = article.get("url") or ""
    sections = article.get("sections") or ""

    raw_image = article.get("image")
    if isinstance(raw_image, dict):
        image = str(raw_image.get("content_url") or "")
    else:
        image = ""

    raw_date = article.get("date_modified")
    if isinstance(raw_date, datetime):
        date_modified = int(raw_date.timestamp() * 1000)
    elif raw_date is not None:
        date_modified = int(raw_date)
    else:
        date_modified = 0

    return b"".join([
        external_serializer.serialize_string(name),
        external_serializer.serialize_string(abstract),
        external_serializer.serialize_string(lang),
        external_serializer.serialize_string(url),
        external_serializer.serialize_string(sections),
        external_serializer.serialize_string(image),
        external_serializer.serialize_uint64(date_modified),
    ])

def _send_wiki_articles(socket, articles):
    """
    Send a WIKI_ARTICLE message to the socket. The articles are serialized into bytes and sent along with the message type.
    """
    if isinstance(articles, dict):
        articles = [articles]
    msg = external_serializer.serialize_uint32(MsgType.WIKI_ARTICLE)
    msg += _serialize_wiki_article_batch(articles)
    socket.sendall(msg)

def _send_end_of_records(socket):
    """
    Send an END_OF_RECORDS message to the socket. This message indicates that there are no more records to be sent.
    """
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECORDS))

def _send_ack(socket):
    """
    Send an ACK message to the socket. This message is used to acknowledge receipt of a previous message.
    """
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))

def _send_query_result(sock, query, result):
    query_bytes = external_serializer.serialize_uint32(query)
    result_json = json.dumps(result, ensure_ascii=False)
    result_bytes = external_serializer.serialize_string(result_json)
    sock.sendall( external_serializer.serialize_uint32(MsgType.QUERY_RESULT) + query_bytes + result_bytes)

def _send_end_of_query_results(sock, query):
    sock.sendall(external_serializer.serialize_uint32(MsgType.END_OF_QUERY_RESULTS) + external_serializer.serialize_uint32(query))

SEND_MSG_HANDLERS = {
    MsgType.WIKI_ARTICLE: _send_wiki_articles,
    MsgType.END_OF_RECORDS: _send_end_of_records,
    MsgType.ACK: _send_ack,
    MsgType.QUERY_RESULT: _send_query_result,
    MsgType.END_OF_QUERY_RESULTS: _send_end_of_query_results
}
"""
    A dictionary mapping message types to their corresponding handler functions for sending messages. Each message type is associated with a function that knows how to serialize and send the message to the socket.
    - `MsgType.WIKI_ARTICLE`: Handled by `_send_wiki_articles`, which serializes a WIKI_ARTICLE containing a batch of articles and sends it to the socket.
    - `MsgType.END_OF_RECORDS`: Handled by `_send_end_of_records`, which sends an END_OF_RECORDS message to the socket, indicating that there are no more records to be sent.
    - `MsgType.ACK`: Handled by `_send_ack`, which sends an ACK message to the socket, acknowledging receipt of a previous message.
"""

def send_msg(socket, msg_type, *args):
    """
    Send a message of the specified type to the socket, using the appropriate handler function to serialize the message.
    """
    handler = SEND_MSG_HANDLERS[msg_type]
    handler(socket, *args)
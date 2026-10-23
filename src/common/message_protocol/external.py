from asyncio import IncompleteReadError
from datetime import datetime
from . import external_serializer

class MsgType:
    WIKI_ARTICLE = 1
    END_OF_RECORDS = 2
    ACK = 3

def _recv_sized(socket, size: int) -> bytes:
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)

def _recv_wiki_article(socket) -> dict:
    name = external_serializer.deserialize_string(socket, _recv_sized)
    abstract = external_serializer.deserialize_string(socket, _recv_sized)
    lang = external_serializer.deserialize_string(socket, _recv_sized)
    url = external_serializer.deserialize_string(socket, _recv_sized)
    sections = external_serializer.deserialize_string(socket, _recv_sized)
    date_modified_bytes = _recv_sized(socket, external_serializer.UINT64_SIZE)
    date_modified = external_serializer.deserialize_uint64(date_modified_bytes)

    return {
        "name": name,
        "abstract": abstract,
        "lang": lang,
        "url": url,
        "sections": sections,
        "date_modified": date_modified,
    }

def _recv_empty(socket):
    return None

RECV_MSG_HANDLERS = {
    MsgType.WIKI_ARTICLE: _recv_wiki_article,
    MsgType.END_OF_RECORDS: _recv_empty,
    MsgType.ACK: _recv_empty,
}

def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, handler(socket))

def _serialize_wiki_article(article: dict) -> bytes:
    name = article.get("name") or article.get("title") or ""
    abstract = article.get("abstract") or ""

    in_lang = article.get("in_language")
    if isinstance(in_lang, dict):
        lang = in_lang.get("identifier", "")
    else:
        lang = article.get("lang") or ""

    url = article.get("url") or ""
    sections = article.get("sections") or ""

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
        external_serializer.serialize_uint64(date_modified),
    ])

def _send_wiki_article(socket, article: dict):
    msg = external_serializer.serialize_uint32(MsgType.WIKI_ARTICLE)
    msg += _serialize_wiki_article(article)
    socket.sendall(msg)

def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECORDS))

def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))

SEND_MSG_HANDLERS = {
    MsgType.WIKI_ARTICLE: _send_wiki_article,
    MsgType.END_OF_RECORDS: _send_end_of_records,
    MsgType.ACK: _send_ack,
}

def send_msg(socket, msg_type, *args):
    handler = SEND_MSG_HANDLERS[msg_type]
    handler(socket, *args)
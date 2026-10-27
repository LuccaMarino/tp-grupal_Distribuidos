import json

# Cada mensaje interno  tiene la forma [msg_type, client_id, ...campos del tipo]

class MsgType:
    DATA = 1
    EOF = 2
    QUERY_RESULT = 3
    QUERY_EOF = 4

# Para indicar de qué tipo de query son los datos
class Query:
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    Q5 = 5


def _serialize(fields):
    return json.dumps(fields).encode("utf-8")

def serialize_data(client_id, records):
    return _serialize([MsgType.DATA, client_id, records])

# Fin de los datos de un cliente de parte del emisor sender_id. El receptor
# guardaría los sender_id recibidos para saber cuándo le llegaron todos
def serialize_eof(client_id, sender_id):
    return _serialize([MsgType.EOF, client_id, sender_id])

# Para envio de resultados de una query (query es de tipo Query)
def serialize_query_result(client_id, query, records):
    return _serialize([MsgType.QUERY_RESULT, client_id, query, records])

# Para avisar que se mandaron todos los resultados de una query
def serialize_query_eof(client_id, query, sender_id):
    return _serialize([MsgType.QUERY_EOF, client_id, query, sender_id])

def deserialize(message):
    [msg_type, client_id, *args] = json.loads(message.decode("utf-8"))
    return msg_type, client_id, args
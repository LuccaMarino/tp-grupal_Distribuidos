import uuid
from common import message_protocol

GATEWAY_ID = 0

class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_article_message(self, data):
        return message_protocol.internal.serialize_data(self.client_id, [data])

    def serialize_eof_message(self, data):
        return message_protocol.internal.serialize_eof(self.client_id, GATEWAY_ID)

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        ...
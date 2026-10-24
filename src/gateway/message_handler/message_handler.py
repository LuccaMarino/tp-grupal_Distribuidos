import uuid
from common import message_protocol

class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_article_message(self, article):
        ...

    def deserialize_result_message(self, message):
        ...
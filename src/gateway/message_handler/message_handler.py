import uuid
from common import message_protocol

GATEWAY_ID = 0

class MessageHandler:
    def __init__(self):
        """
        Initialize the MessageHandler with a unique client ID.
        """
        self.client_id = str(uuid.uuid4())

    def serialize_article_message(self, data):
        """
        Serialize the given data into a message format that can be sent to the gateway.
        """
        if isinstance(data, list):
            return message_protocol.internal.serialize_data(self.client_id, data)
        return message_protocol.internal.serialize_data(self.client_id, [data])

    def serialize_eof_message(self, data):
        """
        Serialize an EOF (End of File) message to indicate that no more data will be sent.
        """
        return message_protocol.internal.serialize_eof(self.client_id, GATEWAY_ID)


    def deserialize_result_message(self, message):
        """
        Deserialize a message received from the gateway. If the message is not intended for this client, return None.
        """
        msg_type, client_id, args = message_protocol.internal.deserialize(message)
        if client_id != self.client_id:
            return None
        else:
            return msg_type, client_id, args

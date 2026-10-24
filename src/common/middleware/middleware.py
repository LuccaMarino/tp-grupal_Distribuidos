from abc import ABC, abstractmethod


class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddlewareDeleteError(Exception):
    pass


class MessageMiddleware(ABC):

    # Starts listening to the queue/exchange and invokes on_message_callback after
    # each data or control message with the message body.
    # on_message_callback parameters:
    # message - The value exactly as received by the send method of this class.
    # ack - Function that, when invoked, sends an ACK for the message being consumed.
    # nack - Function that, when invoked, sends a NACK for the message being consumed.
    # Raises MessageMiddlewareDisconnectedError if the connection to the middleware is lost.
    # Raises MessageMiddlewareMessageError if an unresolvable internal error occurs.
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    # Stops listening if consuming from the queue/exchange. Has no effect
    # and raises no exceptions if not currently consuming from the queue/exchange.
    # Raises MessageMiddlewareDisconnectedError if the connection to the middleware is lost.
    @abstractmethod
    def stop_consuming(self):
        pass

    # Sends a message to the queue or topic with which the exchange was initialized.
    # Raises MessageMiddlewareDisconnectedError if the connection to the middleware is lost.
    # Raises MessageMiddlewareMessageError if an unresolvable internal error occurs.
    @abstractmethod
    def send(self, message):
        pass

    # Disconnects from the connected queue or exchange.
    # Raises MessageMiddlewareCloseError if an unresolvable internal error occurs.
    @abstractmethod
    def close(self):
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    @abstractmethod
    def __init__(self, host, exchange_name, route_keys):
        pass


class MessageMiddlewareQueue(MessageMiddleware):
    @abstractmethod
    def __init__(self, host, queue_name):
        pass
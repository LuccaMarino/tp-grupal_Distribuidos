import logging

import pika
from pika.exceptions import AMQPError, AMQPConnectionError
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)

DISCONNECTED_MSG = "Lost connection to RabbitMQ while {}: {!r}"
RABBITMQ_ERROR_MSG = "RabbitMQ error while {}: {!r}"
CLOSE_ERROR_MSG = "Error closing RabbitMQ resources: {!r}"
ALREADY_CONSUMING_MSG = "Already consuming messages. Stop consuming before starting again."


class _MessageMiddlewareRabbitMQBase:

    def __init__(self, host):
        self._connection = None
        self._channel = None
        self._is_consuming = False
        action = f"connecting to {host}"
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
            self._declare()
        except AMQPConnectionError as e:
            self._close_quietly()
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            self._close_quietly()
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e

    # Cada subclase declara sus colas/exchanges sobre self._channel
    def _declare(self):
        raise NotImplementedError

    # Adapta la firma de callback de pika a la de la interfaz (message, ack, nack)
    def _wrap_callback(self, on_message_callback):
        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return _internal_callback

    def _consume(self, queue_name, on_message_callback):
        if self._is_consuming:
            raise MessageMiddlewareMessageError(ALREADY_CONSUMING_MSG)
        action = f"consuming from {queue_name}"
        try:
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._wrap_callback(on_message_callback),
                auto_ack=False,
            )
            self._is_consuming = True
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e
        finally:
            self._is_consuming = False

    def stop_consuming(self):
        if not self._is_consuming:
            return
        action = "stopping consumption"
        try:
            self._channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e
        self._is_consuming = False

    def stop_consuming_threadsafe(self):
        if self._connection is None or not self._connection.is_open:
            return
        action = "scheduling stop consumption"
        try:
            self._connection.add_callback_threadsafe(self.stop_consuming)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e

    def close(self):
        # Primero el canal y después la conexión. Si falla el canal, igual se
        # intenta cerrar la conexión para no dejarla abierta (antes, podia saltar 
        # al except y la conexión quedaba sin cerrarse)
        channel_error = self._close_resource(self._channel)
        connection_error = self._close_resource(self._connection)
        error = channel_error or connection_error
        if error:
            raise MessageMiddlewareCloseError(CLOSE_ERROR_MSG.format(error)) from error

    # Cierra sin propagar errores de pika. Se usa cuando ya hay una excepción en
    # curso (en __init__) para no taparla con un error del cierre
    def _close_quietly(self):
        for error in (self._close_resource(self._channel), self._close_resource(self._connection)):
            if error:
                logging.warning(f"Ignoring error while releasing RabbitMQ resources: {error!r}")

    # Cierra un canal o una conexión y devuelve el error en vez de elevarlo
    def _close_resource(self, resource):
        try:
            if resource is not None and resource.is_open:
                resource.close()
        except AMQPError as e:
            return e
        return None


class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        super().__init__(host)

    def _declare(self):
        self._channel.queue_declare(queue=self._queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        self._consume(self._queue_name, on_message_callback)

    def send(self, message):
        action = f"sending to queue {self._queue_name}"
        try:
            self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e


class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        super().__init__(host)

    def _declare(self):
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type='direct', durable=True)

    def start_consuming(self, on_message_callback):
        action = f"consuming from exchange {self._exchange_name}"
        try:
            result = self._channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            for routing_key in self._routing_keys:
                self._channel.queue_bind(exchange=self._exchange_name, queue=queue_name, routing_key=routing_key)
            self._consume(queue_name, on_message_callback)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e

    def send(self, message):
        action = f"sending to exchange {self._exchange_name}"
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(exchange=self._exchange_name, routing_key=routing_key, body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e
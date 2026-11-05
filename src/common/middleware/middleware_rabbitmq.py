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

"""
Default configuration values for RabbitMQ connections and operations.
- DEFAULT_PREFETCH_COUNT: The default number of messages to prefetch from the queue.
- DEFAULT_HEARTBEAT: The default heartbeat interval for the RabbitMQ connection.
- DEFAULT_BLOCKED_CONNECTION_TIMEOUT: The default timeout for blocked connections.
Error message templates for various RabbitMQ-related errors.
- DISCONNECTED_MSG: Error message for lost connection to RabbitMQ.
- RABBITMQ_ERROR_MSG: Error message for RabbitMQ errors during operations.
- CLOSE_ERROR_MSG: Error message for errors encountered while closing RabbitMQ resources.
- ALREADY_CONSUMING_MSG: Error message for attempting to start consuming messages when already consuming
"""

DEFAULT_PREFETCH_COUNT = 10
DEFAULT_HEARTBEAT = 120
DEFAULT_BLOCKED_CONNECTION_TIMEOUT = 300

DISCONNECTED_MSG = "Lost connection to RabbitMQ while {}: {!r}"
RABBITMQ_ERROR_MSG = "RabbitMQ error while {}: {!r}"
CLOSE_ERROR_MSG = "Error closing RabbitMQ resources: {!r}"
ALREADY_CONSUMING_MSG = "Already consuming messages. Stop consuming before starting again."


class _MessageMiddlewareRabbitMQBase:
    """
    Base class for RabbitMQ-based message middleware implementations. This class provides common functionality for connecting to RabbitMQ, declaring queues and exchanges, consuming messages, and sending messages. It handles connection errors and provides methods for closing resources.
    """
    def __init__(self, host, prefetch_count):
        self._prefetch_count = prefetch_count
        self._connection = None
        self._channel = None
        self._is_consuming = False
        action = f"connecting to {host}"
        try:
            params = pika.ConnectionParameters(
                host=host,
                heartbeat=DEFAULT_HEARTBEAT,
                blocked_connection_timeout=DEFAULT_BLOCKED_CONNECTION_TIMEOUT
            )
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self._declare()
        except AMQPConnectionError as e:
            self._close_quietly()
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            self._close_quietly()
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e

    def _declare(self):
        """
        Abstract method to declare queues and exchanges specific to the subclass. This method should be implemented by subclasses to set up the necessary RabbitMQ resources.
        """
        raise NotImplementedError

    def _wrap_callback(self, on_message_callback):
        """
        Wraps the callback function provided by the user to adapt it to the signature expected by the Pika library. The wrapped callback will receive the message body and provide acknowledgment and negative acknowledgment functions.
        """
        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return _internal_callback

    def _consume(self, queue_name, on_message_callback):
        """
        Starts consuming messages from the specified queue. The `on_message_callback` function is called for each message received. This method handles connection errors and ensures that only one consuming operation is active at a time.
        """
        if self._is_consuming:
            raise MessageMiddlewareMessageError(ALREADY_CONSUMING_MSG)
        action = f"consuming from {queue_name}"
        try:
            self._channel.basic_qos(prefetch_count=self._prefetch_count)
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
        """
        Stops consuming messages from the queue. This method can be called from a different thread than the one that started consuming. It handles connection errors and ensures that the consuming operation is properly stopped.
        """
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
        """
        Stops consuming messages from the queue in a thread-safe manner. This method can be called from a different thread than the one that started consuming. It schedules the stop operation to be executed in the context of the connection's event loop.
        """
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
        """
        Closes the underlying RabbitMQ resources, including the channel and connection. This method ensures that both the channel and connection are closed properly, even if an error occurs while closing one of them. If any errors occur during the closure, a MessageMiddlewareCloseError is raised with details about the error.
        """
        channel_error = self._close_resource(self._channel)
        connection_error = self._close_resource(self._connection)
        error = channel_error or connection_error
        if error:
            raise MessageMiddlewareCloseError(CLOSE_ERROR_MSG.format(error)) from error

    def _close_quietly(self):
        """
        Attempts to close the underlying RabbitMQ resources (channel and connection) without propagating any exceptions that may occur during the closure. This method is used in scenarios where an error has already occurred to avoid masking the original exception with a closure error.
        """
        for error in (self._close_resource(self._channel), self._close_resource(self._connection)):
            if error:
                logging.warning(f"Ignoring error while releasing RabbitMQ resources: {error!r}")

    def _close_resource(self, resource):
        """
        Attempts to close a given RabbitMQ resource (channel or connection) and returns any exception that occurs during the closure. If the resource is already closed or None, it does nothing and returns None. This method is used to safely close resources without raising exceptions.
        """
        try:
            if resource is not None and resource.is_open:
                resource.close()
        except AMQPError as e:
            return e
        return None

class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name, prefetch_count=DEFAULT_PREFETCH_COUNT):
        """
        Initializes the MessageMiddlewareQueueRabbitMQ with the specified host, queue name, and prefetch count. It sets up a queue for sending and receiving messages. The `prefetch_count` parameter controls how many messages can be prefetched from the queue.
        """
        self._queue_name = queue_name
        super().__init__(host, prefetch_count)

    def _declare(self):
        """
        Declares the queue with the specified name. This method is called during initialization to set up the necessary RabbitMQ resources for the queue.
        """
        self._channel.queue_declare(queue=self._queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        """
        Starts consuming messages from the queue. The `on_message_callback` function is called for each message received. This method handles connection errors and ensures that only one consuming operation is active at a time.
        """
        self._consume(self._queue_name, on_message_callback)

    def send(self, message):
        """
        Sends a message to the queue. The message is published to the specified queue name. This method handles connection errors and ensures that the message is sent successfully.
        """
        action = f"sending to queue {self._queue_name}"
        try:
            self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e

        
class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys, queue_name=None, prefetch_count=DEFAULT_PREFETCH_COUNT):
        """
        Initializes the MessageMiddlewareExchangeRabbitMQ with the specified host, exchange name, routing keys, and optional queue name. If a queue name is provided, the queue is declared and bound to the exchange during initialization. If no queue name is provided, an anonymous queue will be created when start_consuming is called for the first time. The `prefetch_count` parameter controls how many messages can be prefetched from the queue.
        """
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._queue_name = queue_name
        super().__init__(host, prefetch_count)

    def _declare(self):
        """
        Declares the exchange and, if a queue name is provided, declares and binds the queue to the exchange. This method is called during initialization to set up the necessary RabbitMQ resources.
        """
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type='direct', durable=True)
        if self._queue_name is not None:
            self._declare_named_queue(self._queue_name, self._routing_keys)

    def _bind_queue(self, queue_name, routing_keys):
        """
        Binds the specified queue to the exchange with the given routing keys. This method is used to set up the necessary bindings for message routing in RabbitMQ.
        """
        for routing_key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name, queue=queue_name, routing_key=routing_key)

    def _declare_named_queue(self, queue_name, routing_keys):
        """
        Declares a named queue and binds it to the exchange with the specified routing keys. This method is used to set up a queue that can receive messages sent to the exchange with the given routing keys.
        """
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._bind_queue(queue_name, routing_keys)

    def declare_queue(self, queue_name, routing_keys):
        """
        Declares a named queue and binds it to the exchange with the specified routing keys. This method is used by the sender to create consumer queues in advance, ensuring that messages published to the exchange are stored even if the consumer has not started yet.
        """
        action = f"declaring queue {queue_name} on exchange {self._exchange_name}"
        try:
            self._declare_named_queue(queue_name, routing_keys)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e

    def _declare_anonymous_queue(self):
        """
        Declares an anonymous queue and binds it to the exchange with the specified routing keys. This method is used when start_consuming is called for the first time and no queue name was provided during initialization. The anonymous queue will be automatically deleted when the connection is closed.
        """
        action = f"binding a queue to exchange {self._exchange_name}"
        try:
            result = self._channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            self._bind_queue(queue_name, self._routing_keys)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e
        self._queue_name = queue_name

    def start_consuming(self, on_message_callback):
        """
        Starts consuming messages from the exchange. If no queue name was provided during initialization, an anonymous queue will be declared and bound to the exchange. This method handles connection errors and ensures that only one consuming operation is active at a time.
        """
        if self._queue_name is None:
            self._declare_anonymous_queue()
        self._consume(self._queue_name, on_message_callback)

    def send(self, message):
        """
        Publishes a message to all routing keys associated with the exchange.
        """
        for routing_key in self._routing_keys:
            self.send_to(routing_key, message)
            
    def send_to(self, routing_key, message):
        """
        Publishes a message to the specified routing key on the exchange. This method allows sending messages to specific routing keys, enabling targeted message delivery to consumers that are bound to those keys.
        """
        action = f"sending to exchange {self._exchange_name} with routing key {routing_key}"
        try:
            self._channel.basic_publish(exchange=self._exchange_name, routing_key=routing_key, body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTED_MSG.format(action, e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(RABBITMQ_ERROR_MSG.format(action, e)) from e
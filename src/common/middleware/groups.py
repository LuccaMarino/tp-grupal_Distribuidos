import logging
import zlib

from .middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)
from .middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, DEFAULT_PREFETCH_COUNT

"""
Each group is a direct exchange with the name of the group, and each member has a queue named "<group>.<id>" bound to the routing key of the same name. Any sender can calculate the queue of each member knowing the name and size of the group, and each member can calculate its own queue using its id.
This design allows for efficient message distribution among group members, enabling both round-robin and partitioned message sending strategies.
"""

def _member_queue_name(group_name, member_id):
    """
    Constructs the queue name for a specific member of a group. The queue name is formed by concatenating the group name and the member ID, separated by a dot.
    """
    return f"{group_name}.{member_id}"

class GroupSender:
    """
    A sender that distributes messages to members of a group. It supports round-robin sending, partitioned sending based on keys, and broadcasting to all members. Each member has its own queue, and the sender can calculate which queue to send messages to based on the group name and member ID.
    """
    def __init__(self, host, group_name, group_size):
        """
        Initializes the GroupSender with the specified host, group name, and group size. It declares queues for all members of the group to ensure that messages are not lost even if some members have not started yet.
        """
        self._group_name = group_name
        self._group_size = group_size
        self._queue_names = []
        for i in range(group_size):
            self._queue_names.append(_member_queue_name(group_name, i))
        self._next_member = 0
        self._exchange = MessageMiddlewareExchangeRabbitMQ(host, group_name, self._queue_names)
        try:
            for queue_name in self._queue_names:
                self._exchange.declare_queue(queue_name, [queue_name])
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            self._close_quietly()
            raise

    def send(self, message):
        """
        Sends a message to the next member in the group using a round-robin strategy. Each member receives messages from this sender in order, ensuring that any subsequent broadcast (e.g., EOF) will be received after the data messages.
        """
        self._send_to_member(self._next_member, message)
        self._next_member = (self._next_member + 1) % self._group_size

    def send_partitioned(self, items, build_message):
        """
        Distributes the entries in `items` (a dictionary mapping keys to values) among the group members based on their keys. Each member receives a single message containing its assigned entries. The same key will always be sent to the same member, regardless of which sender sends it. The `build_message` function is used to construct the message bytes for each member's entries.
        """
        items_by_member = {}
        for key, value in items.items():
            member_id = self._member_for(key)
            if member_id not in items_by_member:
                items_by_member[member_id] = {}
            items_by_member[member_id][key] = value
        for member_id, member_items in items_by_member.items():
            self._send_to_member(member_id, build_message(member_items))

    def broadcast(self, message):
        """
        Sends a message to all members of the group. This is typically used for as EOF (End of File) notifications.
        """
        self._exchange.send(message)
        
    def close(self):
        """
        Closes the underlying exchange, releasing any resources associated with it.
        """
        self._exchange.close()

    def _member_for(self, key):
        """
        Determines the member ID for a given key based on its hash value. The hash is computed using the CRC32 algorithm, and the result is mapped to a member ID within the range of available group members.
        """
        return zlib.crc32(str(key).encode("utf-8")) % self._group_size

    def _send_to_member(self, member_id, message):
        """
        Sends a message to a specific member of the group identified by `member_id`. The message is sent to the queue corresponding to that member.
        """
        self._exchange.send_to(self._queue_names[member_id], message)

    def _close_quietly(self):
        """
        Attempts to close the underlying exchange, ignoring any exceptions that may occur during the process. This is used to ensure that resources are released even if an error occurs during closure.
        """
        try:
            self._exchange.close()
        except MessageMiddlewareCloseError as e:
            logging.warning(f"Ignoring error while closing group sender {self._group_name}: {e!r}")


class GroupMember:
    """
    A member of a group that consumes messages from its own queue. Each member has a unique ID and listens for messages sent to the group. The member can start and stop consuming messages, and it can also be closed to release resources.
    """
    def __init__(self, host, group_name, member_id, prefetch_count=DEFAULT_PREFETCH_COUNT):
        """
        Initializes the GroupMember with the specified host, group name, and member ID. It sets up a queue for the member to receive messages sent to the group. The `prefetch_count` parameter controls how many messages can be prefetched from the queue.
        """
        queue_name = _member_queue_name(group_name, member_id)
        self._exchange = MessageMiddlewareExchangeRabbitMQ(
            host, group_name, [queue_name], queue_name=queue_name, prefetch_count=prefetch_count
        )

    def start_consuming(self, on_message_callback):
        """
        Starts consuming messages from the member's queue. The `on_message_callback` function is called for each message received.
        """
        self._exchange.start_consuming(on_message_callback)

    def stop_consuming(self):
        """
        Stops consuming messages from the member's queue.
        """
        self._exchange.stop_consuming()

    def stop_consuming_threadsafe(self):
        """
        Stops consuming messages from the member's queue in a thread-safe manner.
        """
        self._exchange.stop_consuming_threadsafe()

    def close(self):
        """
        Closes the underlying exchange, releasing any resources associated with it.
        """
        self._exchange.close()
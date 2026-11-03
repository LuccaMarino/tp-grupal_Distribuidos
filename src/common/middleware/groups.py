import logging
import zlib

from .middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)
from .middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, DEFAULT_PREFETCH_COUNT

# Cada grupo es un exchange direct con el nombre del grupo, y cada miembro tiene una cola
# con nombre "<grupo>.<id>" bindeada a la routing key del mismo nombre. Cualquier emisor 
# puede calcular la cola de cada miembro sabiendo el nombre y el tamaño del grupo, y cada 
# miembro la suya con su id


def _member_queue_name(group_name, member_id):
    return f"{group_name}.{member_id}"


# Lado emisor
# Le envía mensajes a los miembros de un grupo de tamaño group_size.
# Al construirse declara las colas de todos los miembros, para que lo que se envíe
# quede guardado aunque algún miembro todavía no haya arrancado y no se pierda
class GroupSender:

    def __init__(self, host, group_name, group_size):
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

    # Envia y hace round-robin entre los miembros. Cada miembro recibe lo de este emisor
    # en orden, así que un broadcast posterior (EOF) le llega detrás de los datos
    def send(self, message):
        self._send_to_member(self._next_member, message)
        self._next_member = (self._next_member + 1) % self._group_size

    # Particionado: reparte las entradas de items (key -> valor) entre los miembros según
    # su key, y le envía a cada miembro un solo mensaje con sus entradas. Una misma key va
    # siempre al mismo miembro, sin importar qué emisor la envíe.
    # build_message recibe el dict de un miembro y devuelve los bytes a enviar
    def send_partitioned(self, items, build_message):
        items_by_member = {}
        for key, value in items.items():
            member_id = self._member_for(key)
            if member_id not in items_by_member:
                items_by_member[member_id] = {}
            items_by_member[member_id][key] = value
        for member_id, member_items in items_by_member.items():
            self._send_to_member(member_id, build_message(member_items))

    # Broadcast a todos los miembros del grupo
    def broadcast(self, message):
        self._exchange.send(message)
        
    # Atiende la conexión sin enviar nada, para que no se cierre por inactividad.
    # La usa un GroupMember que recibe a este emisor en start_consuming(..., keep_alive)
    def process_pending_events(self):
        self._exchange.process_pending_events()
        
    def close(self):
        self._exchange.close()

    # Devuelve el id del miembro al que le corresponde la key. Se usa crc32 porque 
    # es un hash deterministico
    def _member_for(self, key):
        return zlib.crc32(str(key).encode("utf-8")) % self._group_size

    def _send_to_member(self, member_id, message):
        self._exchange.send_to(self._queue_names[member_id], message)

    def _close_quietly(self):
        try:
            self._exchange.close()
        except MessageMiddlewareCloseError as e:
            logging.warning(f"Ignoring error while closing group sender {self._group_name}: {e!r}")


# Lado receptor
# El miembro member_id de un grupo consume de su propia cola
class GroupMember:

    def __init__(self, host, group_name, member_id, prefetch_count=DEFAULT_PREFETCH_COUNT):
        queue_name = _member_queue_name(group_name, member_id)
        self._exchange = MessageMiddlewareExchangeRabbitMQ(
            host, group_name, [queue_name], queue_name=queue_name, prefetch_count=prefetch_count
        )

    # keep_alive son emisores del nodo que pueden pasar mucho tiempo sin enviar. 
    # Mientras se consume se mantienen sus conexiones vivas
    def start_consuming(self, on_message_callback, keep_alive=()):
        self._exchange.start_consuming(on_message_callback, keep_alive)

    def stop_consuming(self):
        self._exchange.stop_consuming()

    def stop_consuming_threadsafe(self):
        self._exchange.stop_consuming_threadsafe()

    def close(self):
        self._exchange.close()
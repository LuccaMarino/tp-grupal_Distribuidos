UINT32_SIZE = 4
UINT64_SIZE = 8

def serialize_uint32(u: int) -> bytes:
    """
    Serialize a 32-bit integer to bytes in big-endian order.
    """
    return u.to_bytes(UINT32_SIZE, "big")

def deserialize_uint32(b: bytes) -> int:
    """
    Deserialize a 32-bit integer from bytes in big-endian order.
    """
    return int.from_bytes(b, byteorder="big", signed=False)

def serialize_uint64(u: int) -> bytes:
    """
    Serialize a 64-bit integer to bytes in big-endian order.
    """
    return u.to_bytes(UINT64_SIZE, "big")

def deserialize_uint64(b: bytes) -> int:
    """
    Deserialize a 64-bit integer from bytes in big-endian order.
    """
    return int.from_bytes(b, byteorder="big", signed=False)

def serialize_string(s: str) -> bytes:
    """
    Serialize a string to bytes, prefixed with its length as a 4-byte integer.
    """
    encoded = s.encode("utf-8")
    return serialize_uint32(len(encoded)) + encoded

def deserialize_string(socket, recv_sized_func) -> str:
    """
    Deserialize a string from bytes, reading the length as a 4-byte integer first.
    """
    size = deserialize_uint32(recv_sized_func(socket, UINT32_SIZE))
    if size == 0:
        return ""
    return recv_sized_func(socket, size).decode("utf-8")
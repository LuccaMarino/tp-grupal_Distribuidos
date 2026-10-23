UINT32_SIZE = 4
UINT64_SIZE = 8

def serialize_uint32(u: int) -> bytes:
    return u.to_bytes(UINT32_SIZE, "big")

def deserialize_uint32(b: bytes) -> int:
    return int.from_bytes(b, byteorder="big", signed=False)

def serialize_uint64(u: int) -> bytes:
    return u.to_bytes(UINT64_SIZE, "big")

def deserialize_uint64(b: bytes) -> int:
    return int.from_bytes(b, byteorder="big", signed=False)

def serialize_string(s: str) -> bytes:
    encoded = s.encode("utf-8")
    return serialize_uint32(len(encoded)) + encoded

def deserialize_string(socket, recv_sized_func) -> str:
    size = deserialize_uint32(recv_sized_func(socket, UINT32_SIZE))
    if size == 0:
        return ""
    return recv_sized_func(socket, size).decode("utf-8")
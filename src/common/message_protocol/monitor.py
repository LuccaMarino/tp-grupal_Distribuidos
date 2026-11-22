from enum import StrEnum

class BullyMessageType(StrEnum):
    """
    Bully protocol message types.
    - ELECTION: Sent by a monitor to initiate an election.
    - ALIVE: Sent by a monitor to indicate that it is alive and will participate in the election.
    - COORDINATOR: Sent by a monitor to announce that it has won the election and is the new leader.
    """
    ELECTION = "ELECTION"
    ALIVE = "ALIVE"
    COORDINATOR = "COORDINATOR"

def serialize(msg_type: BullyMessageType, sender_id: int) -> bytes:
    """
    Serialize a BullyMessageType and sender ID into a byte string for network transmission.
    Format: `<msg_type>:<sender_id>`
    """
    return f"{msg_type}:{sender_id}".encode("utf-8")

def deserialize(raw: bytes) -> tuple[BullyMessageType, int]:
    """
    Deserialize a byte string into a BullyMessageType and sender ID.
    Expects the format: `<msg_type>:<sender_id>`
    """
    text = raw.decode("utf-8").strip()
    cmd_str, sender_str = text.split(":", 1)
    return BullyMessageType(cmd_str), int(sender_str)
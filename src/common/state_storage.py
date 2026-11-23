import json
import os
import tempfile

class SequentialIdTracker:
    """
    Tracks processed message IDs compactly.
    For sequential integers (1, 2, 3...), compresses prefixes into `max_id` and keeps only out-of-order IDs in `extra_ints`.
    Non-integer IDs (e.g., '15.0') are stored in `extra_strs`.
    """

    def __init__(self, max_id: int = 0, extra_ints: set[int] | None = None, extra_strs: set[str] | None = None):
        self.max_id = max_id
        self.extra_ints = set(extra_ints) if extra_ints else set()
        self.extra_strs = set(extra_strs) if extra_strs else set()

    def contains(self, msg_id):
        if isinstance(msg_id, int) and not isinstance(msg_id, bool):
            return msg_id <= self.max_id or msg_id in self.extra_ints
        return str(msg_id) in self.extra_strs

    def add(self, msg_id):
        if isinstance(msg_id, int) and not isinstance(msg_id, bool):
            if msg_id <= self.max_id or msg_id in self.extra_ints:
                return
            if msg_id == self.max_id + 1:
                self.max_id += 1
                while self.max_id + 1 in self.extra_ints:
                    self.extra_ints.remove(self.max_id + 1)
                    self.max_id += 1
            else:
                self.extra_ints.add(msg_id)
        else:
            self.extra_strs.add(str(msg_id))

    def to_dict(self):
        return {
            "max_id": self.max_id,
            "extra_ints": sorted(self.extra_ints),
            "extra_strs": sorted(self.extra_strs),
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            max_id=int(data.get("max_id", 0)),
            extra_ints=set(data.get("extra_ints", [])),
            extra_strs=set(data.get("extra_strs", [])),
        )

def atomic_write_json(file_path, data):
    """
    Atomically writes JSON data to a file.
    Writes to a temporary file first + fsync + os.replace to ensure atomicity.
    """
    directory = os.path.dirname(file_path) or '.'
    os.makedirs(directory, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=directory, prefix='.tmp', suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as tmp_file:
            json.dump(data, tmp_file, separators=(',', ':'))
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(temp_path, file_path)
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise e

def read_json(file_path, default=None):
    """
    Reads JSON data from a file. Returns default if file doesn't exist or is empty.
    """
    if not os.path.exists(file_path):
        return default
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return default

class ClientStateStorage:
    """
    Client state manager with atomic disk persistence and messaage/EOF deduplication.
    """

    def __init__(self, storage_path: str, default_state_factory):
        self._storage_path = storage_path
        self._default_state_factory = default_state_factory
        self._states: dict[str, dict] = {}
        self._seen_ids: dict[str, SequentialIdTracker] = {}
        self._eofs: dict[str, set[str]] = {}
        self._pending_sends: dict[str, list[dict]] = {}
        self._completed_clients: set[str] = set()

    def _client_path(self, client_id: str) -> str:
        return os.path.join(self._storage_path, f"client_{client_id}.json")

    def _completed_path(self) -> str:
        return os.path.join(self._storage_path, "completed_clients.json")

    def load(self, on_recover=None, send_fn=None) -> None:
        os.makedirs(self._storage_path, exist_ok=True)
        self._completed_clients = set(read_json(self._completed_path(), default=[]))

        for filename in os.listdir(self._storage_path):
            if not (filename.startswith("client_") and filename.endswith(".json")):
                continue
            filepath = os.path.join(self._storage_path, filename)
            data = read_json(filepath)
            if not data:
                continue
            client_id = data["client_id"]
            if client_id in self._completed_clients:
                try:
                    os.remove(filepath)
                except OSError:
                    pass
                continue
            self._states[client_id] = data.get("state", self._default_state_factory())
            self._seen_ids[client_id] = SequentialIdTracker.from_dict(data.get("seen_ids", {}))
            self._eofs[client_id] = set(data.get("eofs", []))
            self._pending_sends[client_id] = data.get("pending_sends", [])
            if on_recover:
                on_recover(client_id, self._states[client_id], self._eofs[client_id], self._pending_sends[client_id])
            if self._pending_sends[client_id] and send_fn:
                self.flush_pending(client_id, send_fn)

    def is_completed(self, client_id: str) -> bool:
        return client_id in self._completed_clients

    def ensure(self, client_id: str) -> None:
        if client_id not in self._states:
            self._states[client_id] = self._default_state_factory()
        if client_id not in self._seen_ids:
            self._seen_ids[client_id] = SequentialIdTracker()
        if client_id not in self._eofs:
            self._eofs[client_id] = set()
        if client_id not in self._pending_sends:
            self._pending_sends[client_id] = []
        return self._states[client_id]

    def get_state(self, client_id: str) -> dict:
        return self.ensure(client_id)

    def get_eofs(self, client_id: str) -> set[str]:
        self.ensure(client_id)
        return self._eofs[client_id]

    def is_duplicate_msg(self, client_id: str, message_id) -> bool:
        self.ensure(client_id)
        return self._seen_ids[client_id].contains(message_id)

    def is_duplicate_eof(self, client_id: str, sender_id: str) -> bool:
        self.ensure(client_id)
        return sender_id in self._eofs[client_id]
    
    def mark_msg_seen(self, client_id: str, message_id) -> None:
        self.ensure(client_id)
        self._seen_ids[client_id].add(message_id)

    def add_eof(self, client_id: str, message_id, sender_id: str) -> int:
        self.ensure(client_id)
        self._seen_ids[client_id].add(message_id)
        self._eofs[client_id].add(sender_id)
        return len(self._eofs[client_id])

    def enqueue_sends(self, client_id: str, items: list[dict]) -> None:
        self.ensure(client_id)
        self._pending_sends[client_id].extend(items)

    def persist(self, client_id: str) -> None:
        self.ensure(client_id)
        payload = {
            "client_id": client_id,
            "state": self._states[client_id],
            "seen_ids": self._seen_ids[client_id].to_dict(),
            "eofs": sorted(self._eofs[client_id]),
            "pending_sends": self._pending_sends[client_id],
        }
        atomic_write_json(self._client_path(client_id), payload)

    def flush_pending(self, client_id: str, send_fn) -> None:
        pending = self._pending_sends.get(client_id, [])
        if not pending:
            return
        client_finished = False
        while pending:
            item = pending[0]
            if send_fn(client_id, item):
                client_finished = True
            pending.pop(0)
            if not client_finished:
                self.persist(client_id)
        if client_finished:
            self.mark_completed(client_id)

    def mark_completed(self, client_id: str) -> None:
        self._completed_clients.add(client_id)
        atomic_write_json(self._completed_path(), sorted(self._completed_clients))
        self._states.pop(client_id, None)
        self._seen_ids.pop(client_id, None)
        self._eofs.pop(client_id, None)
        self._pending_sends.pop(client_id, None)
        path = self._client_path(client_id)
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
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
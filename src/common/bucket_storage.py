import os
import shutil
import zlib

# Cada archivo de bucket se llama bucket_{índice}.{generación}.txt
BUCKET_FILE_PREFIX = "bucket_"
BUCKET_FILE_SUFFIX = ".txt"

class BucketStorage:
    """
    A class that manages the storage of client data in a bucketed file system. Each bucket corresponds to a specific range of words based on their hash values, allowing for efficient updates and retrievals without loading the entire vocabulary into memory.
    Each merge writes the modified buckets to new files tagged with a generation number, leaving the previous files untouched. The storage does not save which generation is current for each bucket: the caller must store `generations()` in the same file where it stores the rest of its state (such as the processed message ids), so that both are updated by a single atomic write. After that write, the caller calls `remove_stale()` to delete the replaced files.
    Attributes:
        _directory (str): The base directory where the client's data is stored.
        _bucket_amount (int): The total number of buckets used for storage.
        _generations (list): The generation of the current file of each bucket, or None if the bucket has no file yet.
        _stale_paths (list): The files replaced by merges since the last call to `remove_stale()`.
    Methods:
        - `bucket_amount()`: Returns the total number of buckets.
        - `generations()`: Returns the generation of the current file of each bucket.
        - `merge(updates, merge_values)`: Merges updates with the stored data, writing the modified buckets to new files. Returns a list of changes made.
        - `remove_stale()`: Deletes the files replaced by previous merges.
        - `read_bucket(bucket_index)`: Reads the contents of a specific bucket and returns it as a dictionary of words and their associated values.
        - `delete()`: Deletes all files associated with the client.
    """
    def __init__(self, base_path, client_id, bucket_amount, generations=None):
        """
        Initializes the BucketStorage instance for a specific client. If `generations` is given (recovered from the caller's state), the storage continues from those files. Any bucket file not referenced by `generations` belongs to a merge that was never committed, and is deleted.
        """
        self._directory = os.path.join(base_path, str(client_id))
        self._bucket_amount = bucket_amount
        if generations is None:
            self._generations = [None] * bucket_amount
        elif len(generations) != bucket_amount:
            raise ValueError(f"Expected {bucket_amount} bucket generations, got {len(generations)}")
        else:
            self._generations = list(generations)
        self._stale_paths = []
        os.makedirs(self._directory, exist_ok=True)
        self._remove_unreferenced_files()

    def bucket_amount(self):
        """
        Returns the total number of buckets used for storage.
        """
        return self._bucket_amount

    def generations(self):
        """
        Returns a copy of the generation of the current file of each bucket (None for buckets without a file). The caller must persist it to be able to recover the storage.
        """
        return list(self._generations)

    def merge(self, updates, merge_values):
        """
        Merges the provided updates with the stored data in the bucketed file system. For existing words, it combines their values using the provided `merge_values` function. For new words, it adds them directly. Only the buckets that have changes are read and written, each one to a new file of the next generation; the replaced files are kept until `remove_stale()` is called. Returns a list of changes made, where each change is represented as `[word, values_before, values_after]` (with values_before being None for new words).
        """
        updates_by_bucket = {}
        for word, values in updates.items():
            bucket_index = self._bucket_for(word)
            if bucket_index not in updates_by_bucket:
                updates_by_bucket[bucket_index] = {}
            updates_by_bucket[bucket_index][word] = values

        generation = self._next_generation()
        changes = []
        for bucket_index, bucket_updates in updates_by_bucket.items():
            stored = self.read_bucket(bucket_index)
            for word, values in bucket_updates.items():
                if word in stored:
                    before = stored[word]
                    after = merge_values(before, values)
                else:
                    before = None
                    after = values
                stored[word] = after
                changes.append([word, before, after])
            self._write_bucket(bucket_index, generation,  stored)
            
            # El archivo anterior se conserva hasta que el estado que apunta al nuevo esté guardado
            previous_generation = self._generations[bucket_index]
            if previous_generation is not None:
                self._stale_paths.append(self._bucket_path(bucket_index, previous_generation))
            self._generations[bucket_index] = generation
        return changes

    def remove_stale(self):
        """
        Deletes the bucket files replaced by the merges since the last call. It must be called only after the caller has persisted the current `generations()`.
        """
        for path in self._stale_paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        self._stale_paths = []

    def read_bucket(self, bucket_index):
        """
        Reads the contents of a specific bucket and returns it as a dictionary where keys are words and values are lists of associated integer values. If the bucket file does not exist, it returns an empty dictionary.
        """
        stored = {}
        generation = self._generations[bucket_index]
        if generation is None:
            return stored
        with open(self._bucket_path(bucket_index, generation), "r") as f:
            for line in f:
                fields = line.rstrip("\n").split("\t")
                values = []
                for field in fields[1:]:
                    values.append(int(field))
                stored[fields[0]] = values
        return stored

    def delete(self):
        """
        Deletes all files associated with the client.
        """
        shutil.rmtree(self._directory, ignore_errors=True)
        self._generations = [None] * self._bucket_amount
        self._stale_paths = []

    def _write_bucket(self, bucket_index, generation, stored):
        """
        Writes the provided stored data to a new file for the specified bucket and generation.
        """
        with open(self._bucket_path(bucket_index, generation), "w") as f:
            for word, values in stored.items():
                fields = [word]
                for value in values:
                    fields.append(str(value))
                f.write("\t".join(fields) + "\n")

    def _next_generation(self):
        """
        Returns the generation number for the next merge: one more than the highest generation in use, or 0 if no bucket has a file yet.
        """
        highest = -1
        for generation in self._generations:
            if generation is not None and generation > highest:
                highest = generation
        return highest + 1

    # Los archivos que no figuran en las generaciones son de un merge que nunca se confirmó
    # (o de una versión anterior del formato), así que su contenido no es válido y se borran
    def _remove_unreferenced_files(self):
        """
        Deletes the bucket files in the client's directory that are not referenced by the current generations. Other files in the directory are left untouched.
        """
        referenced = set()
        for bucket_index in range(self._bucket_amount):
            generation = self._generations[bucket_index]
            if generation is not None:
                referenced.add(os.path.basename(self._bucket_path(bucket_index, generation)))
        for filename in os.listdir(self._directory):
            if filename.startswith(BUCKET_FILE_PREFIX) and filename not in referenced:
                try:
                    os.remove(os.path.join(self._directory, filename))
                except FileNotFoundError:
                    pass

    def _bucket_for(self, word):
        """
        Determines the bucket index for a given word based on its hash value. The hash is computed using the CRC32 algorithm, and the result is mapped to a bucket index within the range of available buckets.
        """
        return (zlib.crc32(word.encode("utf-8")) >> 16) % self.bucket_amount()

    def _bucket_path(self, bucket_index, generation):
        """
        Constructs the file path for a specific bucket and generation. The bucket files are named in the format "bucket_{index}.{generation}.txt" and are stored within the client's directory.
        """
        return os.path.join(self._directory, f"{BUCKET_FILE_PREFIX}{bucket_index}.{generation}{BUCKET_FILE_SUFFIX}")
    
import os
import shutil
import zlib

class BucketStorage:
    """
    A class that manages the storage of client data in a bucketed file system. Each bucket corresponds to a specific range of words based on their hash values, allowing for efficient updates and retrievals without loading the entire vocabulary into memory.
    Attributes:
        _directory (str): The base directory where the client's data is stored.
        _bucket_amount (int): The total number of buckets used for storage.
    Methods:
        - `bucket_amount()`: Returns the total number of buckets.
        - `merge(updates, merge_values)`: Merges updates with the stored data, combining values for existing words and adding new words as needed. Returns a list of changes made.
        - `read_bucket(bucket_index)`: Reads the contents of a specific bucket and returns it as a dictionary of words and their associated values.
        - `delete()`: Deletes all files associated with the client.
    """
    def __init__(self, base_path, client_id, bucket_amount):
        """
        Initializes the BucketStorage instance for a specific client.
        """
        self._directory = os.path.join(base_path, str(client_id))
        self._bucket_amount = bucket_amount
        os.makedirs(self._directory, exist_ok=True)

    def bucket_amount(self):
        """
        Returns the total number of buckets used for storage.
        """
        return self._bucket_amount

    def merge(self, updates, merge_values):
        """
        Merges the provided updates with the stored data in the bucketed file system. For existing words, it combines their values using the provided `merge_values` function. For new words, it adds them directly. Only the buckets that have changes are read and rewritten. Returns a list of changes made, where each change is represented as `[word, values_before, values_after]` (with values_before being None for new words).
        """
        updates_by_bucket = {}
        for word, values in updates.items():
            bucket_index = self._bucket_for(word)
            if bucket_index not in updates_by_bucket:
                updates_by_bucket[bucket_index] = {}
            updates_by_bucket[bucket_index][word] = values

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
            self._write_bucket(bucket_index, stored)
        return changes

    def read_bucket(self, bucket_index):
        """
        Reads the contents of a specific bucket and returns it as a dictionary where keys are words and values are lists of associated integer values. If the bucket file does not exist, it returns an empty dictionary.
        """
        stored = {}
        path = self._bucket_path(bucket_index)
        if not os.path.exists(path):
            return stored
        with open(path, "r") as f:
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

    # Se escribe en un archivo temporal y después se reemplaza el original, así si el
    # nodo se cae en medio de la escritura el bucket anterior queda intacto
    def _write_bucket(self, bucket_index, stored):
        """
        Writes the provided stored data to the specified bucket file. It first writes to a temporary file and then replaces the original bucket file to ensure data integrity in case of failures during the write operation.
        """
        path = self._bucket_path(bucket_index)
        temp_path = path + ".tmp"
        with open(temp_path, "w") as f:
            for word, values in stored.items():
                fields = [word]
                for value in values:
                    fields.append(str(value))
                f.write("\t".join(fields) + "\n")
        os.replace(temp_path, path)

    def _bucket_for(self, word):
        """
        Determines the bucket index for a given word based on its hash value. The hash is computed using the CRC32 algorithm, and the result is mapped to a bucket index within the range of available buckets.
        """
        return (zlib.crc32(word.encode("utf-8")) >> 16) % self.bucket_amount()

    def _bucket_path(self, bucket_index):
        """
        Constructs the file path for a specific bucket based on its index. The bucket files are named in the format "bucket_{index}.txt" and are stored within the client's directory.
        """
        return os.path.join(self._directory, f"bucket_{bucket_index}.txt")
    
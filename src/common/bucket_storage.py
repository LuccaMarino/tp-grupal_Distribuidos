import os
import shutil
import zlib

# Guarda el estado de un cliente ({palabra: lista de enteros}) repartido en
# varios archivos (buckets). Cada palabra cae siempre en el mismo bucket según su
# hash, así que para actualizar o buscar una palabra simplemente se lee su archivo.
# Esto evita tener todo el vocabulario en memoria 
# Formato de cada línea: palabra\tvalor1\tvalor2...
class BucketStorage:

    def __init__(self, base_path, client_id, bucket_amount):
        self._directory = os.path.join(base_path, str(client_id))
        self._bucket_amount = bucket_amount
        os.makedirs(self._directory, exist_ok=True)

    def bucket_amount(self):
        return self._bucket_amount

    # Combina updates ({palabra: valores}) con lo guardado en disco. Las palabras que ya
    # estaban se combinan con merge_values(valores_guardados, valores_nuevos) y las que
    # no estaban se guardan tal cual. Lee y reescribe solo los buckets que tienen cambios.
    # Devuelve una lista de [palabra, valores_antes, valores_despues] (valores_antes es
    # None si la palabra es nueva)
    def merge(self, updates, merge_values):
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

    # Devuelve el contenido de un bucket como {palabra: valores}
    def read_bucket(self, bucket_index):
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

    # Borra todos los archivos del cliente
    def delete(self):
        shutil.rmtree(self._directory, ignore_errors=True)

    # Se escribe en un archivo temporal y después se reemplaza el original, así si el
    # nodo se cae en medio de la escritura el bucket anterior queda intacto
    def _write_bucket(self, bucket_index, stored):
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
        return (zlib.crc32(word.encode("utf-8")) >> 16) % self._bucket_amount

    def _bucket_path(self, bucket_index):
        return os.path.join(self._directory, f"bucket_{bucket_index}.txt")
    
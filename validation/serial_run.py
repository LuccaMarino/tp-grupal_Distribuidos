import collections
import csv
import json
import os
import re
import sys
from datetime import timezone

import pyarrow.parquet as pq

# Reutilizamos los stopwords del word_sum
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "word_sum"))
import stopwords

LAST_OLD_YEAR = 2020
TOP_SIZE = 20
BATCH_SIZE = 10000
BIOGRAPHY_SECTION_NAMES = {"biography", "biographie"}
WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")
COLUMNS = ["name", "abstract", "url", "date_modified", "in_language", "image", "sections"]

# Encabezado de los csv
OUTPUT_FIELDS = {
    "q1": ["url", "date_modified"],
    "q2": ["name", "url", "image"],
    "q3": ["word"],
    "q4": ["word", "count"],
    "q5": ["url", "word_count"],
}

# Orden del top: mayor cantidad primero y, si empatan, alfabético
def _top_order(word_count):
    word, count = word_count
    return (-count, word)

# Palabras del abstract sin stopwords ni números, con repeticiones
def extract_words(abstract):
    if not isinstance(abstract, str):
        return []
    words = []
    for word in WORD_REGEX.findall(abstract.lower()):
        if word not in stopwords.ALL and not NUMBER_REGEX.fullmatch(word):
            words.append(word)
    return words

def has_biography_section(sections):
    if not isinstance(sections, str):
        return False
    for section in json.loads(sections):
        name = section.get("name")
        if name is not None and name.strip().lower() in BIOGRAPHY_SECTION_NAMES:
            return True
    return False

def get_language(article):
    in_language = article["in_language"]
    if not isinstance(in_language, dict):
        return None
    return in_language.get("identifier")


def get_image_url(article):
    image = article["image"]
    if not isinstance(image, dict):
        return None
    return image.get("content_url")

def format_date(date):
    return date.replace(tzinfo=timezone.utc).isoformat()

# Recorre los artículos de todos los parquet de la carpeta de a BATCH_SIZE filas,
# así nunca se carga el dataset completo en memoria
def iter_articles(input_dir):
    for file_name in sorted(os.listdir(input_dir)):
        if not file_name.endswith(".parquet"):
            continue
        parquet_file = pq.ParquetFile(os.path.join(input_dir, file_name))
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=COLUMNS):
            for article in batch.to_pylist():
                yield article


class SerialRun:
    def __init__(self, output_dir, client_name):
        self._output_files = {}
        self._writers = {}
        for query, fields in OUTPUT_FIELDS.items():
            path = os.path.join(output_dir, f"{client_name}_output_{query}.csv")
            output_file = open(path, "w", encoding="utf-8", newline="")
            writer = csv.writer(output_file)
            writer.writerow(fields)
            self._output_files[query] = output_file
            self._writers[query] = writer
        self._counter = collections.Counter()                 # Q4: palabra -> cantidad de abstracts
        self._words_by_language = {"en": set(), "fr": set()}  # Q3
        self._old_total_words = 0                             # Q5: suma de largos hasta 2020
        self._old_articles = 0                                # Q5: abstracts no nulos hasta 2020
        self._new_articles = []                               # Q5: [url, largo] de los posteriores a 2020
        self._rows_by_query = collections.Counter()

    def _write(self, query, row):
        self._writers[query].writerow(row)
        self._rows_by_query[query] += 1

    def process(self, article):
        date = article["date_modified"]
        year = date.year if date is not None else None
        language = get_language(article)
        abstract = article["abstract"]
        words = extract_words(abstract)
        is_old = year is not None and year <= LAST_OLD_YEAR

        # Q1: url y fecha de los artículos en inglés hasta 2020 inclusive
        if is_old and language == "en":
            self._write("q1", [article["url"], format_date(date)])

        # Q2: biografías hasta 2020 inclusive con imagen. La imagen no se descarga:
        # se compara por nombre y url, y la columna queda vacía
        if is_old and get_image_url(article) is not None and has_biography_section(article["sections"]):
            self._write("q2", [article["name"], article["url"], ""])

        # Q3 y Q4: cada palabra cuenta una vez por abstract
        if isinstance(abstract, str):
            unique_words = set(words)
            self._counter.update(unique_words)
            if language in self._words_by_language:
                self._words_by_language[language] |= unique_words

        # Q5: el promedio se calcula al final, así que se guardan los artículos nuevos
        if is_old and isinstance(abstract, str):
            self._old_total_words += len(words)
            self._old_articles += 1
        elif year is not None and year > LAST_OLD_YEAR:
            self._new_articles.append([article["url"], len(words)])

    def finish(self):
        for word in sorted(self._words_by_language["en"] & self._words_by_language["fr"]):
            self._write("q3", [word])
        for word, count in sorted(self._counter.items(), key=_top_order)[:TOP_SIZE]:
            self._write("q4", [word, count])
        average = self._old_total_words / self._old_articles if self._old_articles > 0 else 0
        for url, word_count in self._new_articles:
            if word_count > average:
                self._write("q5", [url, word_count])
        for output_file in self._output_files.values():
            output_file.close()
        return self._rows_by_query


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <input_dir> <output_dir> <client_name>")
        return 1
    input_dir, output_dir, client_name = sys.argv[1:]
    os.makedirs(output_dir, exist_ok=True)
    serial_run = SerialRun(output_dir, client_name)
    for article in iter_articles(input_dir):
        serial_run.process(article)
    rows_by_query = serial_run.finish()
    for query in OUTPUT_FIELDS:
        print(f"{client_name} {query}: {rows_by_query[query]} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
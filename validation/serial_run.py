import collections
import csv
import json
import os
import re
import sys
from datetime import timezone

import pyarrow.parquet as pq

# Reutilize the stopwords from the word_sum module
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "word_sum"))
import stopwords

LAST_OLD_YEAR = 2020
TOP_SIZE = 20
BATCH_SIZE = 10000
BIOGRAPHY_SECTION_NAMES = {"biography", "biographie"}
WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")
COLUMNS = ["name", "abstract", "url", "date_modified", "in_language", "image", "sections"]

# CSV output fields for each query (headers)
OUTPUT_FIELDS = {
    "q1": ["url", "date_modified"],
    "q2": ["name", "url", "image"],
    "q3": ["word"],
    "q4": ["word", "count"],
    "q5": ["url", "word_count"],
}

def _top_order(word_count):
    """
    Returns a tuple that can be used to sort words by total count in descending order, and by word in ascending order for ties.
    """
    word, count = word_count
    return (-count, word)

def extract_words(abstract):
    """
    Extracts words from the abstract, filtering out stopwords and numbers. Returns a list of words in lowercase.
    """
    if not isinstance(abstract, str):
        return []
    words = []
    for word in WORD_REGEX.findall(abstract.lower()):
        if word not in stopwords.ALL and not NUMBER_REGEX.fullmatch(word):
            words.append(word)
    return words

def has_biography_section(sections):
    """
    Checks if the article has a biography section by looking for section names that match known biography section names. Returns True if a biography section is found, otherwise False.
    """
    if not isinstance(sections, str):
        return False
    for section in json.loads(sections):
        name = section.get("name")
        if name is not None and name.strip().lower() in BIOGRAPHY_SECTION_NAMES:
            return True
    return False

def get_language(article):
    """
    Extracts the language identifier from the article's in_language field. Returns the language identifier if present, otherwise None.
    """
    in_language = article["in_language"]
    if not isinstance(in_language, dict):
        return None
    return in_language.get("identifier")


def get_image_url(article):
    """
    Extracts the image URL from the article's image field. Returns the content_url if present, otherwise None.
    """
    image = article["image"]
    if not isinstance(image, dict):
        return None
    return image.get("content_url")

def format_date(date):
    """
    Formats the date as an ISO 8601 string in UTC timezone. Returns the formatted date string.
    """
    return date.replace(tzinfo=timezone.utc).isoformat()

def iter_articles(input_dir):
    """
    Iterates over articles in all Parquet files in the specified input directory, yielding each article as a dictionary. It processes the files in batches to avoid loading the entire dataset into memory at once.
    """
    for file_name in sorted(os.listdir(input_dir)):
        if not file_name.endswith(".parquet"):
            continue
        parquet_file = pq.ParquetFile(os.path.join(input_dir, file_name))
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=COLUMNS):
            for article in batch.to_pylist():
                yield article

class SerialRun:
    """
    Processes articles in a serial manner, applying the required queries and writing the results to CSV files. It accumulates data for each query and handles the final output generation.
    """
    def __init__(self, output_dir, client_name):
        """
        Initializes the SerialRun with the specified output directory and client name. It sets up CSV writers for each query, initializes counters and buffers for processing, and prepares to handle articles.
        """
        self._output_files = {}
        self._writers = {}
        for query, fields in OUTPUT_FIELDS.items():
            path = os.path.join(output_dir, f"{client_name}_output_{query}.csv")
            output_file = open(path, "w", encoding="utf-8", newline="")
            writer = csv.writer(output_file)
            writer.writerow(fields)
            self._output_files[query] = output_file
            self._writers[query] = writer
        self._counter = collections.Counter()                 # Q4: word -> abstract count
        self._words_by_language = {"en": set(), "fr": set()}  # Q3
        self._old_total_words = 0                             # Q5: length sum until 2020
        self._old_articles = 0                                # Q5: abstracts until 2020
        self._new_articles = []                               # Q5: [url, length] of articles after 2020
        self._rows_by_query = collections.Counter()

    def _write(self, query, row):
        """
        Writes a row to the CSV file for the specified query and updates the row count for that query.
        """
        self._writers[query].writerow(row)
        self._rows_by_query[query] += 1

    def process(self, article):
        """
        Processes a single article by applying the required queries (Q1 to Q5) and writing the results to the corresponding CSV files. It extracts relevant information from the article, such as language, date, and abstract, and performs necessary checks for each query.
        """
        date = article["date_modified"]
        year = date.year if date is not None else None
        language = get_language(article)
        abstract = article["abstract"]
        words = extract_words(abstract)
        is_old = year is not None and year <= LAST_OLD_YEAR

        # Q1: url and date of articles in English up to and including 2020
        if is_old and language == "en":
            self._write("q1", [article["url"], format_date(date)])

        # Q2: biographies up to and including 2020 with an image. The image is not downloaded: it is compared by name and url, and the column remains empty
        if is_old and get_image_url(article) is not None and has_biography_section(article["sections"]):
            self._write("q2", [article["name"], article["url"], ""])

        # Q3 and Q4: each word is counted only once per abstract, and the intersection of English and French words is computed for Q3
        if isinstance(abstract, str):
            unique_words = set(words)
            self._counter.update(unique_words)
            if language in self._words_by_language:
                self._words_by_language[language] |= unique_words

        # Q5: the average is calculated at the end, so new articles are stored for later processing
        if is_old and isinstance(abstract, str):
            self._old_total_words += len(words)
            self._old_articles += 1
        elif year is not None and year > LAST_OLD_YEAR:
            self._new_articles.append([article["url"], len(words)])

    def finish(self):
        """
        Finalizes the processing by computing the results for Q3, Q4, and Q5 based on the accumulated data. It writes the final results to the corresponding CSV files and closes all output files. Returns a dictionary with the number of rows written for each query.
        """
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
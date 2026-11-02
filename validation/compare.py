import base64
import binascii
import collections
import csv
import os
import re
import sys

MAX_EXAMPLES = 5
QUERIES = ["q1", "q2", "q3", "q4", "q5"]
IMAGE_ERROR_PREFIX = "ERROR:"
EXPECTED_FILE_REGEX = re.compile(r"(client_\d+)_output_q1\.csv")

# Las imágenes en base64 de Q2 superan el tamaño máximo por defecto de un campo
csv.field_size_limit(sys.maxsize)


def read_rows(path):
    with open(path, encoding="utf-8", newline="") as input_file:
        reader = csv.reader(input_file)
        header = next(reader, [])
        rows = []
        for row in reader:
            rows.append(tuple(row))
    return header, rows


def print_examples(title, rows):
    print(f"    {title}: {len(rows)}")
    for row in rows[:MAX_EXAMPLES]:
        print(f"      {row}")


# Compara sin importar el orden. Se usa un Counter y no un set para detectar
# también los resultados repetidos. Devuelve True si son iguales
def compare_unordered(actual_rows, expected_rows):
    actual = collections.Counter(actual_rows)
    expected = collections.Counter(expected_rows)
    missing = list((expected - actual).elements())
    extra = list((actual - expected).elements())
    if missing:
        print_examples("missing", missing)
    if extra:
        print_examples("unexpected or repeated", extra)
    return not missing and not extra


# El top se compara en orden. Ambos lados se ordenan de mayor a menor cantidad,
# y si empatan, alfabeticamente, así que tienen que dar exactamente la misma lista
def compare_ordered(actual_rows, expected_rows):
    if actual_rows == expected_rows:
        return True
    print_examples("actual", actual_rows)
    print_examples("expected", expected_rows)
    return False


def is_valid_image(image):
    try:
        return len(base64.b64decode(image, validate=True)) > 0
    except (binascii.Error, ValueError):
        return False


# Q2 se compara por nombre y url. De la imagen se verifica que sea base64 válido:
# un error de descarga depende de la red, así que solo se informa como 'warning'
def compare_q2(actual_rows, expected_rows):
    actual_keys = []
    failed_downloads = 0
    invalid_images = []
    for name, url, image in actual_rows:
        actual_keys.append((name, url))
        if image.startswith(IMAGE_ERROR_PREFIX):
            failed_downloads += 1
        elif not is_valid_image(image):
            invalid_images.append((name, url))
    expected_keys = []
    for name, url, _ in expected_rows:
        expected_keys.append((name, url))
    is_equal = compare_unordered(actual_keys, expected_keys)
    if failed_downloads > 0:
        print(f"    warning: {failed_downloads} images could not be downloaded")
    if invalid_images:
        print_examples("invalid images", invalid_images)
    return is_equal and not invalid_images


def compare_query(query, actual_rows, expected_rows):
    if query == "q2":
        return compare_q2(actual_rows, expected_rows)
    if query == "q4":
        return compare_ordered(actual_rows, expected_rows)
    return compare_unordered(actual_rows, expected_rows)


def compare_client(client_name, output_dir, expected_dir):
    print(client_name)
    all_equal = True
    for query in QUERIES:
        file_name = f"{client_name}_output_{query}.csv"
        output_path = os.path.join(output_dir, file_name)
        if not os.path.exists(output_path):
            print(f"  {query}: FAIL (missing file {output_path})")
            all_equal = False
            continue
        actual_header, actual_rows = read_rows(output_path)
        expected_header, expected_rows = read_rows(os.path.join(expected_dir, file_name))
        is_equal = actual_header == expected_header
        if not is_equal:
            print(f"    different header: {actual_header} != {expected_header}")
        is_equal = compare_query(query, actual_rows, expected_rows) and is_equal
        print(f"  {query}: {'OK' if is_equal else 'FAIL'} ({len(actual_rows)} rows, {len(expected_rows)} expected)")
        all_equal = all_equal and is_equal
    return all_equal


def find_clients(expected_dir):
    clients = []
    for file_name in sorted(os.listdir(expected_dir)):
        match = EXPECTED_FILE_REGEX.fullmatch(file_name)
        if match:
            clients.append(match.group(1))
    return clients


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <output_dir> <expected_dir>")
        return 1
    output_dir, expected_dir = sys.argv[1:]
    clients = find_clients(expected_dir)
    if not clients:
        print(f"No expected files found in {expected_dir}")
        return 1
    all_equal = True
    for client_name in clients:
        all_equal = compare_client(client_name, output_dir, expected_dir) and all_equal
    print("RESULT: OK" if all_equal else "RESULT: FAIL")
    return 0 if all_equal else 1


if __name__ == "__main__":
    sys.exit(main())
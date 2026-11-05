import base64
import binascii
import collections
import csv
import os
import re
import sys

# Colors for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

MAX_EXAMPLES = 5
QUERIES = ["q1", "q2", "q3", "q4", "q5"]
IMAGE_ERROR_PREFIX = "ERROR:"
EXPECTED_FILE_REGEX = re.compile(r"(client_\d+)_output_q1\.csv")

# Images in base64 for Q2 exceed the default maximum field size, so we increase it to the maximum allowed by the system.
csv.field_size_limit(sys.maxsize)

def read_rows(path):
    """
    Reads rows from a CSV file at the specified path and returns the header and a list of rows. Each row is returned as a tuple.
    """
    with open(path, encoding="utf-8", newline="") as input_file:
        reader = csv.reader(input_file)
        header = next(reader, [])
        rows = []
        for row in reader:
            rows.append(tuple(row))
    return header, rows

def print_examples(title, rows):
    """
    Prints a title and a limited number of example rows from the provided list. The title is displayed in red, and the rows are indented for clarity.
    """
    print(f"    {RED}{title}: {len(rows)}{RESET}")
    for row in rows[:MAX_EXAMPLES]:
        print(f"      {row}")

def compare_unordered(actual_rows, expected_rows):
    """
    Compares outputs without considering the order of rows. It uses a Counter to detect repeated results and returns True if the actual and expected rows are equal, otherwise it prints examples of missing or unexpected rows.
    """
    actual = collections.Counter(actual_rows)
    expected = collections.Counter(expected_rows)
    missing = list((expected - actual).elements())
    extra = list((actual - expected).elements())
    if missing:
        print_examples("missing", missing)
    if extra:
        print_examples("unexpected or repeated", extra)
    return not missing and not extra

def compare_ordered(actual_rows, expected_rows):
    """
    Compares outputs considering the order of rows. It checks if the actual and expected rows are equal, and if not, it prints examples of the actual and expected rows for comparison.
    """
    if actual_rows == expected_rows:
        return True
    print_examples("actual", actual_rows)
    print_examples("expected", expected_rows)
    return False

def is_valid_image(image):
    """
    Checks if the provided image string is a valid base64-encoded image. It attempts to decode the image and returns True if successful, otherwise it returns False.
    """
    try:
        return len(base64.b64decode(image, validate=True)) > 0
    except (binascii.Error, ValueError):
        return False

def compare_q2(actual_rows, expected_rows):
    """
    Compares outputs for Q2, which involves biographies with images. It checks if the actual and expected rows match by name and URL, while also validating the base64-encoded images. It returns True if the content matches and there are no invalid images, otherwise it prints warnings and examples of invalid images.
    """
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
        print(f"    {YELLOW}warning: {failed_downloads} images could not be downloaded{RESET}")
    if invalid_images:
        print_examples("invalid images", invalid_images)
    return is_equal and not invalid_images


def compare_query(query, actual_rows, expected_rows):
    """
    Compares the actual and expected rows for a specific query. Depending on the query type, it calls the appropriate comparison function (compare_q2 for Q2, compare_ordered for Q4, and compare_unordered for Q1, Q3 & Q5). It returns True if the content matches, otherwise it prints examples of discrepancies.
    """
    if query == "q2":
        return compare_q2(actual_rows, expected_rows)
    if query == "q4":
        return compare_ordered(actual_rows, expected_rows)
    return compare_unordered(actual_rows, expected_rows)

def compare_client(client_name, output_dir, expected_dir):
    """
    Compares the distributed run output files for a specific client against the expected serial run files. It reads the actual and expected rows for each query, calls the appropriate comparison function, and prints the results. It returns True if all queries match, otherwise it returns False.
    """
    print(f"{BOLD}{CYAN}{client_name}{RESET}")
    all_equal = True
    for query in QUERIES:
        file_name = f"{client_name}_output_{query}.csv"
        output_path = os.path.join(output_dir, file_name)
        if not os.path.exists(output_path):
            print(f"  {query}: {RED}FAIL{RESET} (missing file {output_path})")
            all_equal = False
            continue
        actual_header, actual_rows = read_rows(output_path)
        expected_header, expected_rows = read_rows(os.path.join(expected_dir, file_name))
        is_equal = actual_header == expected_header
        if not is_equal:
            print(f"    {RED}different header: {actual_header} != {expected_header}{RESET}")
        is_equal = compare_query(query, actual_rows, expected_rows) and is_equal
        
        status_str = f"{GREEN}OK{RESET}" if is_equal else f"{RED}FAIL{RESET}"
        content_status = f"{GREEN}content matching{RESET}" if is_equal else f"{RED}content mismatch{RESET}"
        print(f"  {query}: {status_str} ({len(actual_rows)} rows, {len(expected_rows)} expected | {content_status})")
        all_equal = all_equal and is_equal
    return all_equal

def find_clients(expected_dir):
    """
    Finds and returns a list of client names based on the expected output files in the specified directory. It looks for files matching the expected naming pattern and extracts the client names from those files.
    """
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
        print(f"{RED}No expected files found in {expected_dir}{RESET}")
        return 1
    all_equal = True
    for client_name in clients:
        all_equal = compare_client(client_name, output_dir, expected_dir) and all_equal
    
    result_str = f"{GREEN}RESULT: OK{RESET}" if all_equal else f"{RED}RESULT: FAIL{RESET}"
    print(f"\n{BOLD}{result_str}")
    return 0 if all_equal else 1

if __name__ == "__main__":
    sys.exit(main())
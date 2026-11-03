import os
import shutil
import kagglehub

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"

# ==============================================================================
# CONFIGURACIÓN ORIGINAL (5 CLIENTES) - COMENTADA PARA VOLVER ATRÁS
# ==============================================================================
# NUM_CLIENTS = 5
# LANGUAGES_CONFIG = [
#     {"base_path": "enwiki/data/enwiki_namespace_0_", "prefix": "en"},
#     {"base_path": "frwiki/data/frwiki_namespace_0_", "prefix": "fr"}
# ]

# def download_datasets_for_clients():
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#     for client_id in range(NUM_CLIENTS):
#         client_folder = os.path.join(OUTPUT_DIR, f"client_{client_id}")
#         os.makedirs(client_folder, exist_ok=True)
#         for lang in LANGUAGES_CONFIG:
#             kaggle_file_path = f"{lang['base_path']}{client_id:05d}.parquet"
#             dest_filename = f"{lang['prefix']}_{client_id}.parquet"
#             destination_path = os.path.join(client_folder, dest_filename)
#             try:
#                 downloaded_file_path = kagglehub.dataset_download(
#                     KAGGLE_DATASET_BASE_PATH, path=kaggle_file_path
#                 )
#                 shutil.copy(downloaded_file_path, destination_path)
#             except Exception as e:
#                 print(f"Error: {e}")
# ==============================================================================

# ==============================================================================
# NUEVA CONFIGURACIÓN: 10 CLIENTES DISTRIBUYENDO TODO EL DATASET (265 EN / 103 FR)
# ==============================================================================
NUM_CLIENTS = 10
EN_TOTAL_FILES = 265
FR_TOTAL_FILES = 103
OUTPUT_DIR = "datasets"

def get_ranges(total_files, num_clients):
    base = total_files // num_clients
    remainder = total_files % num_clients
    ranges = []
    current = 0
    for i in range(num_clients):
        count = base + (1 if i < remainder else 0)
        ranges.append((current, current + count))
        current += count
    return ranges

def download_datasets_for_clients():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    en_ranges = get_ranges(EN_TOTAL_FILES, NUM_CLIENTS)
    fr_ranges = get_ranges(FR_TOTAL_FILES, NUM_CLIENTS)

    for client_id in range(NUM_CLIENTS):
        client_folder = os.path.join(OUTPUT_DIR, f"client_{client_id}")
        os.makedirs(client_folder, exist_ok=True)
        print(f"--- Downloading files for client_{client_id} ---")

        # Descarga archivos de Inglés
        en_start, en_end = en_ranges[client_id]
        print(f"  [EN] Downloading files from index {en_start:05d} to {en_end-1:05d} ({en_end - en_start} files)...")
        for idx in range(en_start, en_end):
            kaggle_file_path = f"enwiki/data/enwiki_namespace_0_{idx:05d}.parquet"
            dest_filename = f"en_{client_id}_{idx:05d}.parquet"
            destination_path = os.path.join(client_folder, dest_filename)
            try:
                downloaded_file_path = kagglehub.dataset_download(
                    KAGGLE_DATASET_BASE_PATH, path=kaggle_file_path
                )
                shutil.copy(downloaded_file_path, destination_path)
            except Exception as e:
                print(f"  [!] Error saving {kaggle_file_path}: {e}")

        # Descarga archivos de Francés
        fr_start, fr_end = fr_ranges[client_id]
        print(f"  [FR] Downloading files from index {fr_start:05d} to {fr_end-1:05d} ({fr_end - fr_start} files)...")
        for idx in range(fr_start, fr_end):
            kaggle_file_path = f"frwiki/data/frwiki_namespace_0_{idx:05d}.parquet"
            dest_filename = f"fr_{client_id}_{idx:05d}.parquet"
            destination_path = os.path.join(client_folder, dest_filename)
            try:
                downloaded_file_path = kagglehub.dataset_download(
                    KAGGLE_DATASET_BASE_PATH, path=kaggle_file_path
                )
                shutil.copy(downloaded_file_path, destination_path)
            except Exception as e:
                print(f"  [!] Error saving {kaggle_file_path}: {e}")

        print(f"--- Finished setting up client_{client_id}\n")

if __name__ == "__main__":
    download_datasets_for_clients()
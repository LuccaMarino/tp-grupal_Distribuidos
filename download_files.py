import os
import shutil
import kagglehub

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"
NUM_CLIENTS = 5

LANGUAGES_CONFIG = [
    {"base_path": "enwiki/data/enwiki_namespace_0_", "prefix": "en"},
    {"base_path": "frwiki/data/frwiki_namespace_0_", "prefix": "fr"}
]
OUTPUT_DIR = "datasets"

def download_datasets_for_clients():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for client_id in range(NUM_CLIENTS):
        client_folder = os.path.join(OUTPUT_DIR, f"client_{client_id}")
        os.makedirs(client_folder, exist_ok=True)

        print(f"--- Downloading files for client_{client_id} ---")

        for lang in LANGUAGES_CONFIG:
            kaggle_file_path = f"{lang['base_path']}{client_id:05d}.parquet"
            # Nombre de destino estandarizado: ej. en_0.parquet, fr_0.parquet
            dest_filename = f"{lang['prefix']}_{client_id}.parquet"
            destination_path = os.path.join(client_folder, dest_filename)

            try:
                downloaded_file_path = kagglehub.dataset_download(
                    KAGGLE_DATASET_BASE_PATH, path=kaggle_file_path
                )

                shutil.copy(downloaded_file_path, destination_path)
                print(f"  [+] Saved: {destination_path}")

            except Exception as e:
                print(
                    f"  [!] Error saving {kaggle_file_path} with kagglehub: {e}"
                )

        print(f"--- Finished setting up client_{client_id}\n")


if __name__ == "__main__":
    download_datasets_for_clients()
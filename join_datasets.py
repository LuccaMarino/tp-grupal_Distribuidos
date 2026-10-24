import os
import pandas as pd
import kagglehub

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"
NUM_CLIENTS = 5

LANGUAGES_FILE_PATHS = [
    "enwiki/data/enwiki_namespace_0_",
    "frwiki/data/frwiki_namespace_0_"
]

COLUMNS_TO_DROP = [
    'additional_entities', 'date_created', 'description',
    'event', 'identifier', 'infoboxes', 'is_part_of',
    'license', 'main_entity', 'references', 'tables', 'version'
]

OUTPUT_DIR = "datasets"

def process_and_merge_single_client(client_id: int):
    dataframes = []

    for file_base_path in LANGUAGES_FILE_PATHS:
        kaggle_file_path = f"{file_base_path}{client_id:05d}.parquet"

        dataset_path = kagglehub.dataset_download(KAGGLE_DATASET_BASE_PATH, path=kaggle_file_path)
        df = pd.read_parquet(dataset_path)

        # Drop unnecessary columns before handling them to client (ask if this is permitted)
        df = df.drop(columns=COLUMNS_TO_DROP, errors='ignore')

        dataframes.append(df)

    wiki_entry_df = pd.concat(dataframes, ignore_index=True)
    wiki_entry_df = wiki_entry_df.sample(frac=1, random_state=42).reset_index(drop=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"client_{client_id}.parquet")
    wiki_entry_df.to_parquet(output_path, index=False)

if __name__ == "__main__":
    for client_id in range(NUM_CLIENTS):
        process_and_merge_single_client(client_id)
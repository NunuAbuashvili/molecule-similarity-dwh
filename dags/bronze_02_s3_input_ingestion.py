import os

from airflow.sdk import dag, task

from include.s3_input_bronze.config import TARGET_TABLE
from include.s3_input_bronze.ingestion import (
    list_matching_keys,
    fetch_csv_from_s3,
    load_csv_to_postgres,
    record_input_load,
)

DEST_DIR = "/tmp/s3_input_bronze"


@dag(
    schedule=None,
    catchup=False,
    tags=["bronze", "s3"],
)
def bronze_02_s3_input_ingestion():

    @task
    def discover_keys() -> list[str]:
        return list_matching_keys()

    @task
    def ingest_input_file(key: str) -> None:
        local_path = None
        try:
            local_path = fetch_csv_from_s3(key=key, dest_dir=DEST_DIR)
            row_count = load_csv_to_postgres(local_path, TARGET_TABLE, source_key=key)
            record_input_load(key, row_count)
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    ingest_input_file.expand(key=discover_keys())


bronze_02_s3_input_ingestion()

"""
Bronze DAG: ingest input-molecule CSVs from S3 into bronze.input_molecules.
"""

import logging
import os
from datetime import timedelta

from airflow.sdk import dag, task, Asset

from include.s3_input_bronze.config import TARGET_TABLE
from include.s3_input_bronze.ingestion import (
    list_matching_keys,
    fetch_csv_from_s3,
    load_csv_to_postgres,
    record_input_load,
)
from include.notifications.teams import notify_task_failure

logger = logging.getLogger(__name__)

DEST_DIR = "/tmp/s3_input_bronze"


@dag(
    schedule=None,
    catchup=False,
    tags=["bronze", "s3"],
    default_args={"retries": 2, "on_failure_callback": notify_task_failure},
)
def bronze_02_s3_input_ingestion():

    @task
    def discover_keys() -> list[str]:
        return list_matching_keys()

    @task(execution_timeout=timedelta(minutes=15))
    def ingest_input_file(key: str) -> None:
        local_path = None
        try:
            local_path = fetch_csv_from_s3(key=key, dest_dir=DEST_DIR)
            row_count = load_csv_to_postgres(
                local_path,
                TARGET_TABLE,
                source_key=key
            )
            record_input_load(key, row_count)
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    @task(outlets=[Asset("bronze.input_molecules")])
    def finalize_input_ingestion() -> None:
        logger.info("Bronze S3 input ingestion complete.")

    ingested = ingest_input_file.expand(key=discover_keys())
    ingested >> finalize_input_ingestion()


bronze_02_s3_input_ingestion()

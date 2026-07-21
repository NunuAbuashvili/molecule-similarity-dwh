"""
Bronze-layer ingestion DAG: ChEMBL SQLite dump -> Postgres via COPY.

Business logic lives in include/chembl_bronze/ — this file only wires up
the DAG/task graph.
"""
import logging
import sqlite3
from datetime import datetime

import chembl_downloader
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, get_current_context

from include.chembl_bronze.config import (
    CHEMBL_VERSION,
    POSTGRES_CONN_ID,
    TABLE_CONFIGS
)
from include.chembl_bronze.ingestion import (
    get_last_loaded_version,
    record_load,
    stream_table_to_postgres,
)
from include.notifications.teams import notify_task_failure

logger = logging.getLogger(__name__)


@task
def prepare_chembl_sqlite() -> str:
    path_obj = chembl_downloader.download_extract_sqlite(
        version=CHEMBL_VERSION
    )
    return str(path_obj)


@task
def ingest_chembl_table(
    sqlite_path: str,
    table_name: str
) -> int:
    force_reload = get_current_context()["params"].get("force_reload", False)
    config = TABLE_CONFIGS[table_name]
    pg_conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()

    try:
        if (
            get_last_loaded_version(pg_conn, table_name) == CHEMBL_VERSION and
            not force_reload
        ):
            logger.info(
                "Skipping %s: already loaded from ChEMBL v%s. "
                "Trigger with force_reload=true to reload anyway.",
                table_name, CHEMBL_VERSION,
            )
            return 0

        sqlite_conn = sqlite3.connect(sqlite_path)
        try:
            row_count = stream_table_to_postgres(
                sqlite_conn=sqlite_conn,
                pg_conn=pg_conn,
                source_table=table_name,
                target_table=config["target_table"],
                columns=config["columns"],
            )
        finally:
            sqlite_conn.close()

        record_load(pg_conn, table_name, CHEMBL_VERSION, row_count)
        logger.info(
            "Loaded %s rows into %s",
            row_count,
            config["target_table"]
        )
        return row_count
    finally:
        pg_conn.close()


@dag(
    dag_id="bronze_01_chembl_ingestion",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={"retries": 2, "on_failure_callback": notify_task_failure},
    params={"force_reload": False},
    tags=["bronze", "chembl"],
)
def bronze_01_chembl_ingestion():
    sqlite_path = prepare_chembl_sqlite()
    ingest_chembl_table.partial(sqlite_path=sqlite_path).expand(
        table_name=list(TABLE_CONFIGS)
    )


bronze_01_chembl_ingestion()

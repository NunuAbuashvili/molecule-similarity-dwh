import csv
import logging
import os
import re

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.s3_input_bronze.config import (
    AWS_CONN_ID,
    BUCKET_NAME,
    KEY_PATTERN,
    INPUT_PREFIX,
    COLUMNS,
    COLUMN_ALIASES,
    POSTGRES_CONN_ID,
    TARGET_TABLE
)

logger = logging.getLogger(__name__)


def get_s3_hook() -> S3Hook:
    return S3Hook(aws_conn_id=AWS_CONN_ID)


def list_matching_keys(
    bucket: str = BUCKET_NAME,
    prefix: str = INPUT_PREFIX,
) -> list[str]:
    """
    List S3 keys under `prefix` that match KEY_PATTERN.
    """
    hook = get_s3_hook()
    try:
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    except Exception as e:
        logger.error(
            "Failed to list keys from S3 bucket %s: %s",
            bucket, e
        )
        raise

    pattern = re.compile(KEY_PATTERN)
    matched = [key for key in keys if pattern.match(key)]

    logger.info(
        "Found %s files matching '%s' under %s",
        len(matched), KEY_PATTERN, prefix
    )
    return matched


def fetch_csv_from_s3(
    key: str,
    dest_dir: str,
    bucket: str = BUCKET_NAME
) -> str:
    """Download one CSV file from S3 to local disk."""
    os.makedirs(dest_dir, exist_ok=True)
    hook = get_s3_hook()
    local_path = os.path.join(dest_dir, os.path.basename(key))

    try:
        s3_obj = hook.get_key(key=key, bucket_name=bucket)
        if not s3_obj:
            raise FileNotFoundError(f"Key s3://{bucket}/{key} not found.")
        s3_obj.download_file(local_path)
        logger.info(
            "Downloaded s3://%s/%s -> %s",
            bucket, key, local_path
        )
    except Exception as e:
        logger.error(
            "Failed downloading s3://%s/%s: %s",
            bucket, key, e
        )
        raise

    return local_path


def resolve_columns(header: list[str]) -> list[str]:
    """
    Map source column names to canonical bronze names and validate.
    """
    resolved = [
        COLUMN_ALIASES.get(col.strip(), col.strip()) for col in header
    ]
    unknown = set(resolved) - set(COLUMNS)
    if unknown:
        raise ValueError(
            f"Unrecognized column(s) {unknown} in header {header}"
        )
    return resolved


def read_csv_header(csv_path: str) -> list[str]:
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            return next(csv.reader(f))
    except Exception as e:
        logger.error(
            "Failed reading header of %s: %s",
            csv_path, e
        )
        raise


def load_csv_to_postgres(
    csv_path: str,
    table_name: str,
    source_key: str
) -> int:
    """Bulk-load CSV into Postgres."""
    target_columns = resolve_columns(read_csv_header(csv_path))
    col_list = ", ".join(target_columns)
    col_defs = ", ".join(f"{col} text" for col in target_columns)

    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TEMP TABLE staging_input ({col_defs})"
                f" ON COMMIT DROP"
            )

            with open(csv_path, newline="", encoding="utf-8") as f:
                copy_sql = (
                    f"COPY staging_input ({col_list}) "
                    f"FROM STDIN WITH (FORMAT csv, HEADER true)"
                )
                cur.copy_expert(copy_sql, f)

            cur.execute(
                f"""
                INSERT INTO {table_name} ({col_list}, _source_file)
                SELECT {col_list}, %s FROM staging_input
                """,
                (source_key,),
            )
            row_count = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Loaded %s rows from %s into %s",
        row_count, csv_path, table_name
    )
    return row_count


def record_input_load(key: str, row_count: int) -> None:
    """Upsert metadata load logs."""
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.load_log (
                    table_name, version, row_count, loaded_at
                )
                VALUES (%s, %s, %s, now())
                ON CONFLICT (table_name, version)
                DO UPDATE SET row_count = EXCLUDED.row_count,
                              loaded_at = EXCLUDED.loaded_at
                """,
                (TARGET_TABLE, key, row_count),
            )
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Recorded load: %s rows from %s into %s",
        row_count, key, TARGET_TABLE
    )

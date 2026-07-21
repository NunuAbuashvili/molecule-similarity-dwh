"""ChEMBL bronze-ingestion logic"""
import csv
import io
import sqlite3

from include.chembl_bronze.config import BATCH_SIZE


def stream_table_to_postgres(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    source_table: str,
    target_table: str,
    columns: list[str],
    batch_size: int = BATCH_SIZE,
) -> int:
    """
    Bulk-copy one ChEMBL SQLite table into Postgres via COPY.
    """
    col_list = ", ".join(columns)
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute(f"SELECT {col_list} FROM {source_table};")

    total_rows = 0
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(f"TRUNCATE TABLE {target_table};")

        while True:
            rows = sqlite_cursor.fetchmany(batch_size)
            if not rows:
                break

            buffer = io.StringIO()
            csv.writer(buffer).writerows(rows)
            buffer.seek(0)

            pg_cursor.copy_expert(
                f"COPY {target_table} ({col_list}) "
                f"FROM STDIN WITH (FORMAT csv, NULL '')",
                buffer,
            )
            total_rows += len(rows)

    pg_conn.commit()
    return total_rows


def get_last_loaded_version(
    pg_conn,
    table_name: str
) -> str | None:
    """
    Return the ChEMBL version last recorded as loaded for this table, or None.
    """
    with pg_conn.cursor() as cursor:
        cursor.execute(
            "SELECT version FROM meta.load_log WHERE table_name = %s;",
            (table_name,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def record_load(
    pg_conn,
    table_name: str,
    version: str,
    row_count: int
) -> None:
    """Upsert the load metadata row for this table."""
    with pg_conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO meta.load_log (
                table_name, version, loaded_at, row_count
            )
            VALUES (%s, %s, now(), %s)
            ON CONFLICT (table_name, version)
            DO UPDATE SET loaded_at = EXCLUDED.loaded_at,
                          row_count = EXCLUDED.row_count;
            """,
            (table_name, version, row_count),
        )
    pg_conn.commit()

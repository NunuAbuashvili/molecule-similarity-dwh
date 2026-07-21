"""Tests for include/chembl_bronze/ingestion.py"""
import sqlite3

import pytest

from include.chembl_bronze.ingestion import (
    stream_table_to_postgres,
    get_last_loaded_version,
    record_load,
)


@pytest.fixture
def sqlite_conn():
    """
    In-memory SQLite DB seeded with a tiny source table, including a NULL.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE molecule_dictionary "
        "(molregno INT, chembl_id TEXT, pref_name TEXT)"
    )
    conn.executemany(
        "INSERT INTO molecule_dictionary VALUES (?, ?, ?)",
        [
            (1, "CHEMBL1", "ASPIRIN"),
            (2, "CHEMBL2", None),
            (3, "CHEMBL3", "PARACETAMOL"),
        ],
    )
    conn.commit()
    yield conn
    conn.close()


class TestStreamTableToPostgres:
    def test_returns_total_row_count(self, sqlite_conn, pg_conn):
        row_count = stream_table_to_postgres(
            sqlite_conn=sqlite_conn,
            pg_conn=pg_conn,
            source_table="molecule_dictionary",
            target_table="bronze.molecule_dictionary",
            columns=["molregno", "chembl_id", "pref_name"],
            batch_size=10,
        )
        assert row_count == 3

    def test_batches_rows_according_to_batch_size(self, sqlite_conn, pg_conn):
        stream_table_to_postgres(
            sqlite_conn=sqlite_conn,
            pg_conn=pg_conn,
            source_table="molecule_dictionary",
            target_table="bronze.molecule_dictionary",
            columns=["molregno", "chembl_id", "pref_name"],
            batch_size=2,
        )
        cursor = pg_conn.cursor.return_value.__enter__.return_value
        assert cursor.copy_expert.call_count == 2

    def test_copy_expert_uses_correct_columns_and_null_marker(
            self,
            sqlite_conn,
            pg_conn
    ):
        stream_table_to_postgres(
            sqlite_conn=sqlite_conn,
            pg_conn=pg_conn,
            source_table="molecule_dictionary",
            target_table="bronze.molecule_dictionary",
            columns=["molregno", "chembl_id", "pref_name"],
            batch_size=10,
        )
        cursor = pg_conn.cursor.return_value.__enter__.return_value
        copy_sql = cursor.copy_expert.call_args[0][0]
        assert (
            "bronze.molecule_dictionary "
            "(molregno, chembl_id, pref_name)" in copy_sql
        )
        assert "NULL ''" in copy_sql

    def test_none_values_serialize_to_empty_csv_field(
            self,
            sqlite_conn,
            pg_conn
    ):
        stream_table_to_postgres(
            sqlite_conn=sqlite_conn,
            pg_conn=pg_conn,
            source_table="molecule_dictionary",
            target_table="bronze.molecule_dictionary",
            columns=["molregno", "chembl_id", "pref_name"],
            batch_size=10,
        )
        cursor = pg_conn.cursor.return_value.__enter__.return_value
        buffer = cursor.copy_expert.call_args[0][1]
        csv_text = buffer.getvalue()
        assert "2,CHEMBL2,\r\n" in csv_text
        assert "None" not in csv_text

    def test_commits_after_load(self, sqlite_conn, pg_conn):
        stream_table_to_postgres(
            sqlite_conn=sqlite_conn,
            pg_conn=pg_conn,
            source_table="molecule_dictionary",
            target_table="bronze.molecule_dictionary",
            columns=["molregno", "chembl_id", "pref_name"],
            batch_size=10,
        )
        pg_conn.commit.assert_called_once()

    def test_empty_source_table_returns_zero(self, pg_conn):
        empty_conn = sqlite3.connect(":memory:")
        empty_conn.execute("CREATE TABLE empty_table (id INT)")
        empty_conn.commit()

        row_count = stream_table_to_postgres(
            sqlite_conn=empty_conn,
            pg_conn=pg_conn,
            source_table="empty_table",
            target_table="bronze.empty_table",
            columns=["id"],
            batch_size=10,
        )

        assert row_count == 0
        cursor = pg_conn.cursor.return_value.__enter__.return_value
        cursor.copy_expert.assert_not_called()
        empty_conn.close()


class TestGetLastLoadedVersion:
    def test_returns_version_when_row_exists(self, pg_conn):
        cursor = pg_conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("chembl_35",)

        version = get_last_loaded_version(
            pg_conn,
            "bronze.molecule_dictionary"
        )

        assert version == "chembl_35"
        cursor.execute.assert_called_once_with(
            "SELECT version FROM meta.load_log WHERE table_name = %s;",
            ("bronze.molecule_dictionary",),
        )

    def test_returns_none_when_no_row(self, pg_conn):
        cursor = pg_conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None

        assert get_last_loaded_version(
            pg_conn,
            "bronze.molecule_dictionary"
        ) is None


class TestRecordLoad:
    def test_executes_upsert_with_correct_params(self, pg_conn):
        record_load(pg_conn, "bronze.molecule_dictionary", "chembl_35", 12345)

        cursor = pg_conn.cursor.return_value.__enter__.return_value
        sql, params = cursor.execute.call_args[0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert "ON CONFLICT (table_name, version)" in sql
        assert params == ("bronze.molecule_dictionary", "chembl_35", 12345)

    def test_commits(self, pg_conn):
        record_load(pg_conn, "bronze.molecule_dictionary", "chembl_35", 12345)
        pg_conn.commit.assert_called_once()

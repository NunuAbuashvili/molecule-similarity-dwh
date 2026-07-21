"""Tests for include/molecule_silver/validation.py"""
from unittest.mock import MagicMock

import pytest

from include.molecule_silver import validation


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    monkeypatch.setattr(
        validation,
        "STRUCTURES_TABLE",
        "bronze.compound_structures"
    )
    monkeypatch.setattr(
        validation,
        "DICTIONARY_TABLE",
        "bronze.molecule_dictionary"
    )
    monkeypatch.setattr(validation, "TARGET_TABLE", "silver.molecule")
    monkeypatch.setattr(validation, "CHEMBL_VERSION", "chembl_35")


@pytest.fixture
def mock_postgres_hook(monkeypatch, pg_conn):
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = pg_conn
    monkeypatch.setattr(validation, "PostgresHook", hook_cls)
    return pg_conn


class TestIsValidSmiles:
    def test_valid_smiles_returns_true(self):
        assert validation.is_valid_smiles("CCO") is True  # ethanol

    def test_invalid_smiles_returns_false(self):
        assert validation.is_valid_smiles("not_a_real_smiles###") is False


class TestFetchChunk:
    def test_executes_correct_query_and_returns_rows(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1, "CHEMBL1", "CCO")]

        result = validation.fetch_chunk(cursor, last_molregno=0, chunk_size=50)

        assert result == [(1, "CHEMBL1", "CCO")]
        sql, params = cursor.execute.call_args[0]
        assert "bronze.compound_structures" in sql
        assert "bronze.molecule_dictionary" in sql
        assert params == (0, 50)


class TestValidateChunk:
    def test_splits_valid_and_invalid_rows(self):
        rows = [
            (1, "CHEMBL1", "CCO"),  # valid
            (2, "CHEMBL2", "not_a_real_smiles###"),  # invalid
            (3, "CHEMBL3", "c1ccccc1"),  # valid (benzene)
        ]

        valid_rows, rejected_count = validation.validate_chunk(rows)

        assert valid_rows == [rows[0], rows[2]]
        assert rejected_count == 1

    def test_all_valid_returns_zero_rejected(self):
        rows = [(1, "CHEMBL1", "CCO"), (2, "CHEMBL2", "c1ccccc1")]
        valid_rows, rejected_count = validation.validate_chunk(rows)
        assert len(valid_rows) == 2
        assert rejected_count == 0

    def test_empty_input_returns_empty_output(self):
        valid_rows, rejected_count = validation.validate_chunk([])
        assert valid_rows == []
        assert rejected_count == 0


class TestLoadChunkToSilver:
    def test_empty_rows_returns_zero_without_touching_cursor(self):
        cursor = MagicMock()
        result = validation.load_chunk_to_silver(cursor, [])
        assert result == 0
        cursor.copy_expert.assert_not_called()

    def test_loads_rows_via_copy_expert(self):
        cursor = MagicMock()
        rows = [(1, "CHEMBL1", "CCO"), (2, "CHEMBL2", "c1ccccc1")]

        result = validation.load_chunk_to_silver(
            cursor,
            rows,
            target_table="silver.molecule"
        )

        assert result == 2
        copy_sql, buffer = cursor.copy_expert.call_args[0]
        assert (
            "COPY silver.molecule "
            "(molregno, chembl_id, canonical_smiles)" in copy_sql
        )
        assert "NULL ''" in copy_sql
        assert "1,CHEMBL1,CCO\r\n" in buffer.getvalue()


class TestGetLastBuiltVersion:
    def test_returns_version_when_present(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("chembl_35",)
        assert validation.get_last_built_version(cursor) == "chembl_35"

    def test_returns_none_when_absent(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        assert validation.get_last_built_version(cursor) is None


class TestRecordBuild:
    def test_executes_upsert_with_correct_params(self):
        cursor = MagicMock()
        validation.record_build(cursor, "chembl_35", 100)

        sql, params = cursor.execute.call_args[0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert "ON CONFLICT (table_name, version)" in sql
        assert params == ("silver.molecule", "chembl_35", 100)


class TestRunValidation:
    def test_skips_when_already_built_and_not_forced(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("chembl_35",)

        validation.run_validation(force_reload=False)

        executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
        assert not any("TRUNCATE" in sql for sql in executed_sql)
        cursor.copy_expert.assert_not_called()

    def test_rebuilds_when_forced_even_if_up_to_date(
        self,
        mock_postgres_hook,
        monkeypatch
    ):
        monkeypatch.setattr(validation, "CHUNK_SIZE", 2)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [(1, "CHEMBL1", "CCO"), (2, "CHEMBL2", "c1ccccc1")],
            [],
        ]

        validation.run_validation(force_reload=True)

        executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
        assert any("TRUNCATE" in sql for sql in executed_sql)

    def test_processes_multiple_chunks_and_records_total(
        self,
        mock_postgres_hook,
        monkeypatch
    ):
        monkeypatch.setattr(validation, "CHUNK_SIZE", 2)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None  # never built before
        cursor.fetchall.side_effect = [
            [(1, "CHEMBL1", "CCO"), (2, "CHEMBL2", "not_a_real_smiles###")],
            [(3, "CHEMBL3", "c1ccccc1")],
        ]

        validation.run_validation(force_reload=False)

        sql, params = cursor.execute.call_args_list[-1][0]
        assert "INSERT " + "INTO meta.load_log" in sql
        # 2 valid rows total (CCO + benzene); the malformed SMILES was rejected
        assert params == ("silver.molecule", "chembl_35", 2)

    def test_closes_connection_even_on_failure(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = RuntimeError("db exploded")

        with pytest.raises(RuntimeError):
            validation.run_validation(force_reload=False)

        mock_postgres_hook.close.assert_called_once()

"""Tests for include/input_molecule_silver/validation.py"""
from unittest.mock import MagicMock

import pytest

from include.input_molecule_silver import validation


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    monkeypatch.setattr(validation, "SOURCE_TABLE", "bronze.input_molecules")
    monkeypatch.setattr(validation, "DICTIONARY_TABLE", "bronze.molecule_dictionary")
    monkeypatch.setattr(validation, "TARGET_TABLE", "silver.input_molecule")


@pytest.fixture
def mock_postgres_hook(monkeypatch, pg_conn):
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = pg_conn
    monkeypatch.setattr(validation, "PostgresHook", hook_cls)
    return pg_conn


class TestBuildOverridesClause:
    def test_empty_overrides_returns_null_placeholder(self, monkeypatch):
        monkeypatch.setattr(validation, "NAME_OVERRIDES", {})

        values_sql, params = validation.build_overrides_clause()

        assert values_sql == "(CAST(NULL AS TEXT), CAST(NULL AS TEXT))"
        assert params == []

    def test_single_override_produces_one_row_and_flat_params(self, monkeypatch):
        monkeypatch.setattr(validation, "NAME_OVERRIDES", {"paracetamol": "acetaminophen"})

        values_sql, params = validation.build_overrides_clause()

        assert values_sql == "(%s, %s)"
        assert params == ["paracetamol", "acetaminophen"]

    def test_multiple_overrides_produce_matching_rows_and_params(self, monkeypatch):
        monkeypatch.setattr(
            validation,
            "NAME_OVERRIDES",
            {"paracetamol": "acetaminophen", "adrenaline": "epinephrine"},
        )

        values_sql, params = validation.build_overrides_clause()

        assert values_sql == "(%s, %s), (%s, %s)"
        assert params == ["paracetamol", "acetaminophen", "adrenaline", "epinephrine"]


class TestValidateAndMatch:
    def _configure_cursor(self, mock_postgres_hook, inserted_count, total_input_count, unmatched_count):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.rowcount = inserted_count
        cursor.fetchone.side_effect = [(total_input_count,), (unmatched_count,)]
        return cursor

    def test_truncates_target_table(self, mock_postgres_hook, monkeypatch):
        monkeypatch.setattr(validation, "NAME_OVERRIDES", {})
        cursor = self._configure_cursor(mock_postgres_hook, 98, 100, 5)

        validation.validate_and_match()

        executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
        assert "TRUNCATE " + "TABLE silver.input_molecule" in executed_sql

    def test_insert_references_correct_tables_and_overrides_params(self, mock_postgres_hook, monkeypatch):
        monkeypatch.setattr(validation, "NAME_OVERRIDES", {"paracetamol": "acetaminophen"})
        cursor = self._configure_cursor(mock_postgres_hook, 98, 100, 5)

        validation.validate_and_match()

        insert_sql, params = cursor.execute.call_args_list[1][0]
        assert "INSERT " + "INTO silver.input_molecule" in insert_sql
        assert "FROM bronze.input_molecules im" in insert_sql
        assert "JOIN bronze.molecule_dictionary md" in insert_sql
        assert params == ["paracetamol", "acetaminophen"]

    def test_commits_and_closes(self, mock_postgres_hook, monkeypatch):
        monkeypatch.setattr(validation, "NAME_OVERRIDES", {})
        self._configure_cursor(mock_postgres_hook, 98, 100, 5)

        validation.validate_and_match()

        mock_postgres_hook.commit.assert_called_once()
        mock_postgres_hook.close.assert_called_once()

    def test_logs_correct_summary_counts(self, mock_postgres_hook, monkeypatch, caplog):
        monkeypatch.setattr(validation, "NAME_OVERRIDES", {})
        self._configure_cursor(mock_postgres_hook, 98, 100, 5)

        with caplog.at_level("INFO"):
            validation.validate_and_match()

        message = caplog.text
        assert "98 of 100 input rows kept" in message
        assert "2 dropped for missing compound_name" in message  # 100 - 98
        assert "5 unmatched" in message

    def test_closes_connection_even_on_failure(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = RuntimeError("db exploded")

        with pytest.raises(RuntimeError):
            validation.validate_and_match()

        mock_postgres_hook.close.assert_called_once()

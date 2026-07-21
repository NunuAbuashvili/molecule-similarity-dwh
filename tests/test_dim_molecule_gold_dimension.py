"""Tests for include/dim_molecule_gold/dimension.py"""
from unittest.mock import MagicMock

import pytest

from include.dim_molecule_gold import dimension


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    monkeypatch.setattr(dimension, "FACT_TABLE", "gold.fact_similarity")
    monkeypatch.setattr(
        dimension,
        "DICTIONARY_TABLE",
        "bronze.molecule_dictionary"
    )
    monkeypatch.setattr(
        dimension,
        "PROPERTIES_TABLE",
        "bronze.compound_properties"
    )
    monkeypatch.setattr(dimension, "TARGET_TABLE", "gold.dim_molecule")
    monkeypatch.setattr(dimension, "CHEMBL_VERSION", "chembl_35")


@pytest.fixture
def mock_postgres_hook(monkeypatch, pg_conn):
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = pg_conn
    monkeypatch.setattr(dimension, "PostgresHook", hook_cls)
    return pg_conn


class TestGetLastBuiltVersion:
    def test_returns_version_when_present(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("chembl_35",)
        assert dimension.get_last_built_version(cursor) == "chembl_35"

    def test_returns_none_when_absent(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        assert dimension.get_last_built_version(cursor) is None


class TestRecordBuild:
    def test_executes_upsert_with_correct_params(self):
        cursor = MagicMock()
        dimension.record_build(cursor, "chembl_35", 42)

        sql, params = cursor.execute.call_args[0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert "ON CONFLICT (table_name, version)" in sql
        assert params == ("gold.dim_molecule", "chembl_35", 42)


class TestBuildDimMolecule:
    def test_skips_when_already_built_and_not_forced(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("chembl_35",)

        dimension.build_dim_molecule(force_reload=False)

        executed_sql = [
            call[0][0] for call in cursor.execute.call_args_list
        ]
        assert not any("TRUNCATE" in sql for sql in executed_sql)
        assert not any("count(*)" in sql for sql in executed_sql)

    def test_raises_when_fact_table_empty(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [None, (0,)]

        with pytest.raises(ValueError, match="ETL aborted"):
            dimension.build_dim_molecule(force_reload=False)

        executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
        assert not any("TRUNCATE" in sql for sql in executed_sql)
        mock_postgres_hook.close.assert_called_once()

    def test_builds_and_records_when_not_up_to_date(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [None, (5,)]
        cursor.rowcount = 5

        dimension.build_dim_molecule(force_reload=False)

        executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
        assert any("TRUNCATE" in sql for sql in executed_sql)

        insert_sql = next(
            sql for sql in executed_sql
            if "INSERT INTO " + "gold.dim_molecule" in sql
        )
        assert "UNION" in insert_sql
        assert "LEFT JOIN bronze.molecule_dictionary md" in insert_sql
        assert "LEFT JOIN bronze.compound_properties cp" in insert_sql

        sql, params = cursor.execute.call_args_list[-1][0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert params == ("gold.dim_molecule", "chembl_35", 5)
        mock_postgres_hook.commit.assert_called_once()

    def test_forces_rebuild_even_if_up_to_date(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (5,)
        cursor.rowcount = 5

        dimension.build_dim_molecule(force_reload=True)

        executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
        assert not any("SELECT version" in sql for sql in executed_sql)
        assert any("TRUNCATE" in sql for sql in executed_sql)

    def test_closes_connection_even_on_failure(self, mock_postgres_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = RuntimeError("db exploded")

        with pytest.raises(RuntimeError):
            dimension.build_dim_molecule(force_reload=False)

        mock_postgres_hook.close.assert_called_once()

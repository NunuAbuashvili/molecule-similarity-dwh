"""Tests for include/s3_input_bronze/ingestion.py"""
from unittest.mock import MagicMock

import pytest

from include.s3_input_bronze import ingestion


@pytest.fixture(autouse=True)
def patch_columns(monkeypatch):
    """Deterministic column config, independent of real config.py values."""
    monkeypatch.setattr(
        ingestion,
        "COLUMNS",
        ["compound_id", "compound_name", "molecular_weight"]
    )
    monkeypatch.setattr(
        ingestion,
        "COLUMN_ALIASES",
        {
            "compound identifier": "compound_id",
            "cmpd_name": "compound_name"
        },
    )


@pytest.fixture
def mock_s3_hook(monkeypatch):
    hook = MagicMock()
    monkeypatch.setattr(ingestion, "get_s3_hook", lambda: hook)
    return hook


@pytest.fixture
def mock_postgres_hook(monkeypatch, pg_conn):
    """Patch PostgresHook in this module so `.get_conn()` returns pg_conn."""
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = pg_conn
    monkeypatch.setattr(ingestion, "PostgresHook", hook_cls)
    return pg_conn


class TestListMatchingKeys:
    def test_filters_keys_by_pattern(self, mock_s3_hook, monkeypatch):
        monkeypatch.setattr(ingestion, "KEY_PATTERN", r".*batch_.*\.csv$")
        mock_s3_hook.list_keys.return_value = [
            "input/nunu_abuashvili/batch_01.csv",
            "input/nunu_abuashvili/batch_02.csv",
            "input/nunu_abuashvili/readme.txt",
        ]

        result = ingestion.list_matching_keys(
            bucket="test-bucket",
            prefix="input/nunu_abuashvili/"
        )

        assert result == [
            "input/nunu_abuashvili/batch_01.csv",
            "input/nunu_abuashvili/batch_02.csv",
        ]

    def test_returns_empty_list_when_no_keys(self, mock_s3_hook, monkeypatch):
        monkeypatch.setattr(ingestion, "KEY_PATTERN", r".*batch_.*\.csv$")
        mock_s3_hook.list_keys.return_value = None

        result = ingestion.list_matching_keys(
            bucket="test-bucket",
            prefix="input/"
        )

        assert result == []

    def test_reraises_on_s3_failure(self, mock_s3_hook, monkeypatch):
        monkeypatch.setattr(ingestion, "KEY_PATTERN", r".*batch_.*\.csv$")
        mock_s3_hook.list_keys.side_effect = RuntimeError("boto3 exploded")

        with pytest.raises(RuntimeError, match="boto3 exploded"):
            ingestion.list_matching_keys(bucket="test-bucket", prefix="input/")


class TestFetchCsvFromS3:
    def test_downloads_to_expected_local_path(self, mock_s3_hook, tmp_path):
        s3_obj = MagicMock()
        mock_s3_hook.get_key.return_value = s3_obj
        dest_dir = str(tmp_path / "downloads")

        local_path = ingestion.fetch_csv_from_s3(
            key="input/nunu_abuashvili/batch_01.csv",
            dest_dir=dest_dir,
        )

        assert local_path == str(tmp_path / "downloads" / "batch_01.csv")
        assert (tmp_path / "downloads").is_dir()
        s3_obj.download_file.assert_called_once_with(local_path)

    def test_raises_when_key_not_found(self, mock_s3_hook, tmp_path):
        mock_s3_hook.get_key.return_value = None

        with pytest.raises(FileNotFoundError):
            ingestion.fetch_csv_from_s3(
                key="missing.csv",
                dest_dir=str(tmp_path)
            )

    def test_reraises_on_download_failure(self, mock_s3_hook, tmp_path):
        s3_obj = MagicMock()
        s3_obj.download_file.side_effect = RuntimeError("network blip")
        mock_s3_hook.get_key.return_value = s3_obj

        with pytest.raises(RuntimeError, match="network blip"):
            ingestion.fetch_csv_from_s3(
                key="batch_01.csv",
                dest_dir=str(tmp_path)
            )


class TestResolveColumns:
    def test_maps_aliases_and_strips_whitespace(self):
        header = [" compound identifier", "cmpd_name ", "molecular_weight"]
        assert ingestion.resolve_columns(header) == [
            "compound_id",
            "compound_name",
            "molecular_weight",
        ]

    def test_passes_through_canonical_names(self):
        header = ["compound_id", "compound_name"]
        assert ingestion.resolve_columns(
            header
        ) == ["compound_id", "compound_name"]

    def test_raises_on_unrecognized_column(self):
        with pytest.raises(ValueError, match="Unrecognized column"):
            ingestion.resolve_columns(["compound_id", "made_up_column"])


class TestReadCsvHeader:
    def test_reads_first_row(self, tmp_path):
        csv_path = tmp_path / "batch_01.csv"
        csv_path.write_text("compound_id,compound_name\nA1,Aspirin\n")

        assert ingestion.read_csv_header(
            str(csv_path)
        ) == ["compound_id", "compound_name"]

    def test_raises_on_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            ingestion.read_csv_header(str(tmp_path / "does_not_exist.csv"))


class TestLoadCsvToPostgres:
    def _write_csv(self, tmp_path):
        csv_path = tmp_path / "batch_01.csv"
        csv_path.write_text(
            "compound identifier,cmpd_name,molecular_weight\n"
            "A1,Aspirin,180.16\n"
            "A2,Paracetamol,151.16\n"
        )
        return str(csv_path)

    def test_creates_staging_table_with_all_columns_typed(
        self,
        tmp_path,
        mock_postgres_hook
    ):
        csv_path = self._write_csv(tmp_path)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.rowcount = 2

        ingestion.load_csv_to_postgres(
            csv_path,
            "bronze.input_molecules",
            source_key="batch_01.csv"
        )

        create_sql = cursor.execute.call_args_list[0][0][0]
        assert "compound_id text" in create_sql
        assert "compound_name text" in create_sql
        assert "molecular_weight text" in create_sql

    def test_copy_expert_uses_resolved_columns(
        self,
        tmp_path,
        mock_postgres_hook
    ):
        csv_path = self._write_csv(tmp_path)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.rowcount = 2

        ingestion.load_csv_to_postgres(
            csv_path,
            "bronze.input_molecules",
            source_key="batch_01.csv"
        )

        copy_sql = cursor.copy_expert.call_args[0][0]
        assert (
            "COPY staging_input (compound_id, compound_name, molecular_weight)"
            in copy_sql
        )
        assert "HEADER true" in copy_sql

    def test_insert_passes_source_key_param(
        self,
        tmp_path,
        mock_postgres_hook
    ):
        csv_path = self._write_csv(tmp_path)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.rowcount = 2

        ingestion.load_csv_to_postgres(
            csv_path,
            "bronze.input_molecules",
            source_key="batch_01.csv"
        )

        insert_sql, params = cursor.execute.call_args_list[-1][0]
        assert "INSERT " + "INTO bronze.input_molecules" in insert_sql
        assert params == ("batch_01.csv",)

    def test_returns_row_count_and_commits(self, tmp_path, mock_postgres_hook):
        csv_path = self._write_csv(tmp_path)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.rowcount = 2

        row_count = ingestion.load_csv_to_postgres(
            csv_path, "bronze.input_molecules", source_key="batch_01.csv"
        )

        assert row_count == 2
        mock_postgres_hook.commit.assert_called_once()

    def test_raises_before_touching_postgres_on_bad_header(
        self,
        tmp_path,
        mock_postgres_hook
    ):
        csv_path = tmp_path / "bad.csv"
        csv_path.write_text("compound_id,made_up_column\nA1,x\n")

        with pytest.raises(ValueError):
            ingestion.load_csv_to_postgres(
                str(csv_path),
                "bronze.input_molecules",
                source_key="bad.csv"
            )

        mock_postgres_hook.cursor.assert_not_called()


class TestRecordInputLoad:
    def test_executes_upsert_with_correct_params(self, mock_postgres_hook):
        ingestion.record_input_load("batch_01.csv", 2)

        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        sql, params = cursor.execute.call_args[0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert "ON CONFLICT (table_name, version)" in sql
        assert params == (ingestion.TARGET_TABLE, "batch_01.csv", 2)

    def test_commits(self, mock_postgres_hook):
        ingestion.record_input_load("batch_01.csv", 5)
        mock_postgres_hook.commit.assert_called_once()

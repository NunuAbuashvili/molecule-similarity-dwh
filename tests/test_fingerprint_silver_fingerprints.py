"""Tests for include/fingerprint_silver/fingerprints.py"""
from unittest.mock import MagicMock

import pytest

from include.fingerprint_silver import fingerprints


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    monkeypatch.setattr(fingerprints, "SOURCE_TABLE", "silver.molecule")
    monkeypatch.setattr(fingerprints, "TARGET_LOG_NAME", "silver.fingerprints")
    monkeypatch.setattr(fingerprints, "BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(fingerprints, "OUTPUT_PREFIX", "final_task/test_test/fingerprints/")
    monkeypatch.setattr(fingerprints, "CHEMBL_VERSION", "chembl_35")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    hook = MagicMock()
    monkeypatch.setattr(fingerprints, "get_s3_hook", lambda: hook)
    return hook


@pytest.fixture
def mock_postgres_hook(monkeypatch, pg_conn):
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = pg_conn
    monkeypatch.setattr(fingerprints, "PostgresHook", hook_cls)
    return pg_conn


class TestComputeFingerprint:
    def test_valid_smiles_returns_256_packed_bytes(self):
        result = fingerprints.compute_fingerprint("CCO")  # ethanol
        assert isinstance(result, bytes)
        assert len(result) == 256  # 2048 bits / 8

    def test_is_deterministic(self):
        first = fingerprints.compute_fingerprint("c1ccccc1")  # benzene
        second = fingerprints.compute_fingerprint("c1ccccc1")
        assert first == second

    def test_invalid_smiles_returns_none(self):
        assert fingerprints.compute_fingerprint("not_a_real_smiles###") is None

    def test_generator_failure_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            fingerprints._generator,
            "GetFingerprint",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        assert fingerprints.compute_fingerprint("CCO") is None


class TestFetchChunk:
    def test_executes_correct_query_and_returns_rows(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1, "CHEMBL1", "CCO")]

        result = fingerprints.fetch_chunk(cursor, last_molregno=0, chunk_size=50)

        assert result == [(1, "CHEMBL1", "CCO")]
        sql, params = cursor.execute.call_args[0]
        assert "silver.molecule" in sql
        assert params == (0, 50)


class TestBuildFingerprintBatch:
    def test_splits_succeeded_and_failed(self):
        rows = [
            (1, "CHEMBL1", "CCO"),
            (2, "CHEMBL2", "not_a_real_smiles###"),
            (3, "CHEMBL3", "c1ccccc1"),
        ]

        records, failed_count = fingerprints.build_fingerprint_batch(rows)

        assert failed_count == 1
        assert [r[0] for r in records] == ["CHEMBL1", "CHEMBL3"]
        assert all(isinstance(r[1], bytes) and len(r[1]) == 256 for r in records)

    def test_empty_input_returns_empty_output(self):
        records, failed_count = fingerprints.build_fingerprint_batch([])
        assert records == []
        assert failed_count == 0


class TestUploadBatchToS3:
    def test_empty_records_returns_none_without_uploading(self, mock_s3_hook):
        result = fingerprints.upload_batch_to_s3([], chunk_number=1)
        assert result is None
        mock_s3_hook.load_bytes.assert_not_called()

    def test_uploads_records_with_expected_key(self, mock_s3_hook):
        records = [("CHEMBL1", b"\x00" * 256)]

        result = fingerprints.upload_batch_to_s3(records, chunk_number=3)

        expected_key = "final_task/test_test/fingerprints/fingerprints_chunk_00003.parquet"
        assert result == expected_key
        _, kwargs = mock_s3_hook.load_bytes.call_args
        assert kwargs["bucket_name"] == "test-bucket"
        assert kwargs["key"] == expected_key
        assert kwargs["replace"] is True


class TestClearExistingOutput:
    def test_deletes_existing_keys(self, mock_s3_hook):
        mock_s3_hook.list_keys.return_value = ["a.parquet", "b.parquet"]

        fingerprints.clear_existing_output("test-bucket", "prefix/")

        mock_s3_hook.delete_objects.assert_called_once_with(
            bucket="test-bucket", keys=["a.parquet", "b.parquet"]
        )

    def test_no_existing_keys_skips_delete(self, mock_s3_hook):
        mock_s3_hook.list_keys.return_value = []

        fingerprints.clear_existing_output("test-bucket", "prefix/")

        mock_s3_hook.delete_objects.assert_not_called()

    def test_none_from_list_keys_skips_delete(self, mock_s3_hook):
        mock_s3_hook.list_keys.return_value = None

        fingerprints.clear_existing_output("test-bucket", "prefix/")

        mock_s3_hook.delete_objects.assert_not_called()


class TestGetLastBuiltVersion:
    def test_returns_version_when_present(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("chembl_35",)
        assert fingerprints.get_last_built_version(cursor) == "chembl_35"

    def test_returns_none_when_absent(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        assert fingerprints.get_last_built_version(cursor) is None


class TestRecordBuild:
    def test_executes_upsert_with_correct_params(self):
        cursor = MagicMock()
        fingerprints.record_build(cursor, "chembl_35", 100)

        sql, params = cursor.execute.call_args[0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert "ON CONFLICT (table_name, version)" in sql
        assert params == ("silver.fingerprints", "chembl_35", 100)


class TestRunFingerprintComputation:
    def test_skips_when_already_built_and_not_forced(self, mock_postgres_hook, mock_s3_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("chembl_35",)

        fingerprints.run_fingerprint_computation(force_reload=False)

        mock_s3_hook.list_keys.assert_not_called()
        cursor.fetchall.assert_not_called()

    def test_processes_multiple_chunks_and_uploads(self, mock_postgres_hook, mock_s3_hook, monkeypatch):
        monkeypatch.setattr(fingerprints, "CHUNK_SIZE", 2)
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None  # never built before
        mock_s3_hook.list_keys.return_value = []  # nothing stale to clear
        cursor.fetchall.side_effect = [
            [(1, "CHEMBL1", "CCO"), (2, "CHEMBL2", "not_a_real_smiles###")],  # full page
            [(3, "CHEMBL3", "c1ccccc1")],  # partial page -> stop
        ]

        fingerprints.run_fingerprint_computation(force_reload=False)

        assert mock_s3_hook.load_bytes.call_count == 2  # one upload per chunk
        sql, params = cursor.execute.call_args_list[-1][0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert params == ("silver.fingerprints", "chembl_35", 2)  # 2 valid total

    def test_closes_connection_even_on_failure(self, mock_postgres_hook, mock_s3_hook):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = RuntimeError("db exploded")

        with pytest.raises(RuntimeError):
            fingerprints.run_fingerprint_computation(force_reload=False)

        mock_postgres_hook.close.assert_called_once()

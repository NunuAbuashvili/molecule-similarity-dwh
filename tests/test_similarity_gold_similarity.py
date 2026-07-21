"""Tests for include/similarity_gold/similarity.py"""
import base64
import io
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from rdkit import DataStructs

from include.similarity_gold import similarity


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    monkeypatch.setattr(
        similarity,
        "SILVER_INPUT_TABLE",
        "silver.input_molecule"
    )
    monkeypatch.setattr(similarity, "TARGET_TABLE", "gold.fact_similarity")
    monkeypatch.setattr(similarity, "BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(
        similarity,
        "FINGERPRINT_PREFIX",
        "final_task/test_test/fingerprints/"
    )
    monkeypatch.setattr(
        similarity,
        "SIMILARITY_OUTPUT_PREFIX",
        "final_task/test_test/similarity/"
    )
    monkeypatch.setattr(similarity, "CHEMBL_VERSION", "chembl_35")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    hook = MagicMock()
    monkeypatch.setattr(similarity, "get_s3_hook", lambda: hook)
    return hook


@pytest.fixture
def mock_postgres_hook(monkeypatch, pg_conn):
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = pg_conn
    monkeypatch.setattr(similarity, "PostgresHook", hook_cls)
    return pg_conn


def _pack_bits(on_bit_indices, num_bits=2048):
    arr = np.zeros(num_bits, dtype=np.uint8)
    arr[on_bit_indices] = 1
    return np.packbits(arr).tobytes()


def _parquet_body(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow")
    body = MagicMock()
    body.read.return_value = buffer.getvalue()
    return body


class TestGetQueryMolecules:
    def test_returns_distinct_chembl_ids(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("CHEMBL1",), ("CHEMBL2",)]

        assert similarity.get_query_molecules(cursor) == {"CHEMBL1", "CHEMBL2"}

    def test_raises_when_no_matched_molecules(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []

        with pytest.raises(ValueError, match="ETL Aborted"):
            similarity.get_query_molecules(cursor)


class TestFetchSourceFingerprints:
    def test_finds_and_base64_encodes_matches(self, mock_s3_hook):
        fp_bytes = _pack_bits([0, 1, 2])
        df = pd.DataFrame(
            [
                {
                    "chembl_id": "CHEMBL1",
                    "fingerprint_bytes": fp_bytes
                }
            ]
        )
        mock_s3_hook.list_keys.return_value = [
            "fingerprints_chunk_00001.parquet"
        ]
        obj = MagicMock()
        obj.get.return_value = {"Body": _parquet_body(df)}
        mock_s3_hook.get_key.return_value = obj

        result = similarity.fetch_source_fingerprints({"CHEMBL1"})

        assert result == [
            {
                "chembl_id": "CHEMBL1",
                "fingerprint_b64": base64.b64encode(fp_bytes).decode("ascii")
            }
        ]

    def test_stops_scanning_once_all_sources_found(self, mock_s3_hook):
        df = pd.DataFrame(
            [{"chembl_id": "CHEMBL1", "fingerprint_bytes": _pack_bits([0])}]
        )
        mock_s3_hook.list_keys.return_value = [
            "fingerprints_chunk_00001.parquet",
            "fingerprints_chunk_00002.parquet",
        ]
        obj = MagicMock()
        obj.get.return_value = {"Body": _parquet_body(df)}
        mock_s3_hook.get_key.return_value = obj

        similarity.fetch_source_fingerprints({"CHEMBL1"})

        mock_s3_hook.get_key.assert_called_once()  # never opened the 2nd file

    def test_warns_but_returns_partial_on_missing_sources(
        self,
        mock_s3_hook,
        caplog
    ):
        df = pd.DataFrame(
            [
                {
                    "chembl_id": "CHEMBL1",
                    "fingerprint_bytes": _pack_bits([0])
                }
            ]
        )
        mock_s3_hook.list_keys.return_value = [
            "fingerprints_chunk_00001.parquet"
        ]
        obj = MagicMock()
        obj.get.return_value = {"Body": _parquet_body(df)}
        mock_s3_hook.get_key.return_value = obj

        with caplog.at_level("WARNING"):
            result = similarity.fetch_source_fingerprints(
                {"CHEMBL1", "CHEMBL_MISSING"}
            )

        assert len(result) == 1
        assert "CHEMBL_MISSING" in caplog.text

    def test_raises_when_no_parquet_files(self, mock_s3_hook):
        mock_s3_hook.list_keys.return_value = ["readme.txt"]

        with pytest.raises(FileNotFoundError):
            similarity.fetch_source_fingerprints({"CHEMBL1"})

    def test_reraises_on_list_keys_failure(self, mock_s3_hook):
        mock_s3_hook.list_keys.side_effect = RuntimeError("boto3 exploded")

        with pytest.raises(RuntimeError, match="boto3 exploded"):
            similarity.fetch_source_fingerprints({"CHEMBL1"})


class TestBytesToBitvect:
    def test_round_trips_bit_pattern_exactly(self):
        on_bits = [0, 5, 100, 2047]
        packed = _pack_bits(on_bits)

        fp = similarity.bytes_to_bitvect(packed, num_bits=2048)

        assert fp.GetNumBits() == 2048
        assert sorted(fp.GetOnBits()) == on_bits

    def test_identical_reconstructed_fingerprints_have_tanimoto_one(self):
        packed = _pack_bits([0, 1, 2, 3])
        fp1 = similarity.bytes_to_bitvect(packed, num_bits=2048)
        fp2 = similarity.bytes_to_bitvect(packed, num_bits=2048)

        assert DataStructs.TanimotoSimilarity(fp1, fp2) == 1.0

    def test_raises_on_malformed_bytes(self):
        oversized = b"\xff" * 1000
        with pytest.raises(Exception):
            similarity.bytes_to_bitvect(oversized, num_bits=8)


class TestSelectTopN:
    def test_basic_ranking_no_ties(self):
        scores = [("A", 0.9), ("B", 0.8), ("C", 0.7), ("D", 0.6), ("E", 0.5)]

        result = similarity.select_top_n(scores, top_n=3)

        assert [r["target_chembl_id"] for r in result] == ["A", "B", "C"]
        assert [r["rank"] for r in result] == [1, 2, 3]
        assert all(
            not r["has_duplicate_of_last_largest_score"]
            for r in result
        )

    def test_flags_cutoff_tie_that_spills_over(self):
        scores = [("A", 0.9), ("B", 0.8), ("C", 0.7), ("D", 0.7), ("E", 0.6)]

        result = similarity.select_top_n(scores, top_n=3)

        flags = {
            r["target_chembl_id"]: r["has_duplicate_of_last_largest_score"]
            for r in result
        }
        assert flags == {"A": False, "B": False, "C": True}

    def test_internal_tie_not_flagged_when_nothing_excluded(self):
        scores = [("A", 0.9), ("B", 0.8), ("C", 0.8)]

        result = similarity.select_top_n(scores, top_n=3)

        assert all(
            not r["has_duplicate_of_last_largest_score"]
            for r in result
        )

    def test_tie_among_top_ranks_not_at_cutoff_is_not_flagged(self):
        scores = [("A", 0.9), ("B", 0.9), ("C", 0.5)]

        result = similarity.select_top_n(scores, top_n=2)

        assert all(
            not r["has_duplicate_of_last_largest_score"]
            for r in result
        )

    def test_fewer_candidates_than_top_n(self):
        result = similarity.select_top_n([("A", 0.5)], top_n=10)

        assert len(result) == 1
        assert result[0]["rank"] == 1
        assert result[0]["has_duplicate_of_last_largest_score"] is False

    def test_empty_scores_returns_empty_list(self):
        assert similarity.select_top_n([], top_n=10) == []


class TestGetLastBuiltVersion:
    def test_returns_version_when_present(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("chembl_35",)
        assert similarity.get_last_built_version(cursor) == "chembl_35"

    def test_returns_none_when_absent(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        assert similarity.get_last_built_version(cursor) is None


class TestRecordBuild:
    def test_executes_upsert_with_correct_params(self):
        cursor = MagicMock()
        similarity.record_build(cursor, "chembl_35", 100)

        sql, params = cursor.execute.call_args[0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert "ON CONFLICT (table_name, version)" in sql
        assert params == ("gold.fact_similarity", "chembl_35", 100)


class TestWriteFullSimilarityTable:
    def test_uploads_expected_key(self, mock_s3_hook):
        records = [("CHEMBL2", 0.9), ("CHEMBL3", 0.5)]

        s3_key = similarity.write_full_similarity_table("CHEMBL1", records)

        assert s3_key == "final_task/test_test/similarity/CHEMBL1.parquet"
        _, kwargs = mock_s3_hook.load_bytes.call_args
        assert kwargs["bucket_name"] == "test-bucket"
        assert kwargs["key"] == s3_key
        assert kwargs["replace"] is True


class TestWriteTopNToGold:
    def test_inserts_one_row_per_candidate(self):
        cursor = MagicMock()
        top_n_rows = [
            {"target_chembl_id": "CHEMBL2", "tanimoto_score": 0.9, "rank": 1,
             "has_duplicate_of_last_largest_score": False},
            {"target_chembl_id": "CHEMBL3", "tanimoto_score": 0.5, "rank": 2,
             "has_duplicate_of_last_largest_score": False},
        ]

        similarity.write_top_n_to_gold(cursor, "CHEMBL1", top_n_rows)

        assert cursor.execute.call_count == 2
        _, params = cursor.execute.call_args_list[0][0]
        assert params == ("CHEMBL1", "CHEMBL2", 0.9, 1, False)

    def test_empty_rows_no_inserts(self):
        cursor = MagicMock()
        similarity.write_top_n_to_gold(cursor, "CHEMBL1", [])
        cursor.execute.assert_not_called()


class TestComputeAndUploadSimilarity:
    def test_skips_when_already_built_and_not_forced(
        self,
        mock_postgres_hook,
        mock_s3_hook
    ):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("chembl_35",)

        similarity.compute_and_upload_similarity(
            [{"chembl_id": "CHEMBL1", "fingerprint_b64": "irrelevant"}]
        )

        mock_s3_hook.list_keys.assert_not_called()

    def test_excludes_self_match_and_ranks_candidates_correctly(
        self,
        mock_postgres_hook,
        mock_s3_hook
    ):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None  # never built before

        src_bytes = _pack_bits([0, 1, 2, 3])
        source_fingerprints = [
            {
                "chembl_id": "CHEMBL_SRC",
                "fingerprint_b64": base64.b64encode(src_bytes).decode("ascii")
            }
        ]

        candidates_df = pd.DataFrame(
            [
                {
                    "chembl_id": "CHEMBL_SRC",
                    "fingerprint_bytes": _pack_bits([0, 1, 2, 3])
                },
                {
                    "chembl_id": "CHEMBL_A",
                    "fingerprint_bytes": _pack_bits([0, 1, 2, 3])
                },
                {
                    "chembl_id": "CHEMBL_B",
                    "fingerprint_bytes": _pack_bits([0, 1])
                },
            ]
        )
        mock_s3_hook.list_keys.return_value = [
            "fingerprints_chunk_00001.parquet"
        ]
        obj = MagicMock()
        obj.get.return_value = {"Body": _parquet_body(candidates_df)}
        mock_s3_hook.get_key.return_value = obj

        similarity.compute_and_upload_similarity(
            source_fingerprints,
            force_reload=False
        )

        upload_key = mock_s3_hook.load_bytes.call_args[1]["key"]
        assert (
            upload_key == "final_task/test_test/similarity/CHEMBL_SRC.parquet"
        )

        insert_calls = [
            call[0][1] for call in cursor.execute.call_args_list
            if call[0][0].strip().startswith(
                "INSERT " + "INTO gold.fact_similarity"
            )
        ]
        target_ids = [params[1] for params in insert_calls]
        assert target_ids == ["CHEMBL_A", "CHEMBL_B"]
        assert insert_calls[0][2] == pytest.approx(1.0)
        assert insert_calls[1][2] == pytest.approx(0.5)

        sql, params = cursor.execute.call_args_list[-1][0]
        assert "INSERT " + "INTO meta.load_log" in sql
        assert params == ("gold.fact_similarity", "chembl_35", 2)

    def test_closes_connection_even_on_failure(
        self,
        mock_postgres_hook,
        mock_s3_hook
    ):
        cursor = mock_postgres_hook.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = RuntimeError("db exploded")

        with pytest.raises(RuntimeError):
            similarity.compute_and_upload_similarity(
                [{"chembl_id": "X", "fingerprint_b64": "AA=="}]
            )

        mock_postgres_hook.close.assert_called_once()

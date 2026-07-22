"""
Compute Tanimoto similarity, write full tables to S3,
and load top-10 into gold.
"""

import base64
import io
import logging

import numpy as np
import pandas as pd
from rdkit import DataStructs

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.similarity_gold.config import (
    AWS_CONN_ID,
    POSTGRES_CONN_ID,
    CHEMBL_VERSION,
    SILVER_INPUT_TABLE,
    TARGET_TABLE,
    BUCKET_NAME,
    FINGERPRINT_PREFIX,
    FINGERPRINT_SIZE,
    TOP_N,
    SIMILARITY_OUTPUT_PREFIX,
)

logger = logging.getLogger(__name__)


def get_s3_hook() -> S3Hook:
    """Return an S3Hook bound to the configured AWS connection."""
    return S3Hook(aws_conn_id=AWS_CONN_ID)


def get_query_molecules(cursor) -> set[str]:
    """
    Return the distinct matched source chembl_ids
    from silver.input_molecule; raise if none.
    """
    cursor.execute(
        f"""
        SELECT DISTINCT chembl_id
        FROM {SILVER_INPUT_TABLE}
        WHERE chembl_id IS NOT NULL
        """
    )
    rows = cursor.fetchall()

    if not rows:
        raise ValueError(
            f"ETL Aborted: No valid matched chembl_ids "
            f"found in {SILVER_INPUT_TABLE}. "
        )
    source_chembl_ids = {row[0] for row in rows}

    logger.info(
        "Retrieved %d source molecule(s) for similarity matching.",
        len(source_chembl_ids),
    )
    return source_chembl_ids


def fetch_source_fingerprints(
    source_ids: set[str],
    bucket: str = BUCKET_NAME,
    prefix: str = FINGERPRINT_PREFIX
) -> list[dict]:
    """
    Scan fingerprint Parquet files for the source ids
    and return their base64-encoded bytes.
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

    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise FileNotFoundError(
            f"No parquet files found in s3://{bucket}/{prefix}"
        )

    logger.info(
        "Scanning %d fingerprint files for %d source molecules...",
        len(parquet_keys), len(source_ids),
    )

    found: dict[str, str] = {}
    for key in parquet_keys:
        obj = hook.get_key(key=key, bucket_name=bucket)
        buffer = io.BytesIO(obj.get()["Body"].read())
        df = pd.read_parquet(
            buffer, columns=["chembl_id", "fingerprint_bytes"]
        )

        matched = df[df["chembl_id"].isin(source_ids)]
        for _, row in matched.iterrows():
            found[row["chembl_id"]] = base64.b64encode(
                row["fingerprint_bytes"]
            ).decode("ascii")

        if len(found) == len(source_ids):
            logger.info(
                "Found all %d source fingerprints, stopping scan early",
                len(source_ids)
            )
            break

    missing = source_ids - found.keys()
    if missing:
        logger.warning(
            "Could not find fingerprints for %d source chembl_id(s): %s",
            len(missing), missing,
        )

    return [
        {"chembl_id": cid, "fingerprint_b64": fp_b64}
        for cid, fp_b64 in found.items()
    ]


def bytes_to_bitvect(
    fp_bytes: bytes,
    num_bits: int = FINGERPRINT_SIZE
) -> DataStructs.ExplicitBitVect:
    """
    Reconstruct an RDKit ExplicitBitVect from packed fingerprint bytes.
    """
    try:
        arr = np.unpackbits(np.frombuffer(fp_bytes, dtype=np.uint8))
        fp = DataStructs.ExplicitBitVect(num_bits)
        on_bits = np.nonzero(arr)[0].tolist()
        fp.SetBitsFromList(on_bits)
        return fp
    except Exception:
        logger.exception(
            "Failed to reconstruct ExplicitBitVect from %d bytes",
            len(fp_bytes)
        )
        raise


def select_top_n(
    target_ids: np.ndarray,
    scores: np.ndarray,
    top_n: int = TOP_N,
) -> list[dict]:
    """
    Select the top-N targets from aligned id/score arrays via a stable sort,
    flagging cutoff ties that spilled over.
    """
    n = scores.shape[0]
    if n == 0:
        return []

    k = min(top_n, n)
    order = np.argsort(-scores, kind="stable")[:k]
    top_ids = target_ids[order]
    top_scores = scores[order]
    cutoff = top_scores[-1]

    total_at_cutoff = int(np.count_nonzero(scores == cutoff))
    top_at_cutoff = int(np.count_nonzero(top_scores == cutoff))
    has_excluded_duplicate = total_at_cutoff > top_at_cutoff

    return [
        {
            "target_chembl_id": str(top_ids[i]),
            "tanimoto_score": float(top_scores[i]),
            "rank": i + 1,
            "has_duplicates_of_last_largest_score": bool(
                has_excluded_duplicate and top_scores[i] == cutoff
            ),
        }
        for i in range(k)
    ]


def get_last_built_version(cursor) -> str | None:
    """
    Return the ChEMBL version gold.fact_similarity
    was last built from, or None.
    """
    cursor.execute(
        "SELECT version FROM meta.load_log WHERE table_name = %s",
        (TARGET_TABLE,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def record_build(cursor, version: str, row_count: int) -> None:
    """Upsert the build metadata row for gold.fact_similarity."""
    cursor.execute(
        """
        INSERT INTO meta.load_log (
            table_name, version, row_count, loaded_at
        )
        VALUES (%s, %s, %s, now())
        ON CONFLICT (table_name, version)
        DO UPDATE SET row_count = EXCLUDED.row_count,
                      loaded_at = EXCLUDED.loaded_at
        """,
        (TARGET_TABLE, version, row_count),
    )


def write_full_similarity_table(
    chembl_id: str,
    target_ids: np.ndarray,
    scores: np.ndarray,
) -> str:
    """
    Write one source's full target/score arrays to a
    Parquet file in S3; return the key.
    """
    df = pd.DataFrame(
        {"target_chembl_id": target_ids, "tanimoto_score": scores}
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow")

    s3_key = f"{SIMILARITY_OUTPUT_PREFIX}{chembl_id}.parquet"
    get_s3_hook().load_bytes(
        buffer.getvalue(),
        bucket_name=BUCKET_NAME,
        key=s3_key,
        replace=True,
    )
    logger.info(
        "Uploaded %s scores for %s to s3://%s/%s",
        len(target_ids), chembl_id, BUCKET_NAME, s3_key,
    )
    return s3_key


def write_top_n_to_gold(
    cursor,
    source_chembl_id: str,
    top_n_rows: list[dict]
) -> None:
    """Insert one source's top-N rows into gold.fact_similarity."""
    for row in top_n_rows:
        cursor.execute(
            f"""
            INSERT INTO {TARGET_TABLE} (
                source_chembl_id, target_chembl_id, tanimoto_score, rank,
                has_duplicates_of_last_largest_score
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                source_chembl_id,
                row["target_chembl_id"],
                row["tanimoto_score"],
                row["rank"],
                row["has_duplicates_of_last_largest_score"],
            ),
        )


def compute_and_upload_similarity(
    source_fingerprints: list[dict],
    force_reload: bool = False,
) -> None:
    """
    Score each source against all candidates in a single memory-bounded pass,
    write full tables to S3, and load top-10 into gold;
    idempotent unless force_reload.
    """
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor() as cursor:
            if not force_reload:
                last_version = get_last_built_version(cursor)
                if last_version == CHEMBL_VERSION:
                    logger.info(
                        "gold.fact_similarity already built "
                        "from ChEMBL version %s, "
                        "skipping (force_reload=False)",
                        CHEMBL_VERSION,
                    )
                    return

            cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")

            # Decode + convert source fingerprints
            sources: dict[str, DataStructs.ExplicitBitVect] = {}
            for item in source_fingerprints:
                fp_bytes = base64.b64decode(item["fingerprint_b64"])
                sources[item["chembl_id"]] = bytes_to_bitvect(fp_bytes)

            hook = get_s3_hook()
            keys = hook.list_keys(
                bucket_name=BUCKET_NAME,
                prefix=FINGERPRINT_PREFIX,
            ) or []
            # Sort so the shared id order is deterministic across runs.
            parquet_keys = sorted(
                k for k in keys if k.endswith(".parquet")
            )

            logger.info(
                "Computing similarity for %d sources "
                "against %d candidate files",
                len(sources),
                len(parquet_keys),
            )

            # Shared candidate ids (stored once) + one float32 score
            # column per source, both accumulated chunk by chunk.
            id_chunks: list[np.ndarray] = []
            score_chunks: dict[str, list[np.ndarray]] = {
                cid: [] for cid in sources
            }

            for chunk_number, key in enumerate(parquet_keys, start=1):
                obj = hook.get_key(key=key, bucket_name=BUCKET_NAME)
                buffer = io.BytesIO(obj.get()["Body"].read())
                df = pd.read_parquet(
                    buffer,
                    columns=["chembl_id", "fingerprint_bytes"],
                )

                id_chunks.append(df["chembl_id"].to_numpy())
                candidate_fps = [
                    bytes_to_bitvect(fp) for fp in df["fingerprint_bytes"]
                ]

                for source_id, source_fp in sources.items():
                    scores = np.asarray(
                        DataStructs.BulkTanimotoSimilarity(
                            source_fp, candidate_fps
                        ),
                        dtype=np.float32,
                    )
                    score_chunks[source_id].append(scores)

                logger.info(
                    "Processed chunk %d/%d (%s)",
                    chunk_number, len(parquet_keys), key,
                )

            global_ids = np.concatenate(id_chunks)
            del id_chunks

            total_row_count = 0
            # pop() frees each source's score chunks as soon as it's written.
            for source_id in list(sources.keys()):
                final_scores = np.concatenate(score_chunks.pop(source_id))

                keep = global_ids != source_id  # drop self-match
                target_ids = global_ids[keep]
                target_scores = final_scores[keep]

                write_full_similarity_table(
                    source_id, target_ids, target_scores
                )

                top_n_rows = select_top_n(target_ids, target_scores)
                write_top_n_to_gold(cursor, source_id, top_n_rows)
                total_row_count += len(top_n_rows)

            record_build(cursor, CHEMBL_VERSION, total_row_count)
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "gold.fact_similarity build complete: "
        "%d sources processed, %d total rows written",
        len(sources),
        total_row_count,
    )

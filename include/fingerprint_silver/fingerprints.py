"""
Compute Morgan fingerprints for validated molecules
and upload as Parquet to S3.
"""

import io
import logging

import numpy as np
import pandas as pd
from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.fingerprint_silver.config import (
    POSTGRES_CONN_ID,
    AWS_CONN_ID,
    SOURCE_TABLE,
    TARGET_LOG_NAME,
    BUCKET_NAME,
    OUTPUT_PREFIX,
    CHUNK_SIZE,
    FINGERPRINT_RADIUS,
    FINGERPRINT_SIZE,
    CHEMBL_VERSION,
)

logger = logging.getLogger(__name__)

_generator = rdFingerprintGenerator.GetMorganGenerator(
    radius=FINGERPRINT_RADIUS, fpSize=FINGERPRINT_SIZE
)


def get_s3_hook() -> S3Hook:
    """Return an S3Hook bound to the configured AWS connection."""
    return S3Hook(aws_conn_id=AWS_CONN_ID)


def compute_fingerprint(smiles: str) -> bytes | None:
    """
    Compute a 256-byte packed Morgan fingerprint for a SMILES string,
    or None on failure.
    """
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        logger.warning(
            "SMILES failed to (re-)parse despite prior validation: %s",
            smiles
        )
        return None
    try:
        fp = _generator.GetFingerprint(mol)
        arr = np.zeros((fp.GetNumBits(),), dtype=np.uint8)
        DataStructs.ConvertToNumpyArray(fp, arr)
        return np.packbits(arr).tobytes()

    except Exception:
        logger.exception(
            "Failed to compute fingerprint for SMILES: %s",
            smiles
        )
        return None


def fetch_chunk(
    cursor,
    last_molregno: int,
    chunk_size: int
) -> list[tuple]:
    """
    Fetch one keyset-paginated chunk of validated molecules from silver.
    """
    cursor.execute(
        f"""
        SELECT molregno, chembl_id, canonical_smiles
        FROM {SOURCE_TABLE}
        WHERE molregno > %s
        ORDER BY molregno
        LIMIT %s
        """,
        (last_molregno, chunk_size)
    )
    rows = cursor.fetchall()
    logger.debug(
        "Fetched %s rows past molregno=%s",
        len(rows), last_molregno
    )
    return rows


def build_fingerprint_batch(rows: list[tuple]) -> tuple[list[tuple], int]:
    """
    Compute fingerprints for a chunk, returning (chembl_id, bytes) records
    and a failed count.
    """
    records = []
    failed_count = 0

    for row in rows:
        _, chembl_id, smiles = row
        fingerprint = compute_fingerprint(smiles)
        if fingerprint is None:
            failed_count += 1
            continue

        records.append((chembl_id, fingerprint))

    logger.info(
        "Built fingerprint batch: %s succeeded, %s failed",
        len(records), failed_count,
    )
    return records, failed_count


def upload_batch_to_s3(
    records: list[tuple],
    chunk_number: int
) -> str | None:
    """
    Write a batch of fingerprint records to Parquet and
    upload to S3; return the key or None.
    """
    if not records:
        logger.warning(
            "No records to upload for chunk %s, skipping",
            chunk_number
        )
        return None

    df = pd.DataFrame(records, columns=["chembl_id", "fingerprint_bytes"])
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow")

    s3_key = f"{OUTPUT_PREFIX}fingerprints_chunk_{chunk_number:05d}.parquet"
    get_s3_hook().load_bytes(
        buffer.getvalue(),
        bucket_name=BUCKET_NAME,
        key=s3_key,
        replace=True,
    )

    logger.info(
        "Uploaded %s records to s3://%s/%s",
        len(records), BUCKET_NAME, s3_key,
    )
    return s3_key


def clear_existing_output(bucket: str, prefix: str) -> None:
    """
    Delete any existing Parquet files under the output prefix before a rebuild.
    """
    hook = get_s3_hook()
    existing_keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    if existing_keys:
        hook.delete_objects(bucket=bucket, keys=existing_keys)
        logger.info(
            "Deleted %s stale objects under s3://%s/%s",
            len(existing_keys), bucket, prefix
        )


def get_last_built_version(cursor) -> str | None:
    """
    Return the ChEMBL version fingerprints were last built from, or None.
    """
    cursor.execute(
        "SELECT version "
        "FROM meta.load_log "
        "WHERE table_name = %s",
        (TARGET_LOG_NAME,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def record_build(
    cursor,
    version: str,
    row_count: int
) -> None:
    """
    Upsert the build metadata row for the fingerprint output.
    """
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
        (TARGET_LOG_NAME, version, row_count),
    )


def run_fingerprint_computation(force_reload: bool = False) -> None:
    """
    Compute Morgan fingerprints for all validated molecules and
    upload them as Parquet chunks to S3; idempotent unless force_reload.
    """
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    last_molregno = 0
    total_valid = 0
    total_rejected = 0
    chunk_number = 0

    try:
        with conn.cursor() as cursor:
            if not force_reload:
                last_version = get_last_built_version(cursor)
                if last_version == CHEMBL_VERSION:
                    logger.info(
                        "Fingerprints already built from ChEMBL version %s, "
                        "skipping (force_reload=False)",
                        CHEMBL_VERSION,
                    )
                    return

            clear_existing_output(BUCKET_NAME, OUTPUT_PREFIX)

            while True:
                chunk_number += 1
                rows = fetch_chunk(cursor, last_molregno, CHUNK_SIZE)
                if not rows:
                    logger.info(
                        "No more rows past molregno=%s, stopping",
                        last_molregno
                    )
                    break

                valid_rows, rejected_count = build_fingerprint_batch(rows)
                upload_batch_to_s3(valid_rows, chunk_number)
                conn.commit()

                total_valid += len(valid_rows)
                total_rejected += rejected_count
                last_molregno = rows[-1][0]

                logger.info(
                    "Chunk %s complete: last_molregno=%s, "
                    "loaded=%s, rejected=%s, "
                    "running totals: valid=%s, rejected=%s",
                    chunk_number, last_molregno,
                    len(valid_rows), rejected_count,
                    total_valid, total_rejected,
                )

                if len(rows) < CHUNK_SIZE:
                    break

            record_build(cursor, CHEMBL_VERSION, total_valid)
            conn.commit()
    finally:
        conn.close()

    logger.info(
        "Fingerprint calculation run finished: "
        "%s chunks, %s valid, %s rejected",
        chunk_number, total_valid, total_rejected,
    )

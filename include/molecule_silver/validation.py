"""
Validate and clean raw ChEMBL molecule structures into silver.molecule.
"""
import csv
import io
import logging

from rdkit import Chem
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.molecule_silver.config import (
    POSTGRES_CONN_ID,
    STRUCTURES_TABLE,
    DICTIONARY_TABLE,
    TARGET_TABLE,
    CHUNK_SIZE,
    CHEMBL_VERSION
)

logger = logging.getLogger(__name__)


def is_valid_smiles(smiles: str) -> bool:
    """
    Check whether RDKit can parse a SMILES string into a molecule.

    Args:
        smiles: A SMILES string.

    Returns:
        True if the string parses into a valid RDKit molecule,
        False otherwise.
    """
    return Chem.MolFromSmiles(smiles) is not None


def fetch_chunk(
    cursor,
    last_molregno: int,
    chunk_size: int
) -> list[tuple]:
    """
    Fetch one keyset-paginated chunk of molecules from bronze.

    Joins compound structures with the molecule dictionary and returns rows
    with a non-null, non-empty canonical SMILES, ordered by molregno.

    Args:
        cursor: An open database cursor.
        last_molregno: The highest molregno already processed; only rows
            with a greater molregno are returned.
        chunk_size: Maximum number of rows to fetch.

    Returns:
        A list of (molregno, chembl_id, canonical_smiles) tuples.
    """
    cursor.execute(
        f"""
        SELECT cs.molregno, md.chembl_id, cs.canonical_smiles
        FROM {STRUCTURES_TABLE} cs
        JOIN {DICTIONARY_TABLE} md
            ON cs.molregno = md.molregno
        WHERE cs.molregno > %s
            AND NULLIF(canonical_smiles, '') IS NOT NULL
        ORDER BY cs.molregno
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


def validate_chunk(rows: list[tuple]) -> tuple[list[tuple], int]:
    """
    Split a chunk into rows with parseable SMILES and a rejected count.

    Args:
        rows: (molregno, chembl_id, canonical_smiles) tuples to validate.

    Returns:
        A tuple of (valid_rows, rejected_count).
    """
    valid_rows = []
    rejected_count = 0

    for row in rows:
        smile = row[2]
        if is_valid_smiles(smile):
            valid_rows.append(row)
        else:
            rejected_count += 1

    logger.info(
        "Validated chunk: %s valid, %s rejected",
        len(valid_rows), rejected_count,
    )
    return valid_rows, rejected_count


def load_chunk_to_silver(
    cursor,
    rows: list[tuple],
    target_table: str = TARGET_TABLE
) -> int:
    """
    Bulk-load validated rows into the silver table.

    Args:
        cursor: An open database cursor.
        rows: (molregno, chembl_id, canonical_smiles) tuples to insert.
        target_table: Fully-qualified destination table name.

    Returns:
        Number of rows loaded.
    """
    if not rows:
        logger.debug(
            "No valid rows to load into %s for this chunk",
            target_table
        )
        return 0

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerows(rows)
    buffer.seek(0)

    col_list = "molregno, chembl_id, canonical_smiles"
    cursor.copy_expert(
        f"COPY {target_table} ({col_list}) "
        f"FROM STDIN WITH (FORMAT csv, NULL '')",
        buffer
    )

    logger.debug(
        "Loaded %s rows into %s",
        len(rows), target_table
    )
    return len(rows)


def get_last_built_version(cursor) -> str | None:
    """
    Look up which ChEMBL version silver.molecule was last built from.

    Args:
        cursor: An open database cursor.

    Returns:
        The recorded version string, or None if silver.molecule has never
        been built.
    """
    cursor.execute(
        "SELECT version "
        "FROM meta.load_log "
        "WHERE table_name = %s",
        (TARGET_TABLE,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def record_build(
    cursor,
    version: str,
    row_count: int
) -> None:
    """
    Upsert the build metadata row for silver.molecule.

    Args:
        cursor: An open database cursor.
        version: The ChEMBL version this build was sourced from.
        row_count: Total number of valid rows loaded.
    """
    cursor.execute(
        """
        INSERT INTO meta.load_log (table_name, version, row_count, loaded_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (table_name, version)
        DO UPDATE SET row_count = EXCLUDED.row_count,
                      loaded_at = EXCLUDED.loaded_at
        """,
        (TARGET_TABLE, version, row_count),
    )


def run_validation(force_reload: bool = False) -> None:
    """
    Build silver.molecule from bronze ChEMBL tables.

    Skips the rebuild if silver.molecule was already built from the current
    ChEMBL version, unless force_reload is True. Otherwise, truncates the
    target table, then pages through bronze in keyset-paginated chunks,
    validating each row's SMILES with RDKit and loading the valid subset.
    Idempotent: safe to re-run in full from scratch.

    Args:
        force_reload: If True, rebuild even if already up to date.
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
                        "silver.molecule already built "
                        "from ChEMBL version %s, "
                        "skipping (force_reload=False)",
                        CHEMBL_VERSION,
                    )
                    return

            cursor.execute(
                f"TRUNCATE TABLE {TARGET_TABLE}"
            )
            conn.commit()

            while True:
                chunk_number += 1
                rows = fetch_chunk(cursor, last_molregno, CHUNK_SIZE)
                if not rows:
                    logger.info(
                        "No more rows past molregno=%s, stopping",
                        last_molregno
                    )
                    break

                valid_rows, rejected_count = validate_chunk(rows)
                loaded = load_chunk_to_silver(cursor, valid_rows)
                conn.commit()

                total_valid += loaded
                total_rejected += rejected_count
                last_molregno = rows[-1][0]

                logger.info(
                    "Chunk %s complete: last_molregno=%s, "
                    "loaded=%s, rejected=%s, "
                    "running totals: valid=%s, rejected=%s",
                    chunk_number, last_molregno,
                    loaded, rejected_count,
                    total_valid, total_rejected,
                )

                if len(rows) < CHUNK_SIZE:
                    break

            record_build(cursor, CHEMBL_VERSION, total_valid)
            conn.commit()
    finally:
        conn.close()

    logger.info(
        "Validation run finished: %s chunks, %s valid, %s rejected",
        chunk_number, total_valid, total_rejected,
    )

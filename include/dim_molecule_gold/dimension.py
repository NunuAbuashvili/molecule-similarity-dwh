import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.dim_molecule_gold.config import (
    POSTGRES_CONN_ID,
    FACT_TABLE,
    DICTIONARY_TABLE,
    PROPERTIES_TABLE,
    TARGET_TABLE,
    CHEMBL_VERSION,
)

logger = logging.getLogger(__name__)


def get_last_built_version(cursor) -> str | None:
    cursor.execute(
        "SELECT version FROM meta.load_log WHERE table_name = %s",
        (TARGET_TABLE,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def record_build(cursor, version: str, row_count: int) -> None:
    """Upsert the build metadata row for gold.dim_molecule."""
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


def build_dim_molecule(force_reload: bool = False) -> None:
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor() as cursor:
            if not force_reload:
                last_version = get_last_built_version(cursor)
                if last_version == CHEMBL_VERSION:
                    logger.info(
                        "gold.dim_molecule already built "
                        "from ChEMBL version %s, "
                        "skipping (force_reload=False)",
                        CHEMBL_VERSION,
                    )
                    return

            cursor.execute(f"SELECT count(*) FROM {FACT_TABLE}")
            if cursor.fetchone()[0] == 0:
                raise ValueError(
                    f"ETL aborted: {FACT_TABLE} is empty, nothing to scope "
                    f"{TARGET_TABLE} to."
                )

            cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")

            cursor.execute(
                f"""
                WITH referenced_molecules AS (
                    SELECT source_chembl_id AS chembl_id FROM {FACT_TABLE}
                    UNION
                    SELECT target_chembl_id AS chembl_id FROM {FACT_TABLE}
                )
                INSERT INTO {TARGET_TABLE} (
                    chembl_id, molecule_type, mw_freebase, alogp, psa,
                    full_mwt, aromatic_rings, heavy_atoms
                )
                SELECT
                    rm.chembl_id,
                    md.molecule_type,
                    cp.mw_freebase,
                    cp.alogp,
                    cp.psa,
                    cp.full_mwt,
                    cp.aromatic_rings,
                    cp.heavy_atoms
                FROM referenced_molecules rm
                LEFT JOIN {DICTIONARY_TABLE} md ON md.chembl_id = rm.chembl_id
                LEFT JOIN {PROPERTIES_TABLE} cp ON cp.molregno = md.molregno
                """
            )
            row_count = cursor.rowcount

            record_build(cursor, CHEMBL_VERSION, row_count)
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "gold.dim_molecule build complete: %d rows written.",
        row_count,
    )

"""
Validate and match bronze.input_molecules against ChEMBL, producing
silver.input_molecule.
"""
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.input_molecule_silver.config import (
    POSTGRES_CONN_ID,
    SOURCE_TABLE,
    DICTIONARY_TABLE,
    TARGET_TABLE,
    NAME_OVERRIDES,
)

logger = logging.getLogger(__name__)


def build_overrides_clause() -> tuple[str, list[str]]:
    """
    Build a VALUES clause and matching flat param list from NAME_OVERRIDES.
    """
    if not NAME_OVERRIDES:
        return "(CAST(NULL AS TEXT), CAST(NULL AS TEXT))", []

    rows = ", ".join("(%s, %s)" for _ in NAME_OVERRIDES)
    params = [value for pair in NAME_OVERRIDES.items() for value in pair]
    return rows, params


def validate_and_match() -> None:
    """
    Build silver.input_molecule from bronze.input_molecules.

    Drops rows with missing compound_name. Casts molecular_weight, logp,
    ic50_nm, and assay_date individually via silver.try_cast_numeric/
    try_cast_date, nulling out any single field that doesn't parse or fails
    its validity check (molecular_weight/ic50_nm must be > 0) -- the row
    itself is kept as long as compound_name is present. Resolves chembl_id
    via a case-insensitive join against molecule_dictionary.pref_name,
    falling back through NAME_OVERRIDES for known synonym mismatches. Rows
    with no match keep chembl_id as NULL rather than being dropped.
    """
    overrides_values, overrides_params = build_overrides_clause()

    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")

            cursor.execute(
                f"""
                INSERT INTO {TARGET_TABLE} (
                    compound_id, compound_name, molecular_weight, logp,
                    ic50_nm, assay_date, lab_id, chembl_id, _source_file
                )
                WITH name_overrides (compound_name, override_name) AS (
                    VALUES {overrides_values}
                )
                SELECT
                    trim(im.compound_id) AS compound_id,
                    trim(im.compound_name) AS compound_name,
                    CASE
                        WHEN silver.try_cast_numeric(im.molecular_weight) > 0
                        THEN silver.try_cast_numeric(im.molecular_weight)
                        ELSE NULL
                    END AS molecular_weight,
                    silver.try_cast_numeric(im.logp) AS logp,
                    CASE
                        WHEN silver.try_cast_numeric(im.ic50_nm) > 0
                        THEN silver.try_cast_numeric(im.ic50_nm)
                        ELSE NULL
                    END AS ic50_nm,
                    silver.try_cast_date(im.assay_date) AS assay_date,
                    trim(im.lab_id) AS lab_id,
                    md.chembl_id,
                    im._source_file
                FROM {SOURCE_TABLE} im
                LEFT JOIN name_overrides ov
                    ON lower(trim(im.compound_name)) = lower(ov.compound_name)
                LEFT JOIN {DICTIONARY_TABLE} md
                    ON lower(coalesce(
                        ov.override_name,
                        trim(im.compound_name)
                    )) = lower(md.pref_name)
                WHERE im.compound_name IS NOT NULL
                  AND trim(im.compound_name) != ''
                """,
                overrides_params,
            )
            inserted_count = cursor.rowcount

            cursor.execute(f"SELECT count(*) FROM {SOURCE_TABLE}")
            total_input_count = cursor.fetchone()[0]

            cursor.execute(
                f"SELECT count(*) "
                f"FROM {TARGET_TABLE} "
                f"WHERE chembl_id IS NULL"
            )
            unmatched_count = cursor.fetchone()[0]

        conn.commit()
    finally:
        conn.close()

    dropped_count = total_input_count - inserted_count
    logger.info(
        "silver.input_molecule build complete: %s of %s input rows kept "
        "(%s dropped for missing compound_name), %s unmatched (no chembl_id).",
        inserted_count, total_input_count, dropped_count, unmatched_count
    )

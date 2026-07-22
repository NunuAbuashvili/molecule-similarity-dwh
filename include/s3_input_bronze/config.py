"""Configuration for ingesting S3 input CSVs into bronze."""

import os

AWS_CONN_ID = "aws_s3"
POSTGRES_CONN_ID = "mol_sim_dwh"

BUCKET_NAME = os.environ["BUCKET_NAME"]
INPUT_PREFIX = os.environ["INPUT_PREFIX"]
KEY_PATTERN = r".*batch_.*\.csv$"

TARGET_TABLE = "bronze.input_molecules"
COLUMNS: list[str] = [
    "compound_id", "compound_name",
    "molecular_weight", "logp",
    "ic50_nm", "assay_date", "lab_id"
]
COLUMN_ALIASES = {
    "IC50_nM": "ic50_nm",
    "collection_date": "assay_date",
}

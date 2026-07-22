"""
Configuration for the Tanimoto similarity computation and gold.fact_similarity.
"""

import os

from include.fingerprint_silver.config import (  # noqa: F401
    BUCKET_NAME,
    OUTPUT_PREFIX as FINGERPRINT_PREFIX,
    FINGERPRINT_SIZE,
)
from include.chembl_bronze.config import CHEMBL_VERSION  # noqa: F401

POSTGRES_CONN_ID = "mol_sim_dwh"
AWS_CONN_ID = "aws_s3"

SILVER_INPUT_TABLE = "silver.input_molecule"
TARGET_TABLE = "gold.fact_similarity"

SIMILARITY_OUTPUT_PREFIX = os.environ["SIMILARITY_OUTPUT_PREFIX"]
TOP_N = 10

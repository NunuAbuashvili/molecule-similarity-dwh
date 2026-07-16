"""
Configuration for computing Morgan fingerprints from
silver.molecule and uploading to S3.
"""

import os

from include.chembl_bronze.config import CHEMBL_VERSION  # noqa: F401

POSTGRES_CONN_ID = "mol_sim_dwh"
AWS_CONN_ID = "aws_s3"

SOURCE_TABLE = "silver.molecule"
# logical name tracked in meta.load_log
TARGET_LOG_NAME = "silver.molecule_fingerprints"

BUCKET_NAME = os.environ["BUCKET_NAME"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]

CHUNK_SIZE = 50_000
FINGERPRINT_RADIUS = 2
FINGERPRINT_SIZE = 2048

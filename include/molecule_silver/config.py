"""
Configuration for building silver.molecule from bronze ChEMBL tables.
"""

from include.chembl_bronze.config import CHEMBL_VERSION

POSTGRES_CONN_ID = "mol_sim_dwh"

STRUCTURES_TABLE = "bronze.compound_structures"
DICTIONARY_TABLE = "bronze.molecule_dictionary"
TARGET_TABLE = "silver.molecule"

CHUNK_SIZE = 50_000

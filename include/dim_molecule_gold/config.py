"""Configuration for building the gold.dim_molecule dimension table."""

from include.chembl_bronze.config import CHEMBL_VERSION  # noqa: F401


POSTGRES_CONN_ID = "mol_sim_dwh"
FACT_TABLE = "gold.fact_similarity"
DICTIONARY_TABLE = "bronze.molecule_dictionary"
PROPERTIES_TABLE = "bronze.compound_properties"
TARGET_TABLE = "gold.dim_molecule"

POSTGRES_CONN_ID = "mol_sim_dwh"

SOURCE_TABLE = "bronze.input_molecules"
DICTIONARY_TABLE = "bronze.molecule_dictionary"
TARGET_TABLE = "silver.input_molecule"

# Known compound_name -> ChEMBL pref_name overrides, confirmed manually.
NAME_OVERRIDES = {
    "paracetamol": "acetaminophen",
}

"""Configuration for ingesting ChEMBL SQLite tables into the bronze layer."""

CHEMBL_VERSION = "37"
POSTGRES_CONN_ID = "mol_sim_dwh"
BATCH_SIZE = 50_000

TABLE_CONFIGS = {
    "chembl_id_lookup": {
        "target_table": "bronze.chembl_id_lookup",
        "columns": [
            "chembl_id", "entity_type",
            "entity_id", "status", "last_active"
        ],
    },
    "molecule_dictionary": {
        "target_table": "bronze.molecule_dictionary",
        "columns": [
            "molregno", "pref_name", "chembl_id",
            "max_phase", "therapeutic_flag",
            "dosed_ingredient", "structure_type",
            "molecule_type", "first_approval", "oral",
            "parenteral", "topical", "black_box_warning",
            "natural_product", "first_in_class",
            "chirality", "prodrug", "inorganic_flag",
            "usan_year", "availability_type", "usan_stem",
            "polymer_flag", "usan_substem",
            "usan_stem_definition", "withdrawn_flag",
            "chemical_probe", "orphan", "veterinary",
        ],
    },
    "compound_properties": {
        "target_table": "bronze.compound_properties",
        "columns": [
            "molregno", "mw_freebase", "alogp",
            "hba", "hbd", "psa", "rtb", "ro3_pass",
            "num_ro5_violations", "full_mwt",
            "aromatic_rings", "heavy_atoms",
            "qed_weighted", "full_molformula",
            "np_likeness_score",
        ],
    },
    "compound_structures": {
        "target_table": "bronze.compound_structures",
        "columns": [
            "molregno", "molfile", "standard_inchi",
            "standard_inchi_key", "canonical_smiles",
        ],
    },
}

-- =============================================================================
-- Database bootstrap for the molecule-similarity DWH.
-- =============================================================================

CREATE DATABASE mol_sim_dwh;

\c mol_sim_dwh

-- Medallion architecture: bronze = raw 1:1 mirror of the ChEMBL source
-- (no transformations, no cleaning), silver = conformed/derived data
-- (e.g. full similarity tables, top-10 selection), gold = the dimensional
-- data mart (dim_molecule, fact_similarity) + presentation views.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;


DROP TABLE IF EXISTS bronze.chembl_id_lookup;
DROP TABLE IF EXISTS bronze.molecule_dictionary;
DROP TABLE IF EXISTS bronze.compound_properties;
DROP TABLE IF EXISTS bronze.compound_structures;
DROP TABLE IF EXISTS bronze.input_molecules;
DROP TABLE IF EXISTS silver.molecule;
DROP TABLE IF EXISTS silver.input_molecule;
DROP TABLE IF EXISTS gold.fact_similarity;


CREATE TABLE bronze.chembl_id_lookup (
    chembl_id       VARCHAR(20) NOT NULL,
    entity_type     VARCHAR(50) NOT NULL,
    entity_id       BIGINT NOT NULL,
    status          VARCHAR(10),
    last_active     INTEGER,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chembl_id)
);


CREATE TABLE bronze.molecule_dictionary (
    molregno                BIGINT NOT NULL,
    pref_name               VARCHAR(255),
    chembl_id               VARCHAR(20) NOT NULL,
    max_phase               NUMERIC(2, 1),
    therapeutic_flag        SMALLINT,
    dosed_ingredient        SMALLINT,
    structure_type          VARCHAR(10),
    molecule_type           VARCHAR(30),
    first_approval          INTEGER,
    oral                    SMALLINT,
    parenteral              SMALLINT,
    topical                 SMALLINT,
    black_box_warning       SMALLINT,
    natural_product         SMALLINT,
    first_in_class          SMALLINT,
    chirality               SMALLINT,
    prodrug                 SMALLINT,
    inorganic_flag          SMALLINT,
    usan_year               INTEGER,
    availability_type       SMALLINT,
    usan_stem               VARCHAR(50),
    polymer_flag            SMALLINT,
    usan_substem            VARCHAR(50),
    usan_stem_definition    VARCHAR(1000),
    withdrawn_flag          SMALLINT,
    chemical_probe          SMALLINT,
    orphan                  SMALLINT,
    veterinary              SMALLINT,
    _ingested_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (molregno),
    UNIQUE (chembl_id)
);


CREATE TABLE bronze.compound_properties (
    molregno            BIGINT NOT NULL,
    mw_freebase         NUMERIC(9, 2),
    alogp               NUMERIC(9, 2),
    hba                 INTEGER,
    hbd                 INTEGER,
    psa                 NUMERIC(9, 2),
    rtb                 INTEGER,
    ro3_pass            VARCHAR(3),
    num_ro5_violations  SMALLINT,
    full_mwt            NUMERIC(9, 2),
    aromatic_rings      INTEGER,
    heavy_atoms         INTEGER,
    qed_weighted        NUMERIC(3, 2),
    full_molformula     VARCHAR(100),
    np_likeness_score   NUMERIC(3, 2),
    _ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (molregno)
);


CREATE TABLE bronze.compound_structures (
    molregno            BIGINT NOT NULL,
    molfile             TEXT,
    standard_inchi      VARCHAR(4000),
    standard_inchi_key  VARCHAR(27),
    canonical_smiles    VARCHAR(4000),
    _ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (molregno)
);


CREATE TABLE IF NOT EXISTS meta.load_log (
    table_name     TEXT NOT NULL,
    version        TEXT NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL,
    row_count      BIGINT NOT NULL,
    PRIMARY KEY (table_name, version)
);


CREATE TABLE bronze.input_molecules (
    id                BIGSERIAL PRIMARY KEY,
    compound_id       TEXT,
    compound_name     TEXT,
    molecular_weight  TEXT,
    logp              TEXT,
    ic50_nm           TEXT,
    assay_date        TEXT,
    lab_id            TEXT,
    _source_file      TEXT NOT NULL,
    _ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE silver.molecule (
    molregno          BIGINT PRIMARY KEY,
    chembl_id         VARCHAR(20) NOT NULL UNIQUE,
    canonical_smiles  TEXT NOT NULL,
    _validated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE silver.input_molecule (
    compound_id         TEXT NOT NULL,
    compound_name       TEXT NOT NULL,
    molecular_weight    NUMERIC,
    logp                NUMERIC,
    ic50_nm             NUMERIC,
    assay_date          DATE,
    lab_id              TEXT,
    chembl_id           VARCHAR(20),
    _source_file        TEXT NOT NULL,
    _validated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE gold.fact_similarity (
    source_chembl_id    VARCHAR(20) NOT NULL,
    target_chembl_id    VARCHAR(20) NOT NULL,
    tanimoto_score      NUMERIC NOT NULL,
    rank                INT NOT NULL,
    has_duplicate_of_last_largest_score BOOLEAN NOT NULL DEFAULT FALSE,
    _computed_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE OR REPLACE FUNCTION silver.try_cast_numeric(p_text TEXT)
RETURNS NUMERIC AS $$
BEGIN
    RETURN p_text::NUMERIC;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION silver.try_cast_date(p_text TEXT)
RETURNS DATE AS $$
BEGIN
    RETURN p_text::DATE;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

# 🧪 ChEMBL Molecule Similarity Data Warehouse

An Airflow-orchestrated data warehouse that ingests the full ChEMBL compound
catalogue, computes Morgan fingerprints for ~2.4M molecules, and returns the
top-10 most structurally similar molecules (by Tanimoto similarity) for a
provided set of ~100 input molecules — served through a dimensional data mart
and a set of analytical SQL views.

Built on a medallion (bronze/silver/gold) architecture: PostgreSQL for the
warehouse, AWS S3 for fingerprint and similarity artifacts, RDKit for the
cheminformatics, and Airflow (Docker Compose) for event-driven orchestration.

---

## Engineering highlights

**Memory-bounded similarity at warehouse scale.** Scoring ~100 source
molecules against ~2.4M candidates is the crux of the pipeline. The naive
approach — accumulating `(candidate_id, score)` pairs per source — duplicates
the candidate id set 100× and needs **~15 GB** of RAM. Instead, candidate ids
are stored **once** in a single shared array, per-source scores are kept as
`float32`, and self-matches are removed by masking against the shared id array
at selection time. Footprint drops to **~1 GB** with a single streaming pass
over the fingerprint Parquet files in S3.

**Vectorized numerics, no per-row Python.** Batch scoring uses RDKit's
`BulkTanimotoSimilarity`; top-N selection uses a single stable descending sort, 
so tie-breaking is deterministic by candidate order and reproducible run-to-run; 
and score arrays flow directly into pandas/pyarrow instead of being materialized 
as millions of Python tuples.

**Compact fingerprint storage.** 2048-bit Morgan fingerprints are bit-packed
to **256 bytes** each (`np.packbits`) and stored as Parquet in S3, then
losslessly reconstructed into RDKit `ExplicitBitVect`s for scoring — an 8×
size reduction versus storing raw bit arrays.

**Idempotent, incremental, event-driven.** Every layer records
`(table, version, row_count)` in `meta.load_log` and skips work if it was
already built from the current ChEMBL version (overridable with
`force_reload=true`). DAGs are chained with **Airflow Assets**, not schedules
or manual `>>` sequencing: each DAG declares the datasets it consumes and
produces, and downstream DAGs fire automatically the moment their inputs
refresh.

**Constant-memory bulk I/O.** All ingestion uses PostgreSQL `COPY`; multi-million
row scans use **keyset pagination** (`WHERE molregno > %s ORDER BY molregno
LIMIT %s`) so memory stays flat regardless of table size; input CSVs land in
`ON COMMIT DROP` temp staging tables that carry source-file provenance.

**Correct, deterministic tie handling.** When several molecules share the
10th-place ("last largest") score and not all fit in the top-10, every affected
row is flagged `has_duplicates_of_last_largest_score`. This is computed by
comparing counts of the cutoff score inside vs. across the full candidate set —
deterministic run-to-run given the stable candidate ordering.

**Operational awareness.** Every DAG carries an `on_failure_callback` that
posts a Microsoft Teams Adaptive Card with DAG, task, error type, message, and
the exact file/line of the failing frame.

---

## Architecture

Medallion architecture (3 layers) on PostgreSQL, orchestrated by Airflow, with
Docker Compose for local deployment and AWS S3 for file artifacts.

```
                 ┌──────────────────────── S3 (surname_name/) ────────────────────────┐
                 │   fingerprints/*.parquet          similarity/<chembl_id>.parquet    │
                 └───────▲───────────────────────────────────▲────────────────────────┘
                         │ write                              │ write
  ChEMBL SQLite ──► [bronze_01] ─► bronze.compound_* ─► [silver_01] ─► silver.molecule
                                                                          │
                                                                    [silver_02] ─► fingerprints (S3)
                                                                          │
  input CSVs (S3) ─► [bronze_02] ─► bronze.input_molecules ─► [silver_03] ─► silver.input_molecule
                                                                          │
                                              [gold_01] ◄── fingerprints + input_molecule
                                                 │  ├─► gold.fact_similarity
                                                 │  └─► similarity/*.parquet (full tables, S3)
                                                 ▼
                                              [gold_02] ─► gold.dim_molecule ─► gold views
```

- **Bronze** — raw ChEMBL tables (`chembl_id_lookup`, `molecule_dictionary`,
  `compound_properties`, `compound_structures`) ingested from the ChEMBL SQLite
  bulk dump, plus the provided input molecule list ingested from S3 CSVs. No
  transformations: a 1:1 mirror of the source.
- **Silver** — validated/derived data: SMILES-validated molecules
  (`silver.molecule`), Morgan fingerprints (radius=2, nBits=2048, stored as
  Parquet in S3), and the input list matched against ChEMBL by compound name
  (`silver.input_molecule`).
- **Gold** — the data mart: `gold.fact_similarity` (top-10 Tanimoto matches per
  source molecule) and `gold.dim_molecule` (molecule properties, scoped to only
  the molecules referenced by the fact table), plus 5 analytical views.

**Tech stack:** Airflow 3.2.2 (TaskFlow API, Asset-based DAG chaining),
PostgreSQL, AWS S3, Docker Compose, RDKit, pandas/pyarrow, NumPy.

---

## Repository layout

```
dags/               Airflow DAG definitions (one file per pipeline step)
include/            Business logic, one package per DAG
postgres/
  init-mol-sim-db.sql   Core schema DDL (bronze/silver/gold tables, meta.load_log)
  gold_views.sql        The 5 analytical views
tests/              pytest unit tests, one file per include/ module
docker-compose.yaml
Dockerfile
requirements.txt
requirements-test.txt
```

---

## Setup

### Prerequisites

- Docker and Docker Compose
- An AWS SSO profile configured locally (`~/.aws/config` / SSO cache) with
  access to the `de-school-educational-data` S3 bucket
- A Microsoft Teams webhook URL for failure notifications

### 1. Configure environment

Create a `.env` file in the project root based on `.env.example`.

### 2. Build and launch

```bash
docker compose up -d --build
```

This starts Airflow (webserver, scheduler, dag-processor) and a dedicated
`mol_sim_dwh` PostgreSQL instance.

### 3. Initialize the database schema

The database is auto-initialized on startup. To apply DDL manually:

```bash
docker exec -i <postgres_container> psql -U $POSTGRES_USER -d mol_sim_dwh < postgres/init-mol-sim-db.sql
docker exec -i <postgres_container> psql -U $POSTGRES_USER -d mol_sim_dwh < postgres/gold_views.sql
```

### 4. Run the pipeline

Open the Airflow UI at `http://localhost:8080`, unpause all DAGs, then trigger
the two root DAGs:

- `bronze_01_chembl_ingestion`
- `bronze_02_s3_input_ingestion`

Everything downstream (`silver_01` → `silver_02`/`silver_03` → `gold_01` →
`gold_02`) triggers automatically via Airflow Assets once its upstream data
dependencies are satisfied — no manual sequencing. Progress can be monitored
per-DAG or in aggregate via the Airflow **Assets** view.

To force a full rebuild of any step regardless of whether it's already up to
date, trigger it with `{"force_reload": true}` in the run config.

---

## Pipeline / DAG reference

| DAG | Produces | Depends on |
|---|---|---|
| `bronze_01_chembl_ingestion` | `bronze.chembl_id_lookup`, `bronze.molecule_dictionary`, `bronze.compound_properties`, `bronze.compound_structures` | — (root) |
| `bronze_02_s3_input_ingestion` | `bronze.input_molecules` | — (root) |
| `silver_01_molecule_validation` | `silver.molecule` | `bronze_01` |
| `silver_02_fingerprint_computation` | Fingerprint Parquet files in S3 | `silver_01` |
| `silver_03_input_molecule_validation` | `silver.input_molecule` | `bronze_01`, `bronze_02` |
| `gold_01_similarity_computation` | `gold.fact_similarity`, full per-source similarity Parquet files in S3 | `silver_02`, `silver_03` |
| `gold_02_dim_molecule` | `gold.dim_molecule` | `gold_01`, `bronze_01` |

Failure notifications are sent to Microsoft Teams via `on_failure_callback` on
every DAG.

---

## Data mart

**`gold.fact_similarity`** — one row per (source molecule, one of its top-10
most similar targets): `source_chembl_id`, `target_chembl_id`,
`tanimoto_score`, `rank`, `has_duplicates_of_last_largest_score`.
Primary key `(source_chembl_id, target_chembl_id)`; indexed on
`target_chembl_id`.

**`gold.dim_molecule`** — one row per molecule referenced by
`gold.fact_similarity` (as source or target): `chembl_id`, `molecule_type`,
`mw_freebase`, `alogp`, `psa`, `cx_logp`, `molecular_species`, `full_mwt`,
`aromatic_rings`, `heavy_atoms`.

> **Note:** `cx_logp` and `molecular_species` are always `NULL` — both are
> absent from the current ChEMBL SQLite bulk dump used for ingestion.

### Analytical views (`postgres/gold_views.sql`)

| View | Description |
|---|---|
| `gold.average_similarity_score` | Average Tanimoto score per source molecule |
| `gold.similarity_property_deviation` | Average absolute `alogp` deviation between each source and its top-10 targets |
| `gold.similarity_pivot_10_sources` | Pivot of 10 chosen source molecules (rows = target, columns = source, cells = score) via `FILTER`ed aggregation |
| `gold.similarity_neighbor_chain` | Every top-10 row, plus the next-most-similar target within the source (`LEAD`) and the source's 2nd-most-similar target |
| `gold.average_similarity_grouped` | Average similarity across 4 grains — source / (aromatic_rings, heavy_atoms) / heavy_atoms / whole dataset — in one `GROUPING SETS` scan (no `UNION`); rolled-up cells labeled `'TOTAL'` |

---

## Example results

**`gold.fact_similarity`** (top matches for one source):

| source_chembl_id | target_chembl_id | tanimoto_score     | rank | has_duplicates_of_last_largest_score |
|------------------|------------------|--------------------|------|---|
| CHEMBL12         | CHEMBL543191       | 0.9743589743589743 | 1    | false |
| CHEMBL12         | CHEMBL286346    | 0.85               | 2    | false |
| CHEMBL12         | CHEMBL65087      | 0.7727272727272727             | 3    | false |
| …                | …                | …                  | …    | … |
| CHEMBL12         | CHEMBL368768     | 0.7272727272727273 | 10   | true |

**`gold.average_similarity_score`:**

| source_chembl_id | average_tanimoto_score |
|---|---|
| CHEMBL1491 | 0.8345 |
| CHEMBL521 | 0.8933 |

**`gold.similarity_neighbor_chain`** (one source shown):

| source_chembl_id | target_chembl_id | tanimoto_score | next_most_similar_target_chembl_id | second_most_similar_target_chembl_id |
|---|---|----------------|---|---|
| CHEMBL1059 | CHEMBL88034 | 1              | CHEMBL167003 | CHEMBL167003 |
| CHEMBL1059 | CHEMBL167003  | 1              | CHEMBL418897    | CHEMBL167003 |
| CHEMBL1059 | CHEMBL418897     | 0.6666666666666666         | CHEMBL190074     | CHEMBL167003 |

**`gold.average_similarity_grouped`** (rollup labeling):

| source_chembl_id | aromatic_rings | heavy_atoms | average_tanimoto_score |
|---|---|---|---|
| CHEMBL1491 | TOTAL | TOTAL | 0.8345 |
| TOTAL    | 3     | 32    | 0.8519 |
| TOTAL    | TOTAL | 26    | 0.8314 |
| TOTAL    | TOTAL | TOTAL | 0.8123 |

---

## Known data notes

**Compound name mismatch (`paracetamol` / `acetaminophen`).** Matching the
input CSVs' `compound_name` against ChEMBL's `molecule_dictionary.pref_name` is
otherwise a direct case-insensitive join, but one molecule didn't match: the
input file uses "Paracetamol," while ChEMBL's preferred name for the same
compound is "Acetaminophen." Confirmed manually that these refer to the same
molecule, and resolved with an explicit, auditable override in
`include/input_molecule_silver/config.py`:

```python
# Known compound_name -> ChEMBL pref_name overrides, confirmed manually.
NAME_OVERRIDES = {
    "paracetamol": "acetaminophen",
}
```

---

## Testing

Tests run inside the same Airflow container as production, to guarantee
identical dependency versions:

```bash
docker compose exec airflow-scheduler pytest tests/
```

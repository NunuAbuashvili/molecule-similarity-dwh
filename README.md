# molecule-similarity-dwh
ETL pipeline that ingests ChEMBL molecule data, computes Morgan fingerprints, and finds each molecule's top-10 most similar compounds via Tanimoto similarity — orchestrated with Airflow, stored in S3 + Postgres DWH.

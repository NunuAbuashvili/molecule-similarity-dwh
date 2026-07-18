from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.similarity_gold.config import POSTGRES_CONN_ID
from include.similarity_gold.similarity import (
    get_query_molecules,
    fetch_source_fingerprints,
    compute_and_upload_similarity,
)


@dag(
    schedule=None,
    catchup=False,
    tags=["gold", "similarity"],
    params={"force_reload": False},
)
def gold_01_similarity_computation():

    @task
    def fetch_sources() -> list[dict]:
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        try:
            with conn.cursor() as cursor:
                source_ids = get_query_molecules(cursor)
        finally:
            conn.close()

        return fetch_source_fingerprints(source_ids)

    @task
    def compute_similarity(source_fingerprints: list[dict]) -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        compute_and_upload_similarity(
            source_fingerprints,
            force_reload=force_reload
        )

    compute_similarity(fetch_sources())


gold_01_similarity_computation()

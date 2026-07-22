from datetime import timedelta

from airflow.sdk import dag, task, get_current_context, Asset

from include.dim_molecule_gold.dimension import build_dim_molecule
from include.notifications.teams import notify_task_failure


@dag(
    schedule=[Asset("gold.fact_similarity"), Asset("bronze.chembl_tables")],
    catchup=False,
    tags=["gold", "dimension"],
    params={"force_reload": False},
    default_args={"retries": 2, "on_failure_callback": notify_task_failure},
)
def gold_02_dim_molecule():

    @task(
        outlets=[Asset("gold.dim_molecule")],
        execution_timeout=timedelta(minutes=15)
    )
    def build() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        build_dim_molecule(force_reload=force_reload)

    build()


gold_02_dim_molecule()

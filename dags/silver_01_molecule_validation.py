from datetime import timedelta

from airflow.sdk import dag, task, get_current_context, Asset

from include.molecule_silver.validation import run_validation
from include.notifications.teams import notify_task_failure


@dag(
    schedule=[Asset("bronze.chembl_tables")],
    catchup=False,
    tags=["silver", "molecule"],
    params={"force_reload": False},
    default_args={"retries": 2, "on_failure_callback": notify_task_failure},
)
def silver_01_molecule_validation():

    @task(
        outlets=[Asset("silver.molecule")],
        execution_timeout=timedelta(hours=1)
    )
    def build_silver_molecule() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        run_validation(force_reload=force_reload)

    build_silver_molecule()


silver_01_molecule_validation()

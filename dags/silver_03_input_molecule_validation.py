from datetime import timedelta

from airflow.sdk import dag, task, Asset

from include.input_molecule_silver.validation import validate_and_match
from include.notifications.teams import notify_task_failure


@dag(
    schedule=[Asset("bronze.input_molecules"), Asset("bronze.chembl_tables")],
    catchup=False,
    tags=["silver", "input_molecule"],
    default_args={"retries": 2, "on_failure_callback": notify_task_failure},
)
def silver_03_input_molecule_validation():

    @task(outlets=[Asset("silver.input_molecule")], execution_timeout=timedelta(minutes=30))
    def build_silver_input_molecule() -> None:
        validate_and_match()

    build_silver_input_molecule()


silver_03_input_molecule_validation()

from airflow.sdk import dag, task

from include.input_molecule_silver.validation import validate_and_match
from include.notifications.teams import notify_task_failure


@dag(
    schedule=None,
    catchup=False,
    tags=["silver", "input_molecule"],
    default_args={"on_failure_callback": notify_task_failure},
)
def silver_03_input_molecule_validation():

    @task
    def build_silver_input_molecule() -> None:
        validate_and_match()

    build_silver_input_molecule()


silver_03_input_molecule_validation()

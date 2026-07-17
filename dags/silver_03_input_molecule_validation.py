from airflow.sdk import dag, task

from include.input_molecule_silver.validation import validate_and_match


@dag(
    schedule=None,
    catchup=False,
    tags=["silver", "input_molecule"],
)
def silver_03_input_molecule_validation():

    @task
    def build_silver_input_molecule() -> None:
        validate_and_match()

    build_silver_input_molecule()


silver_03_input_molecule_validation()

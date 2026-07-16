from airflow.sdk import dag, task, get_current_context

from include.molecule_silver.validation import run_validation


@dag(
    schedule=None,
    catchup=False,
    tags=["silver", "molecule"],
    params={"force_reload": False},
)
def molecule_silver_transformation():

    @task
    def build_silver_molecule() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        run_validation(force_reload=force_reload)

    build_silver_molecule()


molecule_silver_transformation()

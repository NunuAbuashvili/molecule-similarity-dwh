from airflow.sdk import dag, task, get_current_context

from include.molecule_silver.validation import run_validation
from include.notifications.teams import notify_task_failure


@dag(
    schedule=None,
    catchup=False,
    tags=["silver", "molecule"],
    params={"force_reload": False},
    default_args={"on_failure_callback": notify_task_failure},
)
def silver_01_molecule_validation():

    @task
    def build_silver_molecule() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        run_validation(force_reload=force_reload)

    build_silver_molecule()


silver_01_molecule_validation()

from datetime import timedelta

from airflow.sdk import dag, task, get_current_context, Asset

from include.fingerprint_silver.fingerprints import (
    run_fingerprint_computation
)
from include.notifications.teams import notify_task_failure


@dag(
    schedule=[Asset("silver.molecule")],
    catchup=False,
    tags=["silver", "fingerprints"],
    params={"force_reload": False},
    default_args={"retries": 2, "on_failure_callback": notify_task_failure},
)
def silver_02_fingerprint_computation():

    @task(
        outlets=[Asset("silver.fingerprints")],
        execution_timeout=timedelta(hours=2)
    )
    def compute_fingerprints() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        run_fingerprint_computation(force_reload=force_reload)

    compute_fingerprints()


silver_02_fingerprint_computation()

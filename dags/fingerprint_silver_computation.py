from airflow.sdk import dag, task, get_current_context

from include.fingerprint_silver.fingerprints import (
    run_fingerprint_computation
)


@dag(
    schedule=None,
    catchup=False,
    tags=["silver", "fingerprints"],
    params={"force_reload": False},
)
def fingerprint_silver_computation():

    @task
    def compute_fingerprints() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        run_fingerprint_computation(force_reload=force_reload)

    compute_fingerprints()


fingerprint_silver_computation()

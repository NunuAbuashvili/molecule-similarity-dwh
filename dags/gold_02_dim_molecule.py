from airflow.sdk import dag, task, get_current_context

from include.dim_molecule_gold.dimension import build_dim_molecule
from include.notifications.teams import notify_task_failure


@dag(
    schedule=None,
    catchup=False,
    tags=["gold", "dimension"],
    params={"force_reload": False},
    default_args={"on_failure_callback": notify_task_failure},
)
def gold_02_dim_molecule():

    @task
    def build() -> None:
        context = get_current_context()
        force_reload = context["params"]["force_reload"]
        build_dim_molecule(force_reload=force_reload)

    build()


gold_02_dim_molecule()

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dbtdir = "/opt/airflow/dbt"
USER_BIN = "/home/airflow/.local/bin"  # where the dbt CLI lands in this image

with DAG(
    dag_id="dbt_build_daily",
    schedule="0 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "data"},
    tags=["dbt", "silver"],
):
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f'export PATH="$PATH:{USER_BIN}"; ' f"cd {dbtdir} && dbt deps --profiles-dir {dbtdir}"
        ),
        env={"DBT_PROFILES_DIR": dbtdir},
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f'export PATH="$PATH:{USER_BIN}"; '
            f"cd {dbtdir} && dbt build --target dev --fail-fast --profiles-dir {dbtdir}"
        ),
        env={"DBT_PROFILES_DIR": dbtdir},
    )
    deps >> build

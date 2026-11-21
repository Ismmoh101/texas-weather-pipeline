"""Daily Airflow 3 DAG: ingest new weather data, then build and test dbt models.

Point Airflow at this folder (AIRFLOW__CORE__DAGS_FOLDER) and set
WEATHER_PROJECT_ROOT to the repo root.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

PROJECT_ROOT = os.environ.get(
    "WEATHER_PROJECT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
WAREHOUSE = os.environ.get("WEATHER_WAREHOUSE", "duckdb")
DBT_TARGET = "prod" if WAREHOUSE == "snowflake" else "dev"


def ingest(**_):
    sys.path.insert(0, PROJECT_ROOT)
    from pipeline.run import run

    result = run(warehouse=WAREHOUSE)
    if result["status"] != "success":
        raise RuntimeError(f"Ingestion failed: {result}")
    return result


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="texas_weather_pipeline",
    description="Open-Meteo -> warehouse -> dbt marts",
    start_date=datetime(2026, 1, 1),
    schedule="0 7 * * *",  # daily, 07:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "dbt"],
) as dag:
    extract_load = PythonOperator(task_id="extract_load", python_callable=ingest)

    dbt_env = f"cd {PROJECT_ROOT}/dbt && DBT_TARGET={DBT_TARGET} "
    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=dbt_env + "dbt seed --profiles-dir .")
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness", bash_command=dbt_env + "dbt source freshness --profiles-dir ."
    )
    dbt_build = BashOperator(task_id="dbt_build", bash_command=dbt_env + "dbt build --profiles-dir .")

    extract_load >> dbt_seed >> dbt_source_freshness >> dbt_build

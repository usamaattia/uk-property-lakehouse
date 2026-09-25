"""
### UK Property Lakehouse: monthly refresh

HM Land Registry publishes the Price Paid monthly update around the 20th working day of
each month. This DAG runs on the 25th and:

1. **ingest**: lands the monthly change file (and backfills yearly files on the very first run)
2. **check_for_new_data**: skips everything downstream if the file had already been landed
3. **dbt_build**: bronze → silver (CDC merge) → gold, with every data test and unit test
4. **export_site**: writes the dashboard JSON from the gold tables

Set `LAKEHOUSE_HOME` to the repo checkout and `LANDING_URI` / `DBT_TARGET` as for the CLI.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

HOME = os.environ.get("LAKEHOUSE_HOME", "/opt/lakehouse")
LANDING_URI = os.environ.get("LANDING_URI", f"{HOME}/data/landing")
DBT = f"dbt build --project-dir {HOME}/transform --profiles-dir {HOME}/transform"


@dag(
    dag_id="uk_property_lakehouse",
    schedule="0 6 25 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=15)},
    tags=["lakehouse", "land-registry", "dbt"],
    doc_md=__doc__,
)
def uk_property_lakehouse():
    @task
    def ingest() -> list[dict]:
        from lakehouse.landing import open_landing
        from lakehouse.pipeline import run_monthly

        backfill = int(os.environ.get("BACKFILL_YEARS", "3"))
        return [r.__dict__ for r in run_monthly(open_landing(LANDING_URI), backfill)]

    @task.short_circuit
    def check_for_new_data(results: list[dict]) -> bool:
        return any(r["status"] == "landed" for r in results)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT,
        cwd=HOME,
        env={"LANDING_URI": LANDING_URI},
        append_env=True,
    )

    export_site = BashOperator(
        task_id="export_site",
        bash_command="lakehouse export-site",
        cwd=HOME,
        append_env=True,
    )

    check_for_new_data(ingest()) >> dbt_build >> export_site


uk_property_lakehouse()

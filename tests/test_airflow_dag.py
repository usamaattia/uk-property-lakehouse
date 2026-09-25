"""DAG integrity check. Runs where Airflow is installed (a separate CI job)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "orchestration" / "airflow" / "dags"


def test_dag_imports_and_has_expected_shape():
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    dag = bag.dags["uk_property_lakehouse"]
    edges = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
    assert edges == {
        "ingest": ["check_for_new_data"],
        "check_for_new_data": ["dbt_build"],
        "dbt_build": ["export_site"],
        "export_site": [],
    }
    assert dag.max_active_runs == 1 and not dag.catchup

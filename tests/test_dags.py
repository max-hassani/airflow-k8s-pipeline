"""DAG import and structure checks.

Skipped unless Airflow is importable, so the suite still runs on a laptop
without it. CI runs these inside the real `apache/airflow:3.2.2` image, which is
the only place the result is meaningful -- a DAG that imports against some other
Airflow version proves nothing about the one it will actually run on.
"""

from __future__ import annotations

import sys

import pytest
from conftest import REPO_ROOT

pytest.importorskip("airflow", reason="Airflow is only present in the CI image")

# `airflow.models.dagbag` still works but is deprecated in 3.x; the dag_processing
# path is where it lives now.
from airflow.dag_processing.dagbag import DagBag

DAG_FOLDERS = ["dags/local", "dags/pipelines"]
EXPECTED = {
    "dags/local": {"00_smoke_test"},
    "dags/pipelines": {"protein_embedder", "molecule_embedder"},
}


def _bag(folder: str) -> DagBag:
    resolved = str((REPO_ROOT / folder).resolve())
    # Airflow puts a bundle's directory on sys.path when it loads it, which is
    # why `from _inference_pipeline import build_pipeline` resolves in the
    # cluster. A bare DagBag does not, so without this the test would fail on
    # DAGs that work perfectly in production.
    if resolved not in sys.path:
        sys.path.insert(0, resolved)
    return DagBag(dag_folder=resolved, include_examples=False)


@pytest.mark.parametrize("folder", DAG_FOLDERS)
def test_dags_import_without_error(folder: str):
    bag = _bag(folder)
    assert not bag.import_errors, "DAG import failed:\n" + "\n".join(
        f"{p}: {e}" for p, e in bag.import_errors.items()
    )
    assert set(bag.dags) == EXPECTED[folder]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED["dags/pipelines"]))
def test_pipeline_has_a_dag_level_retry_policy(dag_id: str):
    """Retries live in default_args so they cover EVERY task -- including
    discover_jobs, which talks to the API server and MinIO and is exactly as
    able to hit a transient failure as the pod stages are."""
    dag = _bag("dags/pipelines").dags[dag_id]
    assert dag.tasks, f"{dag_id} has no tasks"
    for task in dag.tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id} has no retries"
        assert task.execution_timeout is not None, (
            f"{dag_id}.{task.task_id} has no execution_timeout; a wedged pod would hang the run"
        )


@pytest.mark.parametrize("dag_id", sorted(EXPECTED["dags/pipelines"]))
def test_pipeline_declares_a_deadline_not_a_removed_sla(dag_id: str):
    """`sla=` still exists as an operator parameter in Airflow 3 but is
    discarded with only a UserWarning, so a ported Airflow 2 SLA looks
    configured and does nothing. The replacement is a DAG-level deadline."""
    dag = _bag("dags/pipelines").dags[dag_id]
    assert getattr(dag, "deadline", None) is not None, f"{dag_id} has no deadline configured"
    for task in dag.tasks:
        assert not getattr(task, "sla", None), (
            f"{dag_id}.{task.task_id} sets sla=, which Airflow 3 silently ignores"
        )


@pytest.mark.parametrize("dag_id", sorted(EXPECTED["dags/pipelines"]))
def test_pipeline_stages_run_in_order(dag_id: str):
    """preprocess -> infer -> postprocess, per mapped job.

    They communicate only through the shared volume, so a reordering would not
    fail loudly -- infer would read a manifest that preprocess had not written
    yet, and the message would point at a missing file rather than at the graph.
    """
    dag = _bag("dags/pipelines").dags[dag_id]
    by_id = {t.task_id.rsplit(".", 1)[-1]: t for t in dag.tasks}
    for stage in ("preprocess", "infer", "postprocess"):
        assert stage in by_id, f"{dag_id} is missing the {stage} stage"

    def downstream(task):
        return {t.rsplit(".", 1)[-1] for t in task.downstream_task_ids}

    assert "infer" in downstream(by_id["preprocess"])
    assert "postprocess" in downstream(by_id["infer"])
    assert not downstream(by_id["postprocess"])


@pytest.mark.parametrize("dag_id", sorted(EXPECTED["dags/pipelines"]))
def test_job_pods_get_no_kubernetes_api_token(dag_id: str):
    """Job pods run user-supplied job logic and never call the Kubernetes API.

    The ServiceAccount they use has no bindings and no mounted token; if a DAG
    stopped requesting it, the pods would silently fall back to `default`.
    """
    dag = _bag("dags/pipelines").dags[dag_id]
    pod_tasks = [t for t in dag.tasks if hasattr(t, "service_account_name")]
    assert pod_tasks, f"{dag_id} has no KubernetesPodOperator tasks"
    for task in pod_tasks:
        assert task.service_account_name == "inference-worker", (
            f"{dag_id}.{task.task_id} runs as {task.service_account_name!r}"
        )

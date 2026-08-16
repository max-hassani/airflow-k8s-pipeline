"""Shared builder for the batch-inference pipelines.

Both pipeline DAGs are the same shape -- read a list of job specs, run each
through preprocess/infer/postprocess as KubernetesPodOperator pods -- differing
only in which Airflow Variable holds their specs. That shape lives here once.

This module defines no DAG of its own. Airflow's dag processor will still scan
it (it mentions `airflow`), find nothing, and move on.

Two structural choices worth understanding
------------------------------------------
*Mapped task group, not mapped operators.* Chaining three independently
expanded operators (`pre.expand(...) >> infer.expand(...)`) would put a barrier
between the stages: every preprocess in the DAG must finish before any infer
starts. Mapping the whole group instead gives each job its own independent
three-stage chain, so a slow job never holds up a fast one.

*Specs are pre-serialised to JSON.* Mapping over JSON strings rather than dicts
means each expanded task receives one already-encoded argument, which the
worker parses with `--spec-json`. The alternative -- one operator flag per spec
field -- makes the operator signature grow every time the spec does.
"""

from __future__ import annotations

import json
import os

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG, Variable, task_group
from kubernetes.client import models as k8s

# Injected by the inference-pipeline-config ConfigMap (see the chart's
# templates/configmap.yaml). DAG code cannot read Helm values, so anything
# environment-specific arrives as environment -- no image tags or mount paths
# are hardcoded here.
WORKER_IMAGE = os.environ.get("INFERENCE_WORKER_IMAGE", "inference/worker:local")
WORKER_PULL_POLICY = os.environ.get("INFERENCE_WORKER_IMAGE_PULL_POLICY", "IfNotPresent")
WORKER_SERVICE_ACCOUNT = os.environ.get("INFERENCE_WORKER_SERVICE_ACCOUNT", "inference-worker")
SCRATCH_PATH = os.environ.get("INFERENCE_SCRATCH_PATH", "/scratch")
SCRATCH_CLAIM = os.environ.get("INFERENCE_SCRATCH_CLAIM", "inference-scratch")

# Credentials, endpoint and bucket names, injected wholesale so rotating the
# Secret or renaming a bucket never touches a DAG.
_MINIO_ENV = [
    k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="inference-minio-credentials")),
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="inference-pipeline-config")),
]

# The scratch PVC is what carries bulk data between the three pods. The same
# claim is mounted into the Airflow pods by the chart, which is what makes it
# shared rather than merely present.
_SCRATCH_VOLUME = k8s.V1Volume(
    name="scratch",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=SCRATCH_CLAIM),
)
_SCRATCH_MOUNT = k8s.V1VolumeMount(name="scratch", mount_path=SCRATCH_PATH)

# Job pods run as the Airflow uid/gid so they and the Airflow pods can both
# write the shared volume without it being world-writable.
_SECURITY_CONTEXT = {"runAsUser": 50000, "runAsGroup": 0, "fsGroup": 0}

# Stage name -> resources. `infer` gets more memory because it materialises the
# whole (n_rows, 512) float32 array; the other two only stream bytes.
_STAGE_RESOURCES = {
    "preprocess": k8s.V1ResourceRequirements(
        requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
    ),
    "infer": k8s.V1ResourceRequirements(
        requests={"cpu": "250m", "memory": "256Mi"}, limits={"cpu": "1", "memory": "512Mi"}
    ),
    "postprocess": k8s.V1ResourceRequirements(
        requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
    ),
}


def _stage(stage: str, spec_json) -> KubernetesPodOperator:
    """One pipeline stage as a pod.

    `spec_json` is a mapped argument: Airflow resolves it per expanded instance
    when it renders the templated `arguments` field. dag_id and run_id come
    through as Airflow templates so the worker can scope its scratch directory
    per run without the DAG passing its own identity around.
    """
    return KubernetesPodOperator(
        task_id=stage,
        name=f"inference-{stage}",
        # No `namespace`: in-cluster config resolves it from the pod's own
        # ServiceAccount, so this carries no assumption about namespace or
        # Helm release name.
        image=WORKER_IMAGE,
        image_pull_policy=WORKER_PULL_POLICY,
        arguments=[
            "--stage", stage,
            "--spec-json", spec_json,
            "--work-root", SCRATCH_PATH,
            "--dag-id", "{{ dag.dag_id }}",
            "--run-id", "{{ run_id }}",
        ],
        env_from=_MINIO_ENV,
        volumes=[_SCRATCH_VOLUME],
        volume_mounts=[_SCRATCH_MOUNT],
        security_context=_SECURITY_CONTEXT,
        service_account_name=WORKER_SERVICE_ACCOUNT,
        in_cluster=True,
        # Stream pod stdout into the Airflow task log, so debugging starts in
        # the UI rather than with kubectl.
        get_logs=True,
        # Small result metadata (row counts, output keys) back to Airflow.
        # Bulk data stays on scratch: XCom values live in the metadata DB.
        do_xcom_push=True,
        on_finish_action="delete_pod",
        container_resources=_STAGE_RESOURCES[stage],
    )


def build_pipeline(
    *,
    dag_id: str,
    jobs_variable: str,
    model_family: str,
    description: str,
    schedule=None,
) -> DAG:
    """Build one inference pipeline DAG.

    :param jobs_variable: Airflow Variable holding the job spec list. Read at
        parse time with a `[]` default so the DAG still imports cleanly before
        the Variable is seeded -- an unseeded environment shows an empty DAG
        rather than a broken one.
    """
    # `default=`, not `default_var=`. The Airflow 3 Task SDK's Variable.get has
    # a different signature from the old airflow.models.Variable, which still
    # exists and still takes `default_var` -- an easy way to write code that
    # looks right and fails at parse time.
    specs = Variable.get(jobs_variable, default=[], deserialize_json=True)
    spec_payloads = [json.dumps(s) for s in specs]

    with DAG(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=["inference", model_family],
        default_args={
            # Pod scheduling and image pulls are the flaky part of this
            # pipeline, not the logic; two retries with a short backoff clears
            # transient node pressure without masking a real failure.
            "retries": 2,
            "retry_delay": pendulum.duration(seconds=30),
            # A stage that has not finished in 10 minutes is wedged, not slow:
            # the mocked worker's real work is milliseconds.
            "execution_timeout": pendulum.duration(minutes=10),
        },
        # Keep one run per DAG at a time so concurrent runs cannot interleave
        # on the shared scratch volume.
        max_active_runs=1,
    ) as dag:

        @task_group
        def run_job(spec_json):
            """preprocess -> infer -> postprocess, for a single job spec."""
            _stage("preprocess", spec_json) >> _stage("infer", spec_json) >> _stage("postprocess", spec_json)

        run_job.expand(spec_json=spec_payloads)

    return dag

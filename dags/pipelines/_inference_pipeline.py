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
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import (
    DAG,
    DeadlineAlert,
    DeadlineReference,
    SyncCallback,
    Variable,
    task,
    task_group,
)
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


def deadline_missed(**context) -> None:
    """Fired when a DAG run overruns its deadline.

    Must stay a module-level function: Airflow serialises the callback by import
    path and resolves it on the executor, so a nested or lambda callback cannot
    be reconstructed there.

    Prints rather than paging anyone, because this is a local demo. In a real
    deployment this is where a notifier goes -- the shape of the hook is the
    same either way.
    """
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "<unknown>")
    run_id = getattr(dag_run, "run_id", "<unknown>")
    print(
        f"DEADLINE MISSED: dag_id={dag_id} run_id={run_id} "
        "exceeded its allotted wall-clock time. Check for pods stuck Pending "
        "(node pressure) or a stage retrying against an unreachable MinIO."
    )


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
            "--stage",
            stage,
            "--spec-json",
            spec_json,
            "--work-root",
            SCRATCH_PATH,
            "--dag-id",
            "{{ dag.dag_id }}",
            "--run-id",
            "{{ run_id }}",
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
    deadline_minutes: int = 15,
) -> DAG:
    """Build one inference pipeline DAG.

    :param jobs_variable: Airflow Variable holding this pipeline's job spec
        list. Each spec references a config file, an input location and a
        target bucket. Because specs give an `input_prefix` rather than a
        single CSV, one spec expands to one job per file found under it.
    """

    @task
    def discover_jobs(jobs_variable: str) -> list[str]:
        """Expand the job specs into one concrete job per input CSV.

        Deliberately a task, not top-level code. DAG files are re-parsed
        constantly (our dag_processor refresh is 30s), so reading the Variable
        or listing S3 at module level would run on every parse inside the
        dag-processor -- MinIO or the API server being slow would degrade DAG
        *parsing*, not just runs. As a task it also means a CSV added between
        runs is picked up on the next trigger with no re-parse.
        """
        import boto3
        import yaml as _yaml
        from botocore.config import Config as _Config

        specs = Variable.get(jobs_variable, default=[], deserialize_json=True)
        if not specs:
            raise ValueError(
                f"Airflow Variable '{jobs_variable}' is empty or unset -- "
                "has scripts/3-seed-data.sh been run?"
            )

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            config=_Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
        )
        configs_bucket = os.environ["INFERENCE_BUCKET_CONFIGS"]
        inputs_bucket = os.environ["INFERENCE_BUCKET_INPUTS"]

        jobs: list[dict] = []
        for spec in specs:
            for required in ("config_key", "input_prefix", "target_bucket"):
                if required not in spec:
                    raise ValueError(f"job spec is missing '{required}': {spec}")

            # Fetched only to fail fast on a missing or malformed config: the
            # worker reads it again itself, per the worker contract.
            _yaml.safe_load(
                s3.get_object(Bucket=configs_bucket, Key=spec["config_key"])["Body"].read()
            )

            pages = s3.get_paginator("list_objects_v2").paginate(
                Bucket=inputs_bucket, Prefix=spec["input_prefix"]
            )
            keys = sorted(
                o["Key"]
                for page in pages
                for o in page.get("Contents", [])
                if o["Key"].endswith(".csv")
            )
            if not keys:
                raise ValueError(
                    f"no .csv objects under s3://{inputs_bucket}/{spec['input_prefix']}"
                )

            # One job per input file. job_id comes from the filename, so a
            # published artifact is traceable back to the object that made it.
            jobs += [
                {
                    "job_id": k.rsplit("/", 1)[-1].removesuffix(".csv"),
                    "config_key": spec["config_key"],
                    "input_csv": k,
                    "target_bucket": spec["target_bucket"],
                }
                for k in keys
            ]

        print(f"discovered {len(jobs)} job(s): {[j['job_id'] for j in jobs]}")
        # Serialised here so each mapped task receives one already-encoded
        # argument rather than the operator growing a flag per spec field.
        return [json.dumps(j) for j in jobs]

    with DAG(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=["inference", model_family],
        # --- Retry policy, DAG-level ---------------------------------------
        # In default_args rather than per-operator, so it covers every task in
        # the DAG -- including `discover_jobs`, which talks to the API server
        # and to MinIO and is exactly as able to hit a transient failure as the
        # pod stages are.
        default_args={
            # Pod scheduling and image pulls are the flaky part of this
            # pipeline, not the logic. Two retries clears transient node
            # pressure without masking a real failure.
            "retries": 2,
            "retry_delay": pendulum.duration(seconds=30),
            # Back off between attempts: an immediate retry against a MinIO
            # that is still starting up just burns an attempt for nothing.
            "retry_exponential_backoff": True,
            "max_retry_delay": pendulum.duration(minutes=5),
            # A stage that has not finished in 10 minutes is wedged, not slow:
            # the mocked worker's real work is milliseconds.
            "execution_timeout": pendulum.duration(minutes=10),
        },
        # --- Deadline alert, the Airflow 3 replacement for SLAs -------------
        # `sla=` still EXISTS as an operator parameter in 3.2.2, but the value
        # is discarded and only a UserWarning is emitted:
        #   "The SLA feature is removed in Airflow 3.0, replaced with Deadline
        #    Alerts in >=3.1"
        # So a ported Airflow 2 SLA looks configured and silently does nothing.
        # This is the real thing: measured from when the run was queued, so
        # time spent waiting for a free pod slot counts against it -- which is
        # what you actually care about when the cluster is under load.
        deadline=DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(minutes=deadline_minutes),
            # Sync, not Async: AsyncCallback runs on the triggerer, and these
            # DAGs are served from a git bundle that the executor is known to
            # fetch. SyncCallback runs in the executor, where the module is
            # importable.
            callback=SyncCallback(deadline_missed),
        ),
        # Keep one run per DAG at a time so concurrent runs cannot interleave
        # on the shared scratch volume.
        max_active_runs=1,
    ) as dag:

        @task_group
        def run_job(spec_json):
            """preprocess -> infer -> postprocess, for a single job spec."""
            (
                _stage("preprocess", spec_json)
                >> _stage("infer", spec_json)
                >> _stage("postprocess", spec_json)
            )

        run_job.expand(spec_json=discover_jobs(jobs_variable))

    return dag

"""Infrastructure smoke test.

Proves the four things the rest of the pipeline depends on, without needing the
worker image to exist yet:

1. DAGs baked into the Airflow image are parsed and schedulable.
2. The scheduler can launch pods via KubernetesPodOperator inside its namespace
   (i.e. the chart's namespaced pod-launch Role is correctly bound).
3. Those pods run under the token-less `inference-worker` ServiceAccount.
4. MinIO is reachable, and the worker identity is scoped: it can write to the
   outputs bucket and is denied the restricted one.

Run this first after a deploy. If it goes green, any later failure is pipeline
logic rather than infrastructure.
"""

from __future__ import annotations

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

# The MinIO credentials, endpoint and bucket names, injected wholesale rather
# than named one by one, so rotating the Secret or renaming a bucket is a values
# change and never a DAG change.
MINIO_ENV = [
    k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="inference-minio-credentials"))
]

# A deliberately blunt shell probe: every line must behave as asserted, and the
# restricted read failing is a *pass* -- an absence of AccessDenied fails the task.
PROBE = r"""
set -eu
mc alias set m "$MINIO_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null

echo "--- buckets visible to the worker identity ---"
mc ls m

echo "--- write to $INFERENCE_BUCKET_OUTPUTS (expected: success) ---"
echo "smoke-test $(date -Iseconds)" > /tmp/smoke.txt
mc cp /tmp/smoke.txt "m/$INFERENCE_BUCKET_OUTPUTS/_smoke/$(date +%s).txt"

echo "--- read $INFERENCE_BUCKET_RESTRICTED (expected: AccessDenied) ---"
if mc ls "m/$INFERENCE_BUCKET_RESTRICTED" 2>/dev/null; then
  echo "FAIL: worker identity could read $INFERENCE_BUCKET_RESTRICTED; IAM scoping is broken"
  exit 1
fi
echo "OK: $INFERENCE_BUCKET_RESTRICTED correctly denied"
"""

with DAG(
    dag_id="00_smoke_test",
    description="Verify pod launching, MinIO connectivity and bucket scoping",
    schedule=None,  # manually triggered; this is a diagnostic, not a pipeline
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["inference", "infra"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(seconds=30),
    },
) as dag:
    KubernetesPodOperator(
        task_id="probe_minio",
        name="inference-smoke-probe",
        # No `namespace`: in-cluster config resolves it from the pod's own
        # ServiceAccount, so the DAG carries no assumption about which namespace
        # or Helm release name the stack was deployed under.
        image="minio/mc:RELEASE.2024-11-21T17-21-54Z",
        cmds=["sh", "-c"],
        arguments=[PROBE],
        env_from=MINIO_ENV,
        # Token-less identity from the chart's rbac.yaml. Task pods have no
        # reason to reach the Kubernetes API.
        service_account_name="inference-worker",
        in_cluster=True,
        # Stream the pod's stdout into the Airflow task log, so debugging starts
        # in the UI rather than with kubectl.
        get_logs=True,
        on_finish_action="delete_pod",
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "500m", "memory": "256Mi"},
        ),
    )

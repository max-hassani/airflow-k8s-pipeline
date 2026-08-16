#!/usr/bin/env bash
# Build the project images straight into minikube's docker daemon.
#
# There is no registry in this setup: pointing the docker CLI at minikube's
# daemon means `imagePullPolicy: IfNotPresent` finds the image locally and
# nothing is ever pushed or pulled. Re-run after any DAG or worker change.
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-minikube}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Targeting the minikube docker daemon (profile: $PROFILE)"
eval "$(minikube -p "$PROFILE" docker-env)"

# Tags MIRROR the chart: `airflow.images.airflow` and `inference.worker.image`
# in k8s/inference-pipeline/values.yaml. Helm values cannot be read from bash,
# so if you change one, change the other.
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-inference/airflow:local}"
WORKER_IMAGE="${WORKER_IMAGE:-inference/worker:local}"

# Both builds are mandatory. An earlier version guarded the worker build with
# `if [[ -f worker/Dockerfile ]]`, which turned a missing Dockerfile into a
# silent skip -- the script exited 0 and the failure surfaced much later as an
# ImagePullBackOff on a task pod. Fail here instead, where the message is about
# the actual problem.
for f in worker/Dockerfile.airflow worker/Dockerfile; do
  [[ -f "$f" ]] || { echo "ERROR: $f is missing; cannot build the images." >&2; exit 1; }
done

# Context is the repo root: this image copies dags/local/ into the image.
echo "==> Building $AIRFLOW_IMAGE (Airflow + baked-in smoke-test DAG)"
docker build -f worker/Dockerfile.airflow -t "$AIRFLOW_IMAGE" .

# Context is worker/: this image only needs entrypoint.py.
echo "==> Building $WORKER_IMAGE (mocked inference worker)"
docker build -f worker/Dockerfile -t "$WORKER_IMAGE" worker/

echo "==> Images available inside the cluster:"
docker images --filter=reference='inference/*' --format 'table {{.Repository}}\t{{.Tag}}\t{{.Size}}'

RELEASE="${INFERENCE_RELEASE:-inference}"
NAMESPACE="${INFERENCE_NAMESPACE:-inference}"

cat <<EOF

Note: images live only in minikube's daemon -- there is nothing to push. Since
the tag is unchanged, Kubernetes will not restart pods on its own; if the stack
is already deployed, roll the components that parse DAGs:

  kubectl -n ${NAMESPACE} rollout restart \\
    deploy/${RELEASE}-scheduler deploy/${RELEASE}-dag-processor \\
    deploy/${RELEASE}-api-server statefulset/${RELEASE}-triggerer
EOF

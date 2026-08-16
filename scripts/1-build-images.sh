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

echo "==> Building inference/airflow:local (Airflow + baked-in DAGs)"
docker build -f worker/Dockerfile.airflow -t inference/airflow:local .

if [[ -f worker/Dockerfile ]]; then
  echo "==> Building inference/worker:local (mocked inference job)"
  docker build -f worker/Dockerfile -t inference/worker:local worker/
fi

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

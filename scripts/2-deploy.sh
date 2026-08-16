#!/usr/bin/env bash
# Deploy the whole stack as a single Helm release.
#
# The umbrella chart in k8s/inference-pipeline wraps the upstream Airflow and
# MinIO charts as dependencies and adds this project's own resources (RBAC,
# PersistentVolumes, Secrets, NetworkPolicy). Idempotent -- this is also the
# redeploy path.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RELEASE="${INFERENCE_RELEASE:-inference}"
NAMESPACE="${INFERENCE_NAMESPACE:-inference}"
CHART="./k8s/inference-pipeline"

# `helm dependency build` refuses to run unless every repository named in
# Chart.yaml is in the local helm config -- it fails with "no repository
# definition for ...". So the repos are registered here, next to the command
# that needs them, rather than in cluster bootstrap: they are a client-side
# concern and have nothing to do with minikube.
echo "==> Registering chart repositories"
helm repo add apache-airflow https://airflow.apache.org --force-update >/dev/null
helm repo add minio https://charts.min.io/ --force-update >/dev/null

# `build`, deliberately NOT `update`.
#
#   build  -- vendors exactly the versions recorded in Chart.lock
#   update -- re-resolves against the live repo index and REWRITES Chart.lock
#
# Using `update` here would rewrite the committed lockfile on every deploy,
# which would make committing it pointless. Bumping a dependency is a
# deliberate act: run `helm dependency update k8s/inference-pipeline` by hand
# and commit the resulting Chart.lock.
echo "==> Vendoring chart dependencies from Chart.lock"
helm dependency build "$CHART"

# --create-namespace makes the namespace before the release, so no separate
# manifest is needed. The NetworkPolicy selects the namespace on
# `kubernetes.io/metadata.name`, which the API server sets automatically on
# every namespace, so it matches regardless of how the namespace was created.
# The chart ships no default passwords -- see the TODO in values.yaml. If a
# local override file exists, pass it; otherwise let Helm's `required` produce
# the error, which already says exactly what to do.
LOCAL_VALUES="${CHART}/values.local.yaml"
EXTRA_ARGS=()
if [[ -f "$LOCAL_VALUES" ]]; then
  echo "==> Using local credential overrides from $LOCAL_VALUES"
  EXTRA_ARGS+=(--values "$LOCAL_VALUES")
fi

echo "==> Installing release '$RELEASE' into namespace '$NAMESPACE'"
helm upgrade --install "$RELEASE" "$CHART" \
  --namespace "$NAMESPACE" \
  --create-namespace \
  "${EXTRA_ARGS[@]}" \
  --timeout 10m \
  --wait

echo
kubectl get pods -n "$NAMESPACE"

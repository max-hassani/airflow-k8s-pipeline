#!/usr/bin/env bash
# Load the sample job configs, input CSVs and job spec lists into the cluster.
#
# Run after scripts/2-deploy.sh. Idempotent: re-running overwrites the same
# objects and Variables.
#
# Everything happens through kubectl -- no local `mc`, no `boto3`, no
# port-forward -- so the only tool this needs is the one you already used to
# deploy. Data is copied into a short-lived pod built from the MinIO client
# image, which then uploads it from inside the cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RELEASE="${INFERENCE_RELEASE:-inference}"
NAMESPACE="${INFERENCE_NAMESPACE:-inference}"
MC_IMAGE="${MC_IMAGE:-minio/mc:RELEASE.2024-11-21T17-21-54Z}"
POD="inference-seed"

# Bucket names are read from the cluster so this script cannot drift from
# values.yaml. They are not secret, so reading them onto this host is fine.
secret_val() { kubectl -n "$NAMESPACE" get secret inference-minio-credentials -o jsonpath="{.data.$1}" | base64 -d; }
BUCKET_CONFIGS="$(secret_val INFERENCE_BUCKET_CONFIGS)"
BUCKET_INPUTS="$(secret_val INFERENCE_BUCKET_INPUTS)"

cleanup() { kubectl -n "$NAMESPACE" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

# The MinIO root credentials are mounted into the pod straight from the Secret
# and NEVER read onto this machine. An earlier version pulled them out with
# `kubectl get secret | base64 -d` and passed them to `mc alias set` as
# arguments -- which puts the password in this host's process table (visible to
# `ps` for any local user) and into shell history. Injecting the Secret means
# the credential never leaves the cluster.
#
# Seeding needs root because it WRITES to the configs and inputs buckets, which
# the worker identity is deliberately scoped read-only on. The thing that
# publishes jobs is not the thing that runs them.
echo "==> Starting a temporary uploader pod"
kubectl -n "$NAMESPACE" delete pod "$POD" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl -n "$NAMESPACE" run "$POD" --restart=Never --image="$MC_IMAGE" --overrides="$(cat <<JSON
{
  "spec": {
    "containers": [{
      "name": "$POD",
      "image": "$MC_IMAGE",
      "command": ["sleep", "600"],
      "envFrom": [{"secretRef": {"name": "inference-minio-root"}}]
    }]
  }
}
JSON
)" >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD" --timeout=120s >/dev/null

# Single-quoted on purpose: $rootUser and $rootPassword are expanded by the
# shell INSIDE the container, from the injected Secret, not by this shell.
echo "==> Configuring the uploader"
kubectl -n "$NAMESPACE" exec "$POD" -- sh -c \
  'mc alias set seed "http://'"${RELEASE}"'-minio:9000" "$rootUser" "$rootPassword" >/dev/null'

# Streamed with `mc pipe` rather than staged with `kubectl cp`: kubectl cp
# shells out to `tar` *inside* the target container, and the MinIO client image
# has no tar (it fails with exit 127). Piping needs only stdin.
upload_tree() { # $1 = local dir, $2 = bucket
  local root="$1" bucket="$2" rel
  while IFS= read -r f; do
    rel="${f#"$root"/}"
    kubectl -n "$NAMESPACE" exec -i "$POD" -- mc pipe "seed/${bucket}/${rel}" >/dev/null < "$f"
    echo "    ${bucket}/${rel}"
  done < <(find "$root" -type f | sort)
}

echo "==> Uploading job configs -> ${BUCKET_CONFIGS}"
upload_tree data/configs "$BUCKET_CONFIGS"
echo "==> Uploading input CSVs -> ${BUCKET_INPUTS}"
upload_tree data/inputs "$BUCKET_INPUTS"

echo "==> Setting Airflow Variables from data/jobs/"
# The DAGs read these at parse time, so the job list can change without a code
# change or a bundle refresh.
for f in data/jobs/*.json; do
  key="$(basename "$f" .json)"
  # Passed as a single argv element, so the JSON needs no shell escaping.
  kubectl -n "$NAMESPACE" exec "deploy/${RELEASE}-scheduler" -c scheduler -- \
    airflow variables set "$key" "$(cat "$f")" >/dev/null 2>&1
  echo "    set Variable '$key'"
done

echo
echo "==> Seeded. Trigger a pipeline with:"
echo "      kubectl -n $NAMESPACE exec deploy/${RELEASE}-scheduler -c scheduler -- \\"
echo "        airflow dags trigger protein_embedder"

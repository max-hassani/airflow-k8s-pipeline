#!/usr/bin/env bash
# Expose the Airflow UI and the MinIO console on localhost.
#
# Every Service in this stack is ClusterIP: nothing is reachable from outside
# the cluster unless someone deliberately forwards it. Ctrl-C tears all three
# forwards down.
set -euo pipefail

RELEASE="${INFERENCE_RELEASE:-inference}"
NAMESPACE="${INFERENCE_NAMESPACE:-inference}"

AIRFLOW_PORT="${AIRFLOW_PORT:-8080}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"

cleanup() {
  echo
  echo "==> Stopping port-forwards"
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT INT TERM

kubectl -n "$NAMESPACE" port-forward "svc/${RELEASE}-api-server"     "${AIRFLOW_PORT}:8080"      >/dev/null &
kubectl -n "$NAMESPACE" port-forward "svc/${RELEASE}-minio-console"  "${MINIO_CONSOLE_PORT}:9001" >/dev/null &
kubectl -n "$NAMESPACE" port-forward "svc/${RELEASE}-minio"          "${MINIO_API_PORT}:9000"     >/dev/null &

# Read the demo credentials out of the cluster rather than repeating them here,
# so this script cannot drift from values.yaml.
MINIO_USER="$(kubectl -n "$NAMESPACE" get secret inference-minio-root -o jsonpath='{.data.rootUser}' | base64 -d)"
MINIO_PASS="$(kubectl -n "$NAMESPACE" get secret inference-minio-root -o jsonpath='{.data.rootPassword}' | base64 -d)"

cat <<EOF

  Airflow UI      http://localhost:${AIRFLOW_PORT}
                  admin / admin

  MinIO console   http://localhost:${MINIO_CONSOLE_PORT}
                  ${MINIO_USER} / ${MINIO_PASS}

  MinIO S3 API    http://localhost:${MINIO_API_PORT}
                  (for 'mc alias set' / boto3 from your laptop)

  Ctrl-C to stop.

EOF

wait

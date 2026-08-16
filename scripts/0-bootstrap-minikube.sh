#!/usr/bin/env bash
# Bring up a minikube cluster sized for this pipeline and prepare its host
# directories. Idempotent: safe to re-run against an already-running cluster.
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-minikube}"
CPUS="${MINIKUBE_CPUS:-4}"
MEMORY="${MINIKUBE_MEMORY:-8192}"
DISK="${MINIKUBE_DISK:-40g}"

if minikube status -p "$PROFILE" >/dev/null 2>&1; then
  echo "==> minikube profile '$PROFILE' is already running; leaving it alone."
  echo "    (delete it with 'minikube delete -p $PROFILE' to re-create at the sizes below)"
else
  echo "==> Starting minikube (cpus=$CPUS memory=${MEMORY}MB disk=$DISK)"
  # Airflow's five components + Postgres + MinIO + task pods. 4 CPU / 8 GB is
  # the smallest that comfortably runs the whole pipeline with several
  # concurrent KubernetesPodOperator jobs.
  minikube start \
    --profile "$PROFILE" \
    --cpus "$CPUS" \
    --memory "$MEMORY" \
    --disk-size "$DISK" \
    --driver docker
fi

# ---------------------------------------------------------------------------
# hostPath directories for the PersistentVolumes in k8s/20-storage.yaml.
#
# This step is not optional. kubelet creates hostPath directories as root:root
# and -- unlike CSI volumes -- does NOT apply the pod's fsGroup to them. MinIO
# (uid 1000) and Airflow (uid 50000) both run unprivileged, so without this
# they crash-loop on "Unable to write to the backend" / permission errors.
# ---------------------------------------------------------------------------
echo "==> Preparing hostPath directories with correct ownership"
minikube ssh -p "$PROFILE" -- "
  set -eu
  sudo mkdir -p /data/inference/minio /data/inference/airflow-logs
  sudo chown -R 1000:1000 /data/inference/minio
  sudo chown -R 50000:0   /data/inference/airflow-logs
  sudo chmod -R 2775      /data/inference/airflow-logs
  ls -ld /data/inference/minio /data/inference/airflow-logs
"

echo "==> Cluster ready."
kubectl get nodes

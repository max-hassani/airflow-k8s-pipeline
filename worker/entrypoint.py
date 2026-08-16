#!/usr/bin/env python3
"""Mocked batch-inference worker.

One entrypoint, three stages, selected with ``--stage``. Each stage runs as its
own Kubernetes pod, so nothing is shared between them except:

  * the scratch volume  -- an RWX PVC mounted at ``--work-root``, used to hand
    bulk intermediates from one stage to the next
  * MinIO               -- where the job config and input CSV come from, and
    where the final .npy is published

Stages
------
preprocess  read the job config and input CSV from MinIO, validate them, and
            stage the parsed records plus a manifest on the scratch volume
infer       read the manifest and emit a random (n_rows, 512) float32 array.
            No model is loaded: the challenge only requires an array of the
            correct shape, so this stands in for real inference.
postprocess read the array off scratch, check its shape, and publish it to the
            target bucket under a well-defined prefix

The stages are deliberately separate processes rather than one script with
three function calls: each gets its own pod, its own resource limits, and its
own retry behaviour, and a failure tells you exactly which phase broke.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path

import boto3
import numpy as np
import yaml
from botocore.config import Config
from botocore.exceptions import ClientError

# The mocked model's output width. Fixed by the worker contract: every job
# produces an (n_rows, EMBEDDING_DIM) array regardless of model.
EMBEDDING_DIM = 512


def log(stage: str, msg: str) -> None:
    """Single-line structured-ish logging.

    Goes to stdout so the KubernetesPodOperator streams it straight into the
    Airflow task log -- debugging should start in the UI, not with kubectl.
    """
    print(f"[{stage}] {msg}", flush=True)


def s3_client():
    """S3 client pointed at MinIO.

    Endpoint and credentials arrive as environment variables from the
    inference-minio-credentials Secret, so this container has no knowledge of
    where MinIO lives or who it authenticates as.

    `path` addressing is required: MinIO serves buckets as path prefixes, and
    boto3's default virtual-host addressing would resolve to hostnames like
    `inference-inputs.inference-minio` that do not exist.
    """
    endpoint = os.environ["MINIO_ENDPOINT"]
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def get_object(s3, bucket: str, key: str) -> bytes:
    """Fetch an object, turning the two failures we expect into clear messages.

    A 403 here almost always means the worker identity is not scoped for that
    bucket, which is a very different problem from the object being absent --
    and boto3's raw error makes them look similar.
    """
    try:
        return s3["client"].get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            raise SystemExit(f"object not found: s3://{bucket}/{key}")
        if code in ("AccessDenied", "403"):
            raise SystemExit(
                f"access denied reading s3://{bucket}/{key} as "
                f"'{os.environ.get('AWS_ACCESS_KEY_ID', '?')}'. "
                "This identity is scoped by MinIO IAM policy; check that the "
                "bucket is one it is allowed to read."
            )
        raise


def work_dir(args) -> Path:
    """Per-job scratch directory on the shared volume.

    Scoped by run_id as well as job_id so concurrent runs of the same DAG --
    and retries of the same task -- cannot read each other's intermediates.
    """
    d = Path(args.work_root) / args.dag_id / args.run_id / args.job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------


def stage_preprocess(args, s3, spec: dict) -> None:
    cfg_bucket = os.environ["INFERENCE_BUCKET_CONFIGS"]
    in_bucket = os.environ["INFERENCE_BUCKET_INPUTS"]

    log("preprocess", f"reading job config s3://{cfg_bucket}/{spec['config_key']}")
    config = yaml.safe_load(get_object(s3, cfg_bucket, spec["config_key"]))

    # The config carries only the model-side settings. Routing -- which CSV,
    # which output bucket -- comes from the job spec, so neither file repeats
    # a field the other owns. Validated here rather than failing three stages
    # later with a KeyError that points at the wrong place.
    missing = [k for k in ("batch_size", "model_name") if k not in config]
    if missing:
        raise SystemExit(
            f"job config s3://{cfg_bucket}/{spec['config_key']} is missing: {', '.join(missing)}"
        )

    log(
        "preprocess",
        f"job_id={spec['job_id']} model={config['model_name']} batch_size={config['batch_size']}",
    )

    log("preprocess", f"reading input s3://{in_bucket}/{spec['input_csv']}")
    raw = get_object(s3, in_bucket, spec["input_csv"]).decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(raw)))
    if not rows:
        raise SystemExit(f"input CSV s3://{in_bucket}/{spec['input_csv']} has no data rows")

    d = work_dir(args)
    (d / "records.jsonl").write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")

    # The manifest is what the next stage reads. Carrying n_rows forward means
    # `infer` never has to re-read or re-parse the CSV.
    manifest = {
        "job_id": spec["job_id"],
        "model_name": config["model_name"],
        "batch_size": int(config["batch_size"]),
        "n_rows": len(rows),
        "columns": list(rows[0].keys()),
        "source": f"s3://{in_bucket}/{spec['input_csv']}",
        "target_bucket": spec["target_bucket"],
    }
    (d / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    log("preprocess", f"staged {len(rows)} rows -> {d}")
    emit_xcom({"n_rows": len(rows), "work_dir": str(d)})


def stage_infer(args, s3, spec: dict) -> None:
    d = work_dir(args)
    manifest = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    n_rows, batch_size = manifest["n_rows"], manifest["batch_size"]

    log("infer", f"model={manifest['model_name']} rows={n_rows} batch_size={batch_size}")

    # Generated in batches purely so batch_size from the config is honoured and
    # visible in the logs -- it is what a real batched forward pass would do,
    # and it makes the mock behave like the thing it stands in for.
    rng = np.random.default_rng()
    batches = []
    for start in range(0, n_rows, batch_size):
        n = min(batch_size, n_rows - start)
        batches.append(rng.standard_normal((n, EMBEDDING_DIM), dtype=np.float32))
        log("infer", f"batch {start // batch_size + 1}: rows {start}..{start + n - 1}")

    embeddings = np.vstack(batches)
    if embeddings.shape != (n_rows, EMBEDDING_DIM):
        raise SystemExit(f"internal error: produced {embeddings.shape}, expected {(n_rows, EMBEDDING_DIM)}")

    out = d / "embeddings.npy"
    np.save(out, embeddings)
    log("infer", f"wrote {embeddings.shape} float32 -> {out} ({out.stat().st_size} bytes)")
    emit_xcom({"shape": list(embeddings.shape), "dtype": str(embeddings.dtype)})


def stage_postprocess(args, s3, spec: dict) -> None:
    d = work_dir(args)
    manifest = json.loads((d / "manifest.json").read_text(encoding="utf-8"))

    embeddings = np.load(d / "embeddings.npy")
    expected = (manifest["n_rows"], EMBEDDING_DIM)
    # Re-checked here because this is the last point before the artifact
    # becomes visible to downstream consumers.
    if embeddings.shape != expected:
        raise SystemExit(f"refusing to publish: shape {embeddings.shape}, expected {expected}")

    target = manifest["target_bucket"]
    key = f"{manifest['model_name']}/{manifest['job_id']}.npy"

    # Re-serialise from the loaded array rather than uploading the file, so
    # what is published is exactly what was just validated.
    buf = io.BytesIO()
    np.save(buf, embeddings)
    buf.seek(0)

    log("postprocess", f"publishing {embeddings.shape} -> s3://{target}/{key}")
    s3["client"].put_object(
        Bucket=target,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
        Metadata={
            "job-id": str(manifest["job_id"]),
            "model-name": str(manifest["model_name"]),
            "n-rows": str(manifest["n_rows"]),
            "embedding-dim": str(EMBEDDING_DIM),
        },
    )
    log("postprocess", f"published s3://{target}/{key}")
    emit_xcom({"output_key": key, "output_bucket": target, "shape": list(embeddings.shape)})


def emit_xcom(payload: dict) -> None:
    """Hand a small result back to Airflow.

    KubernetesPodOperator reads /airflow/xcom/return.json when do_xcom_push is
    set. Only metadata travels this way -- bulk data goes on the scratch volume,
    because XCom values live in the Airflow metadata database.
    """
    xcom_dir = Path("/airflow/xcom")
    if not xcom_dir.is_dir():
        return
    (xcom_dir / "return.json").write_text(json.dumps(payload), encoding="utf-8")


STAGES = {
    "preprocess": stage_preprocess,
    "infer": stage_infer,
    "postprocess": stage_postprocess,
}


def main() -> int:
    p = argparse.ArgumentParser(description="Mocked batch-inference worker")
    p.add_argument("--stage", required=True, choices=sorted(STAGES))
    # The job spec arrives as JSON so the DAG can pass a whole mapped spec as a
    # single argument, instead of the operator growing a flag per field.
    p.add_argument("--spec-json", required=True, help="JSON job spec")
    p.add_argument("--work-root", default="/scratch", help="Shared scratch volume mount point")
    p.add_argument("--dag-id", dest="dag_id", required=True)
    p.add_argument("--run-id", dest="run_id", required=True)
    args = p.parse_args()

    try:
        spec = json.loads(args.spec_json)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--spec-json is not valid JSON: {exc}")

    for key in ("job_id", "config_key", "input_csv", "target_bucket"):
        if key not in spec:
            raise SystemExit(f"job spec is missing '{key}': {spec}")

    # job_id names the scratch directory, so every stage of a job resolves to
    # the same place. The DAG derives it from the input filename, which keeps a
    # published artifact traceable back to the object that produced it.
    args.job_id = spec["job_id"]

    # run_id contains characters (colons, plus signs) that are legal in a path
    # but awkward everywhere else; normalise once, here.
    args.run_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in args.run_id)

    STAGES[args.stage](args, {"client": s3_client()}, spec)
    return 0


if __name__ == "__main__":
    sys.exit(main())

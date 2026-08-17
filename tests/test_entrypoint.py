"""Worker stage logic.

The three stages run as separate pods and hand data to each other through a
shared volume, so the properties worth pinning down are the ones that only break
across that boundary: the manifest one stage writes is the manifest the next one
reads, and the array shape survives to publication.
"""

from __future__ import annotations

import io
import json

import numpy as np
import pytest
from conftest import FakeS3, entrypoint

# ---------------------------------------------------------------------------
# get_object: turning two indistinguishable-looking S3 failures into clear ones
# ---------------------------------------------------------------------------


def test_missing_object_names_the_key(s3, buckets):
    with pytest.raises(SystemExit) as exc:
        entrypoint.get_object(s3, "test-inputs", "nope.csv")
    assert "s3://test-inputs/nope.csv" in str(exc.value)


def test_access_denied_blames_iam_not_the_object(s3, buckets):
    """A 403 and a 404 look alike in boto3's raw error but mean different things.

    Getting this wrong sends someone hunting for a missing file when the real
    problem is that the worker identity is not scoped for the bucket.
    """
    s3["client"].denied.add("test-restricted")
    with pytest.raises(SystemExit) as exc:
        entrypoint.get_object(s3, "test-restricted", "secret.csv")
    message = str(exc.value)
    assert "access denied" in message.lower()
    assert "test-worker" in message, "should name the identity that was refused"
    assert "IAM" in message


def test_unexpected_error_is_not_swallowed(s3, buckets):
    """Only the two anticipated codes are translated; anything else propagates."""
    s3["client"].objects.clear()
    original = s3["client"].get_object

    def boom(**kwargs):
        raise FakeS3._error("InternalError", "GetObject")

    s3["client"].get_object = boom
    with pytest.raises(Exception) as exc:
        entrypoint.get_object(s3, "test-inputs", "x.csv")
    assert not isinstance(exc.value, SystemExit)
    s3["client"].get_object = original


# ---------------------------------------------------------------------------
# preprocess
# ---------------------------------------------------------------------------


def test_preprocess_writes_manifest_the_next_stage_can_read(args, seeded, spec, buckets):
    entrypoint.stage_preprocess(args, seeded, spec)

    d = entrypoint.work_dir(args)
    manifest = json.loads((d / "manifest.json").read_text())

    assert manifest["n_rows"] == 5
    assert manifest["batch_size"] == 2
    assert manifest["model_name"] == "protein-embedder-v1"
    # job_id and target_bucket come from the SPEC, model settings from the
    # CONFIG -- the split that keeps either file from redefining the other.
    assert manifest["job_id"] == "batch_a"
    assert manifest["target_bucket"] == "test-outputs"
    assert len((d / "records.jsonl").read_text().splitlines()) == 5


def test_preprocess_rejects_config_missing_model_settings(args, seeded, spec, buckets):
    seeded["client"].objects[(buckets["INFERENCE_BUCKET_CONFIGS"], spec["config_key"])] = (
        b"model_name: only-this\n"
    )
    with pytest.raises(SystemExit) as exc:
        entrypoint.stage_preprocess(args, seeded, spec)
    assert "batch_size" in str(exc.value)


def test_preprocess_rejects_header_only_csv(args, seeded, spec, buckets):
    """A CSV with a header and no rows would otherwise produce a (0, 512) array."""
    seeded["client"].objects[(buckets["INFERENCE_BUCKET_INPUTS"], spec["input_csv"])] = (
        b"record_id,sequence\n"
    )
    with pytest.raises(SystemExit) as exc:
        entrypoint.stage_preprocess(args, seeded, spec)
    assert "no data rows" in str(exc.value)


# ---------------------------------------------------------------------------
# infer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("n_rows", "batch_size"),
    [
        (5, 2),  # final batch is short -- the off-by-one that vstack would hide
        (4, 4),  # exactly one batch
        (3, 10),  # batch larger than the input
        (1, 1),  # single row
    ],
)
def test_infer_shape_is_independent_of_batching(args, s3, n_rows, batch_size):
    """Whatever the batching, the array must be (n_rows, 512) float32.

    The contract is the shape; batch_size is an implementation detail of how it
    gets filled, and must not leak into the result.
    """
    d = entrypoint.work_dir(args)
    (d / "manifest.json").write_text(
        json.dumps(
            {
                "n_rows": n_rows,
                "batch_size": batch_size,
                "model_name": "m",
                "job_id": "j",
                "target_bucket": "test-outputs",
            }
        )
    )

    entrypoint.stage_infer(args, s3, {})

    arr = np.load(d / "embeddings.npy")
    assert arr.shape == (n_rows, entrypoint.EMBEDDING_DIM)
    assert arr.dtype == np.float32


def test_infer_output_is_not_constant(args, s3):
    """Guards against a refactor that returns zeros or a broadcast row."""
    d = entrypoint.work_dir(args)
    (d / "manifest.json").write_text(
        json.dumps(
            {
                "n_rows": 4,
                "batch_size": 2,
                "model_name": "m",
                "job_id": "j",
                "target_bucket": "test-outputs",
            }
        )
    )
    entrypoint.stage_infer(args, s3, {})
    arr = np.load(d / "embeddings.npy")
    assert arr.std() > 0
    assert not np.allclose(arr[0], arr[1])


# ---------------------------------------------------------------------------
# postprocess
# ---------------------------------------------------------------------------


def _stage_ready_to_publish(args, n_rows=5):
    d = entrypoint.work_dir(args)
    (d / "manifest.json").write_text(
        json.dumps(
            {
                "n_rows": n_rows,
                "batch_size": 2,
                "model_name": "protein-embedder-v1",
                "job_id": "batch_a",
                "target_bucket": "test-outputs",
            }
        )
    )
    return d


def test_postprocess_publishes_under_model_and_job_id(args, s3, buckets):
    d = _stage_ready_to_publish(args)
    np.save(d / "embeddings.npy", np.zeros((5, entrypoint.EMBEDDING_DIM), dtype=np.float32))

    entrypoint.stage_postprocess(args, s3, {})

    (put,) = s3["client"].puts
    assert put["Bucket"] == "test-outputs"
    assert put["Key"] == "protein-embedder-v1/batch_a.npy"
    assert put["Metadata"]["n-rows"] == "5"
    assert put["Metadata"]["embedding-dim"] == str(entrypoint.EMBEDDING_DIM)

    # What was published must be loadable as a real .npy, not just bytes.
    arr = np.load(io.BytesIO(put["Body"]))
    assert arr.shape == (5, entrypoint.EMBEDDING_DIM)


def test_postprocess_refuses_to_publish_a_wrong_shape(args, s3, buckets):
    """The last gate before an artifact becomes visible downstream."""
    d = _stage_ready_to_publish(args, n_rows=5)
    np.save(d / "embeddings.npy", np.zeros((4, entrypoint.EMBEDDING_DIM), dtype=np.float32))

    with pytest.raises(SystemExit) as exc:
        entrypoint.stage_postprocess(args, s3, {})
    assert "refusing to publish" in str(exc.value)
    assert s3["client"].puts == [], "nothing may be written when validation fails"


# ---------------------------------------------------------------------------
# argument handling
# ---------------------------------------------------------------------------


def _run_main(monkeypatch, argv, recorder=None):
    monkeypatch.setattr(entrypoint, "s3_client", lambda: FakeS3())
    if recorder is not None:
        monkeypatch.setattr(entrypoint, "STAGES", dict.fromkeys(entrypoint.STAGES, recorder))
    monkeypatch.setattr("sys.argv", ["entrypoint.py", *argv])
    return entrypoint.main()


def test_run_id_is_normalised_for_use_as_a_path(monkeypatch, tmp_path):
    """Airflow run_ids contain ':' and '+', which are legal in a path but awkward.

    Every stage must derive the same directory from the same run, so this has to
    be deterministic rather than merely filesystem-safe.
    """
    seen = {}

    def recorder(args, s3, spec):
        seen["run_id"] = args.run_id
        seen["job_id"] = args.job_id

    _run_main(
        monkeypatch,
        [
            "--stage",
            "infer",
            "--spec-json",
            json.dumps(
                {
                    "job_id": "batch_a",
                    "config_key": "c.yaml",
                    "input_csv": "i.csv",
                    "target_bucket": "b",
                }
            ),
            "--work-root",
            str(tmp_path),
            "--dag-id",
            "d",
            "--run-id",
            "manual__2026-08-16T11:02:45.035001+00:00",
        ],
        recorder=recorder,
    )
    assert ":" not in seen["run_id"]
    assert "+" not in seen["run_id"]
    assert "." not in seen["run_id"]
    assert seen["run_id"].startswith("manual__2026-08-16T11_02_45")
    assert seen["job_id"] == "batch_a"


@pytest.mark.parametrize("missing", ["job_id", "config_key", "input_csv", "target_bucket"])
def test_main_rejects_incomplete_spec(monkeypatch, tmp_path, missing):
    full = {
        "job_id": "j",
        "config_key": "c.yaml",
        "input_csv": "i.csv",
        "target_bucket": "b",
    }
    full.pop(missing)
    with pytest.raises(SystemExit) as exc:
        _run_main(
            monkeypatch,
            [
                "--stage",
                "preprocess",
                "--spec-json",
                json.dumps(full),
                "--work-root",
                str(tmp_path),
                "--dag-id",
                "d",
                "--run-id",
                "r",
            ],
            recorder=lambda *a: None,
        )
    assert missing in str(exc.value)


def test_main_rejects_malformed_spec_json(monkeypatch, tmp_path):
    with pytest.raises(SystemExit) as exc:
        _run_main(
            monkeypatch,
            [
                "--stage",
                "preprocess",
                "--spec-json",
                "{not json",
                "--work-root",
                str(tmp_path),
                "--dag-id",
                "d",
                "--run-id",
                "r",
            ],
            recorder=lambda *a: None,
        )
    assert "not valid JSON" in str(exc.value)


# ---------------------------------------------------------------------------
# the three stages, end to end over a shared directory
# ---------------------------------------------------------------------------


def test_stages_compose_across_the_shared_volume(args, seeded, spec, buckets):
    """The real integration risk: each stage runs in its own pod, so the only
    thing connecting them is what the previous one left on the volume."""
    entrypoint.stage_preprocess(args, seeded, spec)
    entrypoint.stage_infer(args, seeded, spec)
    entrypoint.stage_postprocess(args, seeded, spec)

    (put,) = seeded["client"].puts
    arr = np.load(io.BytesIO(put["Body"]))
    assert arr.shape == (5, entrypoint.EMBEDDING_DIM)
    assert put["Key"] == "protein-embedder-v1/batch_a.npy"

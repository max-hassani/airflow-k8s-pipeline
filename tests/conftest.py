"""Shared fixtures.

The worker talks to S3 through exactly two calls -- `get_object` and
`put_object` -- so this uses a hand-written fake rather than pulling in moto.
That keeps the test suite dependency-light and, more importantly, keeps the
tests about *our* logic rather than about boto3's behaviour. The fake still
raises real `botocore` `ClientError`s, because the error-code branching in
`get_object` is one of the things worth testing.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_entrypoint():
    """Import worker/entrypoint.py by path.

    `worker/` is not a package and is deliberately not importable as one -- it
    is a container build context, not a library -- so the module is loaded from
    its file path rather than added to sys.path.
    """
    path = REPO_ROOT / "worker" / "entrypoint.py"
    spec = importlib.util.spec_from_file_location("worker_entrypoint", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["worker_entrypoint"] = module
    spec.loader.exec_module(module)
    return module


entrypoint = _load_entrypoint()


class FakeS3:
    """Minimal in-memory stand-in for the S3 client.

    `objects` maps (bucket, key) -> bytes. `denied` holds buckets that raise
    AccessDenied, which is how MinIO responds when an identity's IAM policy does
    not cover a bucket -- the case the worker is expected to explain clearly.
    """

    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None, denied=()):
        self.objects: dict[tuple[str, str], bytes] = dict(objects or {})
        self.denied = set(denied)
        self.puts: list[dict] = []

    @staticmethod
    def _error(code: str, op: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": code}}, op)

    def get_object(self, Bucket: str, Key: str):
        if Bucket in self.denied:
            raise self._error("AccessDenied", "GetObject")
        if (Bucket, Key) not in self.objects:
            raise self._error("NoSuchKey", "GetObject")
        payload = self.objects[(Bucket, Key)]
        return {"Body": SimpleNamespace(read=lambda: payload)}

    def put_object(self, **kwargs):
        if kwargs["Bucket"] in self.denied:
            raise self._error("AccessDenied", "PutObject")
        self.puts.append(kwargs)
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {}


@pytest.fixture
def buckets(monkeypatch):
    """Bucket names reach the worker as environment, exactly as in the cluster."""
    names = {
        "INFERENCE_BUCKET_CONFIGS": "test-configs",
        "INFERENCE_BUCKET_INPUTS": "test-inputs",
        "INFERENCE_BUCKET_OUTPUTS": "test-outputs",
        "INFERENCE_BUCKET_RESTRICTED": "test-restricted",
        "AWS_ACCESS_KEY_ID": "test-worker",
    }
    for k, v in names.items():
        monkeypatch.setenv(k, v)
    return names


@pytest.fixture
def s3():
    return {"client": FakeS3()}


@pytest.fixture
def args(tmp_path):
    """The attribute bag the stage functions expect from argparse."""
    return SimpleNamespace(
        work_root=str(tmp_path),
        dag_id="test_dag",
        run_id="manual__2026-01-01T00_00_00",
        job_id="batch_a",
    )


@pytest.fixture
def spec():
    return {
        "job_id": "batch_a",
        "config_key": "protein.yaml",
        "input_csv": "proteins/batch_a.csv",
        "target_bucket": "test-outputs",
    }


@pytest.fixture
def seeded(s3, buckets, spec):
    """A config and a 5-row CSV in place, which is the normal starting state."""
    s3["client"].objects[(buckets["INFERENCE_BUCKET_CONFIGS"], spec["config_key"])] = (
        b"model_name: protein-embedder-v1\nbatch_size: 2\n"
    )
    rows = "\n".join(f"r-{i},SEQ{i}" for i in range(5))
    s3["client"].objects[(buckets["INFERENCE_BUCKET_INPUTS"], spec["input_csv"])] = (
        f"record_id,sequence\n{rows}\n".encode()
    )
    return s3

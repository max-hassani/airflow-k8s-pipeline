"""The sample data is wiring, not decoration.

A job spec points at a config file by key and at inputs by prefix; the seed
script turns each `data/jobs/<name>.json` into an Airflow Variable named
`<name>`; and each DAG asks for a Variable by name. None of those links is
type-checked anywhere, and a rename breaks them silently -- the DAG parses
fine, `Variable.get` returns its `[]` default, and the pipeline produces zero
tasks with no error. These tests are the check that would otherwise not exist.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import pytest
import yaml
from conftest import REPO_ROOT

JOBS_DIR = REPO_ROOT / "data" / "jobs"
CONFIGS_DIR = REPO_ROOT / "data" / "configs"
INPUTS_DIR = REPO_ROOT / "data" / "inputs"
PIPELINES_DIR = REPO_ROOT / "dags" / "pipelines"

JOB_FILES = sorted(JOBS_DIR.glob("*.json"))
CONFIG_FILES = sorted(CONFIGS_DIR.glob("*.yaml"))
CSV_FILES = sorted(INPUTS_DIR.rglob("*.csv"))

# Fields the DAG's discover_jobs task requires of every spec.
SPEC_REQUIRED = {"config_key", "input_prefix", "target_bucket"}
# Fields worker/entrypoint.py requires of every config.
CONFIG_REQUIRED = {"model_name", "batch_size"}


def test_fixtures_exist():
    assert JOB_FILES, "no job spec files found"
    assert CONFIG_FILES, "no config files found"
    assert CSV_FILES, "no input CSVs found"


@pytest.mark.parametrize("path", JOB_FILES, ids=lambda p: p.name)
def test_job_spec_shape(path: Path):
    specs = json.loads(path.read_text())
    assert isinstance(specs, list) and specs, f"{path.name} must be a non-empty list"
    for spec in specs:
        missing = SPEC_REQUIRED - spec.keys()
        assert not missing, f"{path.name}: spec missing {sorted(missing)}"
        assert spec["input_prefix"].endswith("/"), (
            f"{path.name}: input_prefix {spec['input_prefix']!r} must end with '/' -- "
            "without it, 'proteins' would also match a 'proteins-archive/' prefix"
        )


@pytest.mark.parametrize("path", CONFIG_FILES, ids=lambda p: p.name)
def test_config_shape(path: Path):
    config = yaml.safe_load(path.read_text())
    missing = CONFIG_REQUIRED - config.keys()
    assert not missing, f"{path.name}: missing {sorted(missing)}"
    assert isinstance(config["batch_size"], int) and config["batch_size"] > 0
    # Routing belongs to the spec. A config that also names an input or a bucket
    # means two files can disagree about the same job.
    assert not ({"input_csv", "input_prefix", "target_bucket"} & config.keys()), (
        f"{path.name}: routing fields belong in the job spec, not the config"
    )


@pytest.mark.parametrize("path", CSV_FILES, ids=lambda p: p.name)
def test_input_csv_has_a_header_and_rows(path: Path):
    rows = list(csv.DictReader(path.read_text().splitlines()))
    assert rows, f"{path.name} has a header but no data rows"
    assert "record_id" in rows[0], f"{path.name} must carry a record_id column"


@pytest.mark.parametrize("path", JOB_FILES, ids=lambda p: p.name)
def test_every_spec_resolves_to_a_real_config_and_real_inputs(path: Path):
    """The cross-file links the seed script assumes but never verifies."""
    for spec in json.loads(path.read_text()):
        config = CONFIGS_DIR / spec["config_key"]
        assert config.is_file(), (
            f"{path.name}: config_key {spec['config_key']!r} does not exist in data/configs/"
        )
        prefix_dir = INPUTS_DIR / spec["input_prefix"].rstrip("/")
        found = sorted(prefix_dir.glob("*.csv")) if prefix_dir.is_dir() else []
        assert found, (
            f"{path.name}: input_prefix {spec['input_prefix']!r} matches no CSV under "
            "data/inputs/ -- discover_jobs would fail the run"
        )


def test_variable_names_match_what_the_dags_ask_for():
    """`data/jobs/<name>.json` becomes Variable `<name>` (scripts/3-seed-data.sh
    uses the basename). A DAG asks for a Variable by name. Rename either side
    and the pipeline silently produces zero tasks."""
    seeded = {p.stem for p in JOB_FILES}

    requested = set()
    for dag_file in PIPELINES_DIR.glob("*.py"):
        requested |= set(
            re.findall(r'jobs_variable\s*=\s*["\']([^"\']+)["\']', dag_file.read_text())
        )

    assert requested, "no DAG requested a jobs_variable -- has the wiring changed?"
    unseeded = requested - seeded
    assert not unseeded, (
        f"DAGs request Variables that no data/jobs file seeds: {sorted(unseeded)}. "
        f"Seeded: {sorted(seeded)}"
    )

"""Molecule embedding pipeline.

Same shape as the protein pipeline but a different model family, a different
Airflow Variable, and different batch sizes -- the point being that adding a
pipeline costs a file like this one, not a copy of the orchestration logic.

The word "Airflow" above is load-bearing if `core.dag_discovery_safe_mode` is
ever turned back on: that setting only parses files containing both "airflow"
and "dag", so a wrapper this thin would otherwise be skipped silently. The
chart disables safe mode, but this keeps the file correct either way.
"""

from __future__ import annotations

from _inference_pipeline import build_pipeline

dag = build_pipeline(
    dag_id="molecule_embedder",
    jobs_variable="molecule_embedder_jobs",
    model_family="molecule",
    description="Batch molecule embedding: 1 job, 3 pods",
    schedule=None,
)

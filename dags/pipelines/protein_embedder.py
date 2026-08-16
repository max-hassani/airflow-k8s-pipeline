"""Protein embedding pipeline.

Runs every job listed in the `protein_embedder_jobs` Airflow Variable through
preprocess -> infer -> postprocess, one KubernetesPodOperator pod per stage,
publishing an (n_rows, 512) .npy per job to the outputs bucket.

Served from git via the `inference-pipelines` DAG bundle, so every run records
the commit that defined it in `dag_run.bundle_version`.
"""

from __future__ import annotations

from _inference_pipeline import build_pipeline

dag = build_pipeline(
    dag_id="protein_embedder",
    jobs_variable="protein_embedder_jobs",
    model_family="protein",
    description="Batch protein embedding: 2 jobs, 3 pods each",
    # Triggered on demand. In the event-driven follow-up this becomes
    # `schedule=[configs_asset]` so a new config landing in MinIO starts a run.
    schedule=None,
)

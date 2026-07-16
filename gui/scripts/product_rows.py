"""Render-time row builders: registry products + session-job overlay.

Solara-free (architecture contract #7): pure transforms over the Project
document and the module-level job lists. Each builder returns plain dicts the
tab widgets turn into ProductTable rows (callbacks are attached in the tiles).

Suppression contract: workers never remove their own job rows (the job lists
are unsynchronized read-copy-set reactives). Instead a *completed* job row is
dropped here, at render time, ONLY when its registered product is present in
the project snapshot being rendered. A completed job whose registration didn't
land keeps rendering, so a run can never silently vanish.

These builders replace gui/scripts/job_restore.py (deleted): products now
derive from the registries at render time instead of being mirrored into fake
"completed" job dicts at load time.
"""

from typing import Any, Dict, List, Optional


def _active_jobs_first(jobs: Optional[List[dict]]) -> List[dict]:
    """Session jobs, newest first (they sit above the product rows)."""
    return list(reversed(jobs or []))


# --- Train ------------------------------------------------------------------

def train_rows(
    project: Any, jobs: Optional[List[dict]], model_labels: Optional[Dict[str, str]] = None
) -> List[dict]:
    """Job rows (newest first) then one row per registered model."""
    labels = model_labels or {}
    models = (getattr(project, "models", None) or {}) if project is not None else {}

    rows: List[dict] = []
    for job in _active_jobs_first(jobs):
        if job.get("status") == "completed":
            key = job.get("model_storage_key")
            if key and key in models:
                continue  # superseded by its product row below
        rows.append(
            {
                "kind": "job",
                "key": f"job_{job['id']}",
                "job_id": job["id"],
                "name": job.get("model_name") or job.get("model_label", "—"),
                "model_label": job.get("model_label", job.get("model_type", "—")),
                "dataset_name": job.get("dataset_name", "—"),
                "deviance": job.get("deviance"),
                "n_samples": job.get("n_samples"),
                "status": job.get("status", "running"),
                "error": job.get("error"),
            }
        )
    for key, model in models.items():
        model_type = getattr(model, "model_type", "") or ""
        rows.append(
            {
                "kind": "model",
                "key": key,
                "name": key,
                "model_label": labels.get(model_type, model_type),
                "dataset_name": getattr(model, "dataset_name", None) or "—",
                "deviance": getattr(model, "deviance", None),
                "n_samples": getattr(model, "n_samples", None),
                "status": "trained",
                "error": None,
            }
        )
    return rows


# --- Inference ---------------------------------------------------------------

def prediction_row_key(pred: Any) -> str:
    """Row identity: the user-chosen name, else the legacy provenance token."""
    name = getattr(pred, "name", None)
    if name:
        return name
    model_key = getattr(pred, "model_key", None) or "—"
    dataset_name = getattr(pred, "dataset_name", None) or "—"
    return f"{model_key}__{dataset_name}"


def job_row_key(job: dict) -> str:
    """Row key a session inference job maps to (mirrors prediction_row_key)."""
    if job.get("pred_name"):
        return job["pred_name"]
    return f"{job.get('model_key', '—')}__{job.get('dataset_name', '—')}"


def prediction_groups(project: Any) -> Dict[str, dict]:
    """Registered predictions grouped by row key (multi-raster runs share one)."""
    predictions = (getattr(project, "predictions", None) or {}) if project is not None else {}
    groups: Dict[str, dict] = {}
    for storage_key, pred in predictions.items():
        row_key = prediction_row_key(pred)
        group = groups.setdefault(
            row_key,
            {
                "row_key": row_key,
                "name": getattr(pred, "name", None),
                "model_key": getattr(pred, "model_key", None) or "—",
                "dataset_name": getattr(pred, "dataset_name", None) or "—",
                "storage_keys": [],
            },
        )
        group["storage_keys"].append(storage_key)
    return groups


def inference_rows(project: Any, jobs: Optional[List[dict]]) -> List[dict]:
    """Job rows (newest first) then one row per registered prediction group."""
    groups = prediction_groups(project)

    rows: List[dict] = []
    for job in _active_jobs_first(jobs):
        if job.get("status") == "completed" and job_row_key(job) in groups:
            continue  # superseded by its product row below
        rows.append(
            {
                "kind": "job",
                "key": f"job_{job['id']}",
                "job_id": job["id"],
                "name": job.get("pred_name") or job.get("model_key", "—"),
                "model_key": job.get("model_key", "—"),
                "dataset_name": job.get("dataset_name", "—"),
                "storage_keys": [],
                "status": job.get("status", "running"),
                "error": job.get("error"),
            }
        )
    for row_key, group in groups.items():
        rows.append(
            {
                "kind": "prediction",
                "key": row_key,
                "name": group["name"] or row_key,
                "model_key": group["model_key"],
                "dataset_name": group["dataset_name"],
                "storage_keys": list(group["storage_keys"]),
                "status": "ready",
                "error": None,
            }
        )
    return rows


# --- Sampling -----------------------------------------------------------------

def sample_rows(project: Any, jobs: Optional[List[dict]]) -> List[dict]:
    """Job rows (newest first) then one row per registered sample set."""
    samples = (getattr(project, "samples", None) or {}) if project is not None else {}

    rows: List[dict] = []
    for job in _active_jobs_first(jobs):
        if job.get("status") == "completed" and job.get("name") in samples:
            continue  # superseded by its product row below
        rows.append(
            {
                "kind": "job",
                "key": f"job_{job['id']}",
                "job_id": job["id"],
                "name": job.get("name", "—"),
                "strategy": job.get("strategy", "—"),
                "allocation": None,
                "n_total": job.get("n_total"),
                "class_counts": job.get("class_counts"),
                "status": job.get("status", "running"),
                "error": job.get("error"),
            }
        )
    for key, s in samples.items():
        rows.append(
            {
                "kind": "sample",
                "key": key,
                "name": key,
                "strategy": getattr(s, "strategy", "—"),
                "allocation": getattr(s, "allocation", None),
                "n_total": getattr(s, "n_total", None),
                "class_counts": getattr(s, "class_counts", None) or {},
                "status": "ready",
                "error": None,
            }
        )
    return rows


# --- Evaluation ---------------------------------------------------------------

def evaluation_tab_rows(project: Any, jobs: Optional[List[dict]]) -> List[dict]:
    """Job rows (newest first) then saved records, newest first."""
    evaluations = (getattr(project, "evaluations", None) or {}) if project is not None else {}

    rows: List[dict] = []
    for job in _active_jobs_first(jobs):
        if job.get("status") == "completed" and any(
            getattr(rec, "run_id", None) == job["id"] for rec in evaluations.values()
        ):
            continue  # superseded by its saved record below
        rows.append(
            {
                "kind": "job",
                "key": f"job_{job['id']}",
                "job_id": job["id"],
                "truth_tag": job.get("truth_tag", "—"),
                "n_maps": job.get("n_maps", 0),
                "created_at": job.get("created_at", "—"),
                "status": job.get("status", "running"),
                "error": job.get("error"),
            }
        )
    records = sorted(
        evaluations.items(),
        key=lambda kv: getattr(kv[1], "created_at", "") or "",
        reverse=True,
    )
    for key, rec in records:
        rows.append(
            {
                "kind": "evaluation",
                "key": key,
                "truth_tag": getattr(rec, "truth_tag", key),
                "n_maps": len(getattr(rec, "prediction_keys", None) or []),
                "created_at": getattr(rec, "created_at", "") or "—",
                "status": "ready",
                "error": None,
            }
        )
    return rows

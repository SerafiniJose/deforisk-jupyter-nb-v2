"""Rebuild the Train/Inference job lists from a loaded project.

The Train and Inference tiles render their lists from module-level "job"
reactives (``train_jobs``, ``inference_jobs``) that are only appended to when a
job runs in the current session. A project loaded from disk repopulates
``project.models`` / ``project.predictions`` but those reactives know nothing
about it, so restored work never shows in the GUI. These helpers reconstruct
equivalent "completed" job dicts so loaded models/predictions render exactly as
freshly-run ones do.

Solara-free (architecture contract #7): pure data transforms over the Project
document, called from an on-load effect in the shell.
"""

from typing import Any, Dict, List, Optional


def build_train_jobs(
    project: Any, model_labels: Optional[Dict[str, str]] = None
) -> List[dict]:
    """One 'completed' training-job dict per registered model.

    Mirrors the job dict that ``train_tile._run_training`` produces so
    ``TrainModelList`` renders restored models identically to in-session ones.

    Parameters
    ----------
    project : Project | None
        The loaded project (or None).
    model_labels : dict, optional
        Map of ``model_type`` -> human label (e.g. ``{"glm": "GLM ..."}``).
        Missing entries (and an absent map) fall back to the raw ``model_type``.
    """
    if project is None or not getattr(project, "models", None):
        return []
    labels = model_labels or {}
    jobs: List[dict] = []
    for key, model in project.models.items():
        model_type = getattr(model, "model_type", "") or ""
        jobs.append(
            {
                # Registry key: stable + unique, and the key train_tile._do_remove
                # passes to delete_model — so id and model_storage_key both use it.
                "id": key,
                "model_type": model_type,
                "model_label": labels.get(model_type, model_type),
                "dataset_name": getattr(model, "dataset_name", None) or "—",
                "sample_name": getattr(model, "sample_name", None),
                "status": "completed",
                "error": None,
                "deviance": getattr(model, "deviance", None),
                "n_samples": getattr(model, "n_samples", None),
                "model_storage_key": key,
            }
        )
    return jobs


def build_inference_jobs(project: Any) -> List[dict]:
    """One 'completed' inference-job dict per (model_key, dataset_name) run.

    The inference tile groups a run's output rasters by (model_key,
    dataset_name) — see ``InferenceTile._matching_predictions`` — so restored
    jobs are keyed the same way. The per-job map toggle then resolves the right
    rasters via ``project.filter_predictions(model_key=..., dataset_name=...)``.
    """
    if project is None or not getattr(project, "predictions", None):
        return []
    seen: Dict[tuple, dict] = {}
    for pred in project.predictions.values():
        model_key = getattr(pred, "model_key", None) or "—"
        dataset_name = getattr(pred, "dataset_name", None) or "—"
        combo = (model_key, dataset_name)
        if combo in seen:
            continue
        seen[combo] = {
            "id": f"{model_key}__{dataset_name}",
            "model_key": model_key,
            "dataset_name": dataset_name,
            "status": "completed",
            "error": None,
            "output_path": "see project predictions",
        }
    return list(seen.values())

"""Single end-of-apply prediction-registration helper for the supervised path.

Builds a PredictionSpec-shaped payload (PR #8 fields minus the live .project
back-ref) and hands it to an injected register_prediction callable — the one
place GLM/RF/iCAR register their output (spec §9.2).
"""

from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union


def build_dataset_snapshot(dataset: Any) -> Dict[str, Any]:
    """References-only snapshot of a dataset (kept verbatim from PR #8).

    Avoids dataset.model_dump() so the live Dataset.project cycle is never
    traversed.
    """
    if dataset is None:
        return {}
    target = getattr(dataset, "target", None)
    return {
        "name": getattr(dataset, "name", None),
        "year": getattr(dataset, "year", None),
        "target_name": getattr(target, "name", None) if target is not None else None,
        "target_year": getattr(target, "year", None) if target is not None else None,
        "feature_names": [f.name for f in getattr(dataset, "features", [])],
    }


def make_prediction_payload(
    path: Union[str, Path],
    model_key: str,
    dataset: Optional[Any],
    year: Optional[int],
    model_year: Optional[int],
    window: Optional[int],
    model_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """Assemble the kwargs for ProjectSession.register_prediction / PredictionSpec."""
    ds_name = (getattr(dataset, "name", None) or "unknown") if dataset is not None else "unknown"
    return {
        "path": str(path),
        "model_key": model_key,
        "dataset_name": ds_name,
        "year": year if year is not None else model_year,
        "window": window,
        "model_snapshot": dict(model_snapshot),
        "dataset_snapshot": build_dataset_snapshot(dataset),
    }


def register_supervised(
    register_prediction: Optional[Callable],
    path: Union[str, Path],
    model_key: str,
    dataset: Optional[Any],
    year: Optional[int],
    model_year: Optional[int],
    window: Optional[int],
    model_snapshot: Dict[str, Any],
) -> None:
    """Register a single supervised prediction via the injected callable.

    No-ops when register_prediction is None (direct apply() outside a session).
    """
    if register_prediction is None:
        return
    payload = make_prediction_payload(
        path=path,
        model_key=model_key,
        dataset=dataset,
        year=year,
        model_year=model_year,
        window=window,
        model_snapshot=model_snapshot,
    )
    register_prediction(**payload)

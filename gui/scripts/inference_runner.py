# gui/scripts/inference_runner.py
"""Family-aware adapter: turn a (model, dataset) into the correct model.apply() call.

This is the single place where the ML-vs-JNR-vs-MW apply() signature divergence
lives. apply() auto-registers Prediction(s) on the project, so this returns nothing
but raises clear errors on missing preconditions.
"""

import logging
from pathlib import Path

from spatialrisk.evaluation import interval_from_target

logger = logging.getLogger("spatial_risk")

_ML_FOLDER = {"glm": "glm_model", "rf": "rf_model", "icar": "icar_model"}


def run_inference(project, model_key, dataset_name, name=None):
    """Run inference for one registered model on one dataset.

    Parameters
    ----------
    name : str, optional
        User-chosen name for this run's prediction output(s). When given, each
        run gets its own output subfolder (so distinct names don't clobber files
        on disk) and the prediction(s) are keyed/labelled by it (see
        ``BaseRiskModel._register_prediction``). The caller is responsible for
        passing a path-safe token. When omitted, the legacy provenance-derived
        paths and keys are used unchanged.

    Raises ValueError if preconditions are missing (no dataset target, no forest
    feature for ML models, unresolvable time interval for benchmark models).
    """
    model = project.models[model_key]
    dataset = project.get_dataset(dataset_name)
    if dataset is None:
        raise ValueError(f"Dataset '{dataset_name}' not found.")
    if getattr(dataset, "target", None) is None:
        raise ValueError(f"Dataset '{dataset_name}' has no target set.")

    family = model_key.split("_")[0]
    model.project = project
    model.dataset = dataset
    # Hand the name to _register_prediction. Set unconditionally (None when not
    # provided) so a stale name from a prior named run on the same model instance
    # can't leak into a later unnamed run; None keeps the provenance-derived key.
    model._pending_pred_name = name or None

    if family in _ML_FOLDER:
        mask = next((f.path for f in dataset.features if f.name == "forest_gfc"), None)
        if mask is None:
            raise ValueError(
                f"ML inference needs a 'forest_gfc' feature in dataset "
                f"'{dataset_name}' to use as a mask."
            )
        subfolder = name or (getattr(model, "name", None) or model_key)
        out_dir = Path(getattr(project.folders, _ML_FOLDER[family])) / subfolder
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{dataset_name}.tif"
        model.apply(out, dataset, mask, 0)
        return

    ti = interval_from_target(dataset.target.name)
    if ti is None:
        raise ValueError(
            f"Cannot derive time_interval from target '{dataset.target.name}'."
        )

    if family == "jnr":
        out_dir = Path(project.folders.rmj_bm) / (name or dataset_name)
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"prob_bm_{dataset_name}.tif"
        model.apply(out, dataset, time_interval=ti, deforate_model=None)
        return

    if family == "mw":
        out_folder = Path(project.folders.rmj_mw)
        if name:
            out_folder = out_folder / name
        model.apply(dataset, time_interval=ti, output_folder=out_folder)
        return

    raise ValueError(
        f"Unknown model family '{model_key.split('_')[0]}' for key '{model_key}'."
    )

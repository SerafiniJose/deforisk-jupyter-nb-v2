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


def run_inference(project, model_key, dataset_name):
    """Run inference for one registered model on one dataset.

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

    if family in _ML_FOLDER:
        mask = next((f.path for f in dataset.features if f.name == "forest_gfc"), None)
        if mask is None:
            raise ValueError(
                f"ML inference needs a 'forest_gfc' feature in dataset "
                f"'{dataset_name}' to use as a mask."
            )
        out_dir = Path(getattr(project.folders, _ML_FOLDER[family])) / getattr(
            model, "name", model_key)
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{dataset_name}.tif"
        model.apply(out, dataset, mask, 0)
        return

    ti = interval_from_target(dataset.target.name)
    if ti is None:
        raise ValueError(
            f"Cannot derive time_interval from target '{dataset.target.name}'.")

    if family == "jnr":
        out_dir = Path(project.folders.rmj_bm) / dataset_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"prob_bm_{dataset_name}.tif"
        model.apply(out, dataset, time_interval=ti, deforate_model=None)
        return

    if family == "mw":
        model.apply(dataset, time_interval=ti, output_folder=Path(project.folders.rmj_mw))
        return

    raise ValueError(f"Unknown model family '{family}' for key '{model_key}'.")

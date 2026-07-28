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

# Catalogue key of the Hansen layer used to *suggest* a mask in the Predict
# dialog. A dataset feature carries the *variable* name, which bakes in the
# layer's parameters ("forest_gfc_tc30"), so suggestions are found by resolving
# the name back to this key — never by comparing against it. The mask itself is
# generic: any dataset feature (or none) can be assigned.
_FOREST_CATALOGUE_KEY = "forest_gfc"


def is_ml_family(model_key):
    """Whether *model_key* names a family whose apply() takes a mask layer."""
    return str(model_key or "").split("_")[0] in _ML_FOLDER


def mask_layer_candidates(dataset):
    """Names of *dataset*'s features assignable as the ML mask layer.

    Every feature qualifies — the mask is a plain 1=keep/0=suppress raster with
    no assumption about where it came from. This is the single source the
    Predict dialog lists from, so the dialog can never offer a layer the runner
    would reject.
    """
    features = getattr(dataset, "features", None) or []
    return [f.name for f in features]


def suggested_mask_layer(dataset):
    """The feature name the Predict dialog seeds its mask select with.

    Masking to forest-at-period-start is the usual deforestation setup, so a
    sole Hansen-derived layer (``forest_gfc_tc<threshold>``, or the legacy bare
    ``forest_gfc``) is suggested. Zero or several such layers return "" — the
    choice is then the user's, because only they know which forest definition
    (or no mask at all) a run is meant to use.
    """
    # Imported inside the function: this module has no gui.scripts imports at
    # module scope, and keeping it that way avoids an import cycle.
    from gui.scripts.predefined_variables import resolve_predefined

    features = getattr(dataset, "features", None) or []
    forest = [
        f.name
        for f in features
        if resolve_predefined(f.name)[0] == _FOREST_CATALOGUE_KEY
    ]
    return forest[0] if len(forest) == 1 else ""


def _resolve_mask(dataset, dataset_name, mask_feature):
    """Path of the feature an ML run masks with, or None for no mask.

    ``mask_feature`` is the user's explicit choice from the Predict dialog;
    empty means "no mask" (predict over the full stack). Nothing is ever
    resolved on the user's behalf here — the dialog owns the forest suggestion.
    """
    if not mask_feature:
        return None
    features = list(getattr(dataset, "features", None) or [])
    feature = next((f for f in features if f.name == mask_feature), None)
    if feature is None:
        raise ValueError(
            f"Mask layer '{mask_feature}' is not in dataset "
            f"'{dataset_name}'. Available features: {[f.name for f in features]}."
        )
    return feature.path


def run_inference(project, model_key, dataset_name, name=None, mask_feature=None):
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
    mask_feature : str, optional
        Name of the dataset feature to mask with (ML families only), as
        assigned in the Predict dialog. Omitted or blank means no mask: the
        prediction covers the full raster stack. Ignored by the JNR/MW
        families, which resolve their own layers.

    Raises ValueError if preconditions are missing (no dataset target, a mask
    feature not in the dataset, unresolvable time interval for benchmark
    models).
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
        mask = _resolve_mask(dataset, dataset_name, mask_feature)
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

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

# Catalogue key of the Hansen layer ML models mask with. A dataset feature
# carries the *variable* name, which bakes in the layer's parameters
# ("forest_gfc_tc30"), so candidates are found by resolving the name back to
# this key — never by comparing against it.
_FOREST_CATALOGUE_KEY = "forest_gfc"


def is_ml_family(model_key):
    """Whether *model_key* names a family whose apply() takes a forest mask."""
    return str(model_key or "").split("_")[0] in _ML_FOLDER


def forest_feature_candidates(dataset):
    """Names of *dataset*'s features usable as the ML forest mask.

    A Hansen layer added from the Add Variable modal is named
    ``forest_gfc_tc<threshold>``, so an exact-name match misses it; each feature
    name is resolved back to its catalogue key instead. Legacy bare
    ``forest_gfc`` features still resolve, so older projects keep working.

    This is the single definition of "which features are forest masks". The
    Predict dialog lists exactly these and ``run_inference`` picks from exactly
    these, so the dialog can never offer a layer the runner would reject.
    """
    # Imported inside the function: this module has no gui.scripts imports at
    # module scope, and keeping it that way avoids an import cycle.
    from gui.scripts.predefined_variables import resolve_predefined

    features = getattr(dataset, "features", None) or []
    return [
        f.name
        for f in features
        if resolve_predefined(f.name)[0] == _FOREST_CATALOGUE_KEY
    ]


def _resolve_forest_mask(dataset, dataset_name, forest_feature):
    """Path of the feature an ML run masks with. Raises when unresolvable.

    ``forest_feature`` is the user's explicit choice from the Predict dialog.
    When it is empty the mask is resolved automatically, which only succeeds
    when the dataset holds exactly one forest feature: two candidates are never
    disambiguated here, because only the user knows which forest definition a
    run was meant to use.
    """
    features = list(getattr(dataset, "features", None) or [])
    if forest_feature:
        feature = next((f for f in features if f.name == forest_feature), None)
        if feature is None:
            raise ValueError(
                f"Forest feature '{forest_feature}' is not in dataset "
                f"'{dataset_name}'. Available features: {[f.name for f in features]}."
            )
        return feature.path

    candidates = forest_feature_candidates(dataset)
    if not candidates:
        raise ValueError(
            f"ML inference needs a Hansen forest feature (a "
            f"'{_FOREST_CATALOGUE_KEY}' layer, e.g. "
            f"'{_FOREST_CATALOGUE_KEY}_tc30') in dataset '{dataset_name}' to "
            f"use as a mask."
        )
    if len(candidates) > 1:
        raise ValueError(
            f"Dataset '{dataset_name}' has more than one forest feature "
            f"({', '.join(candidates)}). Choose which one to use as the mask "
            f"in the Predict dialog."
        )
    return next(f.path for f in features if f.name == candidates[0])


def run_inference(project, model_key, dataset_name, name=None, forest_feature=None):
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
    forest_feature : str, optional
        Name of the dataset feature to mask with (ML families only), as chosen
        in the Predict dialog. Omitted or blank means "resolve it for me", which
        works whenever the dataset holds exactly one forest feature. Ignored by
        the JNR/MW families, which resolve their own layers.

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
        mask = _resolve_forest_mask(dataset, dataset_name, forest_feature)
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

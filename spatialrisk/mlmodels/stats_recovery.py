"""Read-only recovery of model statistics from on-disk artifacts (Spec A §3).

For models trained before the ``stats`` field existed. NEVER writes back into
the model or project, and creates nothing on disk — opening a dialog must not
trigger a save, nor rebuild a folder tree the user deleted. That extends to how
the MW/JNR output folder is resolved: ``project.folders`` is a property that
mkdirs the whole project tree, so it is deliberately avoided (see
``_model_folder``). Returns None on any failure: recovery is best-effort by
contract, and the caller shows an empty state rather than an error.

GLM/RF pickles store no coefficient names, so the patsy design info is rebuilt
from samples_path + formula — the same reconstruction apply() performs
(glm_model.py). feature_names is never used for labels: the design matrix
expands factors, so it is neither the right length nor order.

MW/JNR values are re-read from the tab_dist.csv the fit already wrote and are
handed to ``build_rmj_stats`` with ``float("nan")`` for anything the table
cannot supply; the schema coerces non-finite floats to None, so "missing" and
"unrecoverable" read back identically.
"""

import pickle
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np

if TYPE_CHECKING:  # avoid importing the schema at module import time
    from spatialrisk.mlmodels.stats import ModelStatsBase


def recover_stats(model) -> Optional["ModelStatsBase"]:
    """Best-effort stats for a pre-change model; None when unrecoverable."""
    mt = getattr(model, "model_type", None)
    try:
        if mt == "glm":
            return _recover_sklearn(model, kind="glm")
        if mt == "rf":
            return _recover_sklearn(model, kind="rf")
        if mt == "icar":
            return _recover_icar(model)
        if mt in ("mw", "jnr"):
            return _recover_rmj(model)
    except Exception:
        # Recovery is best-effort by contract: a corrupt pickle, a renamed
        # column or a formula the CSV no longer satisfies must leave the caller
        # showing an empty state, never an error dialog.
        return None
    return None


def _load_pickle(model) -> Optional[dict]:
    """Payload dict from the model's pickle, or None when it is unavailable."""
    path = getattr(model, "model_path", None)
    if not path or not Path(path).exists():
        return None
    with open(path, "rb") as fh:
        return pickle.load(fh)


def _design_from_samples(formula, samples_path, *, add_cell=False):
    """(y, x) patsy matrices rebuilt from the stored training CSV."""
    import pandas as pd
    from patsy import dmatrices

    if not formula or not samples_path or not Path(samples_path).exists():
        return None, None
    df = pd.read_csv(samples_path).dropna()
    if add_cell and "cell" not in df.columns:
        # The iCAR formula carries '+ cell' but the CSV stores cell_id; the
        # design's column NAMES don't depend on a numeric term's values.
        df = df.assign(cell=0)
    return dmatrices(formula, df, NA_action="drop")


def _recover_sklearn(model, *, kind):
    """GLMStats/RFStats from a saved estimator plus its rebuilt design."""
    from spatialrisk.mlmodels.stats import (
        collect_glm_stats,
        collect_rf_stats,
        sample_design_label,
    )

    payload = _load_pickle(model)
    if not payload:
        return None
    clf = payload.get("ml_model")
    formula = payload.get("formula") or getattr(model, "formula", None)
    samples = payload.get("samples_path") or getattr(model, "samples_path", None)
    y, x = _design_from_samples(formula, samples)
    if clf is None or x is None:
        return None
    y_arr = np.asarray(y)[:, 0]
    common = dict(
        n_rows=int(x.shape[0]),
        n_events=int(y_arr.sum()),
        sample_design=sample_design_label(getattr(model, "sample", None)),
    )
    if kind == "glm":
        return collect_glm_stats(
            clf,
            x.design_info,
            max_iter=getattr(model, "max_iter", None),
            **common,
        )
    return collect_rf_stats(clf, x.design_info, **common)


def _recover_icar(model):
    """Point estimates only — the posterior died with the MCMC subprocess."""
    from spatialrisk.mlmodels.icar_model import build_icar_stats
    from spatialrisk.mlmodels.stats import sample_design_label

    payload = _load_pickle(model)
    if not payload:
        return None
    ml = payload.get("ml_model") or {}
    icar_formula = ml.get("formula")
    samples = payload.get("samples_path") or getattr(model, "samples_path", None)
    y, x = _design_from_samples(icar_formula, samples, add_cell=True)
    if x is None or "betas" not in ml:
        return None
    posteriors = {
        "betas": ml["betas"],
        "rho": ml.get("rho", []),
        "Vrho": ml.get("Vrho"),
        "posterior_summary": None,
    }
    return build_icar_stats(
        posteriors,
        x.design_info.column_names,
        n_rows=int(x.shape[0]),
        n_events=int(np.asarray(y)[:, 0].sum()),
        sample_design=sample_design_label(getattr(model, "sample", None)),
    )


# Family -> output sub-folder inside the project directory, mirroring
# Project.initialize_folders(). The models' own _default_folder() cannot be
# used here: it reads project.folders, a property that calls
# initialize_folders(), which mkdirs the project folder and all ten of its
# sub-folders. That would make opening a stats dialog resurrect a deleted
# folder tree, and on a read-only mount the mkdir raises — losing a table that
# is sitting right there. Project._project_dir() is the pure accessor.
_RMJ_SUBFOLDER = {"mw": "rmj_mw", "jnr": "rmj_bm"}


def _model_folder(model) -> Optional[Path]:
    """Output folder of an MW/JNR model, resolved without creating anything."""
    subfolder = _RMJ_SUBFOLDER.get(getattr(model, "model_type", None))
    # _project_dir() is 'downloads_folder / project_name' and nothing else.
    project_dir = getattr(getattr(model, "project", None), "_project_dir", None)
    if subfolder is None or project_dir is None:
        return None
    return Path(project_dir()) / subfolder


def _find_tab_dist(model) -> Optional[Path]:
    """Locate <folder>/<period>/tab_dist.csv for an MW/JNR model."""
    # MW: the period dir is where the ldefrate rasters live.
    for p in (getattr(model, "ldefrate_files", None) or {}).values():
        cand = Path(p).parent / "tab_dist.csv"
        if cand.exists():
            return cand
    # JNR (and MW fallback): fit() writes into <model folder>/<period>/, where
    # period is the training dataset's name (falling back to the model's own).
    # Only that exact folder is accepted — every model of a family shares the
    # root, one sub-folder per period, so a search would risk attaching another
    # period's numbers to this model.
    folder = _model_folder(model)
    period = getattr(model, "dataset_name", None) or getattr(model, "name", None)
    if folder is None or not period:
        return None
    cand = folder / str(period) / "tab_dist.csv"
    return cand if cand.exists() else None


def _recover_rmj(model):
    """MWStats/JNRStats re-read from the period's tab_dist.csv."""
    import pandas as pd

    from spatialrisk.mlmodels.stats import build_rmj_stats

    tab = _find_tab_dist(model)
    if tab is None:
        return None
    t = pd.read_csv(tab)
    if t.empty:
        return None
    # fit() stores dist_edge_threshold's ``tot_def``, the area of ALL deforested
    # pixels, whereas ``cum`` accumulates only the pixels that fell inside
    # dist_bins. Taking cum.iloc[-1] would under-report the headline figure by
    # (100 - perc.iloc[-1])%. The table carries perc = 100 * cum / tot_def, so
    # the original value divides back out exactly.
    cum_last = float(t["cum"].iloc[-1])
    perc_last = float(t["perc"].iloc[-1])
    tot_def = 100.0 * cum_last / perc_last if perc_last > 0 else float("nan")
    dist_thresh = getattr(model, "dist_thresh", None)
    perc = None
    if dist_thresh is not None:
        at = t[t["distance"] == dist_thresh]
        # Rounded exactly as dist_edge_threshold rounds it, so the same model
        # reads the same way whether its stats were recovered or freshly fit.
        perc = float(np.around(at["perc"].iloc[0], 2)) if not at.empty else None
    pngs = sorted(tab.parent.glob("perc_dist_*.png"))
    png = pngs[-1] if pngs else None
    result = {
        "tot_def": tot_def,
        "dist_thresh": dist_thresh if dist_thresh is not None else float("nan"),
        "perc_thresh": perc if perc is not None else float("nan"),
    }
    # The stats class follows the model family, never the data: a JNR model
    # with unpopulated dist_bins must still produce JNRStats, because that is
    # what JNRBenchmarkModel.stats is typed to hold.
    n_classes = None
    if getattr(model, "model_type", None) == "jnr":
        n_classes = max(len(getattr(model, "dist_bins", None) or []) - 1, 0)
    stats = build_rmj_stats(
        result,
        tab_dist_path=tab,
        # build_rmj_stats requires a figure path; the real value is set below.
        # Recording a path that cannot exist would be worse than recording
        # nothing, so a missing figure leaves the field empty.
        perc_dist_png=png or tab,
        n_classes=n_classes,
    )
    stats.perc_dist_png = png
    return stats

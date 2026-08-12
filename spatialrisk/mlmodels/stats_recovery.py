"""Read-only recovery of model statistics from on-disk artifacts (Spec A §3).

For models trained before the ``stats`` field existed. NEVER writes back into
the model or project — opening a dialog must not trigger a save. Returns None
on any failure: recovery is best-effort by contract, and the caller shows an
empty state rather than an error.

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


def _find_tab_dist(model) -> Optional[Path]:
    """Locate <folder>/<period>/tab_dist.csv for an MW/JNR model."""
    # MW: the period dir is where the ldefrate rasters live.
    for p in (getattr(model, "ldefrate_files", None) or {}).values():
        cand = Path(p).parent / "tab_dist.csv"
        if cand.exists():
            return cand
    # JNR (and MW fallback): search the model's own output folder.
    folder = None
    if getattr(model, "project", None) is not None:
        folder = model._default_folder()
    if not folder or not Path(folder).exists():
        return None
    folder = Path(folder)
    # fit() writes into <folder>/<period>/, where period is the training
    # dataset's name (falling back to the model's own name).
    period = getattr(model, "dataset_name", None) or getattr(model, "name", None)
    if period:
        cand = folder / str(period) / "tab_dist.csv"
        if cand.exists():
            return cand
    # Last resort: one unambiguous table. Every model of a family shares that
    # folder, one sub-folder per period, so choosing among several would
    # attach another model's numbers to this one.
    hits = sorted(folder.glob("*/tab_dist.csv"))
    return hits[0] if len(hits) == 1 else None


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
    tot_def = float(t["cum"].iloc[-1])
    dist_thresh = getattr(model, "dist_thresh", None)
    perc = None
    if dist_thresh is not None:
        at = t[t["distance"] == dist_thresh]
        perc = float(at["perc"].iloc[0]) if not at.empty else None
    pngs = sorted(tab.parent.glob("perc_dist_*.png"))
    png = pngs[-1] if pngs else None
    result = {
        "tot_def": tot_def,
        "dist_thresh": dist_thresh if dist_thresh is not None else float("nan"),
        "perc_thresh": perc if perc is not None else float("nan"),
    }
    dist_bins = getattr(model, "dist_bins", None)
    stats = build_rmj_stats(
        result,
        tab_dist_path=tab,
        # build_rmj_stats requires a figure path; the real value is set below.
        # Recording a path that cannot exist would be worse than recording
        # nothing, so a missing figure leaves the field empty.
        perc_dist_png=png or tab,
        n_classes=max(len(dist_bins) - 1, 0) if dist_bins else None,
    )
    stats.perc_dist_png = png
    return stats

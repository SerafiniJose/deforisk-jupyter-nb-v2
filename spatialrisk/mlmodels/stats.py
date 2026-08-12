"""Typed per-family training statistics + pure collectors (Spec A §1-2).

Schema classes are plain pydantic models nested inside each BaseRiskModel
subclass's ``stats`` field; they ride the existing project JSON round-trip
(model_dump(mode="json") on save, model_cls(**data) on load) with no manual
path rehydration — nested ``Path`` fields are coerced by pydantic.

Collectors are pure functions over already-fitted objects so they are
unit-testable without dataset/sample fixtures. Coefficient names ALWAYS come
from the patsy design info, never from ``feature_names`` — the design matrix
expands factors, so feature_names is neither the right length nor order.
"""

import math
from pathlib import Path
from typing import List, Optional

import numpy as np
from pydantic import BaseModel, field_validator


class _FiniteFloats(BaseModel):
    """Coerce non-finite floats to None so ``nan`` never reaches JSON/UI."""

    @field_validator("*", mode="before")
    @classmethod
    def _drop_non_finite(cls, v):
        if isinstance(v, float) and not math.isfinite(v):
            return None
        return v


class Coefficient(_FiniteFloats):
    """One fitted term. std/ci_* stay None outside the Bayesian family.

    ``odds_ratio`` is deliberately NOT stored — the GUI computes
    exp(estimate) at display time (spec §2.1).
    """

    name: str
    estimate: Optional[float] = None
    std: Optional[float] = None
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None


class Importance(_FiniteFloats):
    """One named feature importance value (e.g. RF feature_importances_)."""

    name: str
    value: float


class ModelStatsBase(_FiniteFloats):
    """Shared context.

    n_rows is the post-NA-drop design row count; n_events counts target==1
    rows (1=event by convention). sample_design names the sampling strategy
    so a balanced case-control count is never mistaken for landscape
    prevalence.
    """

    n_rows: Optional[int] = None
    n_events: Optional[int] = None
    sample_design: Optional[str] = None


class GLMStats(ModelStatsBase):
    """Fitted GLM (logistic regression) statistics."""

    coefficients: List[Coefficient] = []
    # patsy emits an 'Intercept' design column AND sklearn fits its own
    # intercept_; the effective intercept is the sum. Two fields, labelled
    # separately (spec §2.1).
    intercept_design: Optional[float] = None
    intercept_fitted: Optional[float] = None
    n_iter: Optional[int] = None
    max_iter: Optional[int] = None


class RFStats(ModelStatsBase):
    """Fitted Random Forest statistics."""

    importances: List[Importance] = []
    # sklearn oob_score_ — plain accuracy on the training sample, never to be
    # presented as validation (spec §2.3).
    oob_accuracy: Optional[float] = None


class ICARStats(ModelStatsBase):
    """Fitted iCAR (Bayesian spatial random effect) statistics."""

    coefficients: List[Coefficient] = []
    vrho: Optional[Coefficient] = None
    # Summary of the cell-level spatial random effect vector (rho at the
    # native csize cells, not the interpolated raster).
    rho_min: Optional[float] = None
    rho_max: Optional[float] = None
    rho_mean: Optional[float] = None
    rho_std: Optional[float] = None


class MWStats(ModelStatsBase):
    """Moving-window deforestation-risk statistics."""

    dist_thresh: Optional[float] = None
    perc_thresh: Optional[float] = None
    tot_defor_ha: Optional[float] = None
    tab_dist_path: Optional[Path] = None
    perc_dist_png: Optional[Path] = None


class JNRStats(MWStats):
    """JNR statistics: MWStats plus the risk-map class count."""

    n_classes: Optional[int] = None


def sample_design_label(sample) -> Optional[str]:
    """'strategy[, allocation] (name)' from a Sample record, or None."""
    if sample is None:
        return None
    parts = [getattr(sample, "strategy", None), getattr(sample, "allocation", None)]
    desc = ", ".join(str(p) for p in parts if p)
    name = getattr(sample, "name", None)
    if not desc and not name:
        return None
    return f"{desc} ({name})" if name else desc


def _named_values(design_info, values, what):
    names = list(design_info.column_names)
    vals = np.asarray(values, dtype=float)
    if len(names) != len(vals):
        raise ValueError(
            f"design has {len(names)} columns but the model has "
            f"{len(vals)} {what} — refusing to zip mismatched labels"
        )
    return list(zip(names, vals))


def collect_glm_stats(
    clf, design_info, *, n_rows, n_events, sample_design, max_iter
) -> GLMStats:
    """GLMStats from a fitted LogisticRegression + its patsy design info."""
    intercept_design = None
    rows = []
    for name, est in _named_values(design_info, clf.coef_[0], "coefficients"):
        if name == "Intercept":
            intercept_design = float(est)
        else:
            rows.append(Coefficient(name=name, estimate=float(est)))
    n_iter = getattr(clf, "n_iter_", None)
    return GLMStats(
        n_rows=n_rows,
        n_events=n_events,
        sample_design=sample_design,
        coefficients=rows,
        intercept_design=intercept_design,
        intercept_fitted=float(np.asarray(clf.intercept_)[0]),
        n_iter=int(np.asarray(n_iter).ravel()[0]) if n_iter is not None else None,
        max_iter=max_iter,
    )


def collect_rf_stats(clf, design_info, *, n_rows, n_events, sample_design) -> RFStats:
    """RFStats from a fitted RandomForestClassifier + its design info.

    The constant 'Intercept' design column is dropped (importance 0 by
    construction); rows are sorted descending for display.
    """
    rows = [
        Importance(name=name, value=float(v))
        for name, v in _named_values(
            design_info, clf.feature_importances_, "importances"
        )
        if name != "Intercept"
    ]
    rows.sort(key=lambda r: r.value, reverse=True)
    oob = getattr(clf, "oob_score_", None)
    return RFStats(
        n_rows=n_rows,
        n_events=n_events,
        sample_design=sample_design,
        importances=rows,
        oob_accuracy=float(oob) if oob is not None else None,
    )


def summarize_icar_mcmc(mcmc, column_names) -> dict:
    """Posterior mean/SD/95% CI from forestatrisk's retained trace.

    Layout is fixed by forestatrisk's model_binomial_iCAR: trace columns are
    [betas..., Vrho, Deviance] and the design's FINAL column is always
    consumed as the spatial 'cell' term, so beta names = column_names[:-1].
    Both facts are asserted — a silent off-by-one would mislabel every
    coefficient (spec §2.2).

    Returns plain dicts/lists (picklable across the MCMC process boundary).
    """
    names = list(column_names)
    if not names or names[-1] != "cell":
        raise ValueError(
            "expected the design's last column to be 'cell' (forestatrisk "
            f"strips it as the spatial term); got {names[-1] if names else None!r}"
        )
    beta_names = names[:-1]
    arr = np.asarray(mcmc, dtype=float)
    if arr.ndim != 2 or arr.shape[1] != len(beta_names) + 2:
        raise ValueError(
            f"trace has shape {arr.shape}; expected "
            f"(n, {len(beta_names)} betas + Vrho + Deviance)"
        )

    def _summ(col):
        return {
            "mean": float(np.mean(col)),
            "std": float(np.std(col, ddof=1)),
            "ci_low": float(np.percentile(col, 2.5)),
            "ci_high": float(np.percentile(col, 97.5)),
        }

    return {
        "betas": [{"name": n, **_summ(arr[:, i])} for i, n in enumerate(beta_names)],
        "vrho": _summ(arr[:, -2]),
    }


def build_rmj_stats(result, *, tab_dist_path, perc_dist_png, n_classes=None):
    """MW/JNR stats from dist_edge_threshold's return dict (Spec A §2.4).

    Keeps tot_def and perc_thresh, which the fit methods currently discard,
    and records the tab_dist.csv / perc_dist png the models already write.
    n_classes given -> JNRStats, else MWStats.

    ``result`` is read tolerantly: a missing key yields a None-valued field
    instead of raising, exactly like a present-but-non-finite (nan/inf) value
    is coerced to None by the schema's _FiniteFloats validator below. The two
    cases are deliberately unified — a later recovery task (A7) rebuilds this
    dict from partial on-disk state and calls this function with
    ``float("nan")`` for values it cannot recover, so "missing" and
    "unrecoverable" must behave identically rather than one of them raising.
    """

    def _optional_float(key):
        value = result.get(key)
        return None if value is None else float(value)

    kwargs = dict(
        dist_thresh=_optional_float("dist_thresh"),
        perc_thresh=_optional_float("perc_thresh"),
        tot_defor_ha=_optional_float("tot_def"),
        tab_dist_path=Path(tab_dist_path),
        perc_dist_png=Path(perc_dist_png),
    )
    if n_classes is not None:
        return JNRStats(n_classes=int(n_classes), **kwargs)
    return MWStats(**kwargs)

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

    ``rhat``/``ess`` are per-parameter MCMC convergence diagnostics
    (see ``mcmc_diagnostics``); only the iCAR family fills them.
    """

    name: str
    estimate: Optional[float] = None
    std: Optional[float] = None
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None
    rhat: Optional[float] = None
    ess: Optional[float] = None


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
    # Posterior of the deviance trace column (mean/SD/CI as a Coefficient) —
    # the model's own `deviance` attribute is a point value without spread.
    deviance_summary: Optional[Coefficient] = None
    # Intercept-only reference deviance for "% explained" (closed form from
    # the sample counts — see binomial_null_deviance).
    null_deviance: Optional[float] = None
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


def mcmc_diagnostics(col):
    """``(split_rhat, ess)`` for one trace column, or ``(None, None)``.

    The numeric stand-in for deforisk's mcmc.pdf trace plots: split R-hat
    (Gelman-Rubin on the chain's two halves — a trending, unconverged chain
    has halves with different means, pushing R-hat above 1) and effective
    sample size from the autocorrelation, truncated by Geyer's initial
    positive-sequence rule (summed pairs of autocorrelations stay positive
    for a stationary chain; stop at the first non-positive pair).

    A degenerate column — constant, or too short to split — diagnoses
    nothing and returns ``(None, None)`` rather than dividing by zero.
    """
    x = np.asarray(col, dtype=float)
    half = x.size // 2
    # np.ptp, not a variance test: a constant chain's variance computes as a
    # ~1e-31 residue of mean subtraction, not 0.0, and would sail past a
    # zero-variance guard into nonsense diagnostics. The range is exact.
    if half < 2 or np.ptp(x) == 0:
        return (None, None)

    # split R-hat over m=2 half-chains of length `half`
    a, b = x[:half], x[half : 2 * half]
    within = (a.var(ddof=1) + b.var(ddof=1)) / 2.0
    if within == 0:
        return (None, None)
    grand = (a.mean() + b.mean()) / 2.0
    between = half * ((a.mean() - grand) ** 2 + (b.mean() - grand) ** 2)
    var_plus = (half - 1) / half * within + between / half
    rhat = float(np.sqrt(var_plus / within))

    # ESS = n / (1 + 2 * sum of autocorrelations kept by Geyer's rule)
    n = x.size
    centred = x - x.mean()
    autocov = np.correlate(centred, centred, "full")[n - 1 :] / n
    rho = autocov / autocov[0]
    tail = 0.0
    t = 1
    while t + 1 < n:
        pair = rho[t] + rho[t + 1]
        if pair <= 0:
            break
        tail += pair
        t += 2
    ess = float(np.clip(n / (1.0 + 2.0 * tail), 1.0, n))
    return (rhat, ess)


def binomial_null_deviance(n_events, n_rows):
    """Deviance of the intercept-only binomial model, in closed form.

    The reference for a "% of deviance explained" figure (deforisk's
    model_deviances.csv fitted a whole sklearn LogisticRegression for it; the
    intercept-only MLE is just the event rate, so the number is exact here).
    Undefined when the sample is all-event or event-free — the log of a zero
    rate — so those return None and the comparison is simply not offered.
    """
    if not n_rows or not n_events or n_events >= n_rows:
        return None
    p = n_events / n_rows
    return -2.0 * (n_events * math.log(p) + (n_rows - n_events) * math.log(1.0 - p))


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
        rhat, ess = mcmc_diagnostics(col)
        return {
            "mean": float(np.mean(col)),
            "std": float(np.std(col, ddof=1)),
            "ci_low": float(np.percentile(col, 2.5)),
            "ci_high": float(np.percentile(col, 97.5)),
            "rhat": rhat,
            "ess": ess,
        }

    return {
        "betas": [{"name": n, **_summ(arr[:, i])} for i, n in enumerate(beta_names)],
        "vrho": _summ(arr[:, -2]),
        # Deviance is a trace column like any other (deforisk's summary printed
        # it as a table row); its posterior spread rides along here.
        "deviance": _summ(arr[:, -1]),
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

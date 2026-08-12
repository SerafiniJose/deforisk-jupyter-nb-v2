"""Schema + pure collectors for per-family training statistics (Spec A §1-2)."""

import numpy as np
import pytest

from spatialrisk.mlmodels.stats import (
    Coefficient,
    JNRStats,
    MWStats,
    build_rmj_stats,
    collect_glm_stats,
    collect_rf_stats,
    sample_design_label,
    summarize_icar_mcmc,
)


class _FakeDesignInfo:
    def __init__(self, names):
        self.column_names = list(names)


class _FakeSample:
    def __init__(self, name="random_1", strategy="random", allocation=None):
        self.name, self.strategy, self.allocation = name, strategy, allocation


def test_non_finite_floats_coerce_to_none():
    """NaN/inf floats are coerced to None on validation."""
    c = Coefficient(name="x", estimate=float("nan"), std=float("inf"))
    assert c.estimate is None and c.std is None
    s = MWStats(dist_thresh=float("nan"))
    assert s.dist_thresh is None


def test_finite_values_survive():
    """Finite floats pass through the validator unchanged."""
    c = Coefficient(name="x", estimate=-0.4746, ci_low=-0.6, ci_high=-0.3)
    assert c.estimate == -0.4746 and c.ci_low == -0.6


def test_jnr_stats_extends_mw_stats():
    """JNRStats is-a MWStats and carries n_classes."""
    s = JNRStats(dist_thresh=2010.0, perc_thresh=99.5, n_classes=29)
    assert isinstance(s, MWStats) and s.n_classes == 29


def test_sample_design_label():
    """sample_design_label formats 'strategy[, allocation] (name)', or None."""
    assert sample_design_label(_FakeSample()) == "random (random_1)"
    assert (
        sample_design_label(_FakeSample(allocation="balanced"))
        == "random, balanced (random_1)"
    )
    assert sample_design_label(None) is None


def _fit_glm():
    """Tiny fitted LogisticRegression on a 3-column design incl. Intercept."""
    from sklearn.linear_model import LogisticRegression

    rng = np.random.default_rng(0)
    x = np.column_stack([np.ones(200), rng.normal(size=200), rng.normal(size=200)])
    y = (x[:, 1] + rng.normal(scale=0.5, size=200) > 0).astype(int)
    clf = LogisticRegression(max_iter=500).fit(x, y)
    return clf, _FakeDesignInfo(["Intercept", "scale(a)", "scale(b)"]), y


def test_collect_glm_stats_splits_the_two_intercepts():
    """collect_glm_stats splits the patsy Intercept from sklearn's intercept_."""
    clf, di, y = _fit_glm()
    s = collect_glm_stats(
        clf,
        di,
        n_rows=200,
        n_events=int(y.sum()),
        sample_design="random (r1)",
        max_iter=500,
    )
    # The patsy Intercept column is NOT a coefficient row; it and sklearn's
    # own intercept_ are stored as two separate fields (spec §2.1).
    assert [c.name for c in s.coefficients] == ["scale(a)", "scale(b)"]
    assert s.intercept_design == pytest.approx(float(clf.coef_[0][0]))
    assert s.intercept_fitted == pytest.approx(float(clf.intercept_[0]))
    assert s.n_iter == int(np.asarray(clf.n_iter_)[0]) and s.max_iter == 500


def test_collect_glm_stats_rejects_shape_mismatch():
    """collect_glm_stats raises when design columns and coef_ counts differ."""
    clf, _, y = _fit_glm()
    with pytest.raises(ValueError, match="columns"):
        collect_glm_stats(
            clf,
            _FakeDesignInfo(["Intercept", "only_one"]),
            n_rows=200,
            n_events=1,
            sample_design=None,
            max_iter=500,
        )


def test_collect_rf_stats_sorted_descending_without_intercept():
    """collect_rf_stats drops Intercept and sorts importances descending."""
    from sklearn.ensemble import RandomForestClassifier

    rng = np.random.default_rng(0)
    x = np.column_stack([np.ones(200), rng.normal(size=200), rng.normal(size=200)])
    y = (x[:, 1] > 0).astype(int)
    clf = RandomForestClassifier(n_estimators=20, oob_score=True, random_state=0).fit(
        x, y
    )
    s = collect_rf_stats(
        clf,
        _FakeDesignInfo(["Intercept", "a", "b"]),
        n_rows=200,
        n_events=int(y.sum()),
        sample_design=None,
    )
    names = [i.name for i in s.importances]
    assert "Intercept" not in names
    vals = [i.value for i in s.importances]
    assert vals == sorted(vals, reverse=True)
    assert s.oob_accuracy == pytest.approx(float(clf.oob_score_))


def test_summarize_icar_mcmc_slices_and_names():
    """summarize_icar_mcmc slices betas/Vrho and labels them from column_names."""
    # Layout fixed by forestatrisk model_binomial_iCAR: columns are
    # [betas..., Vrho, Deviance]; design names minus trailing 'cell' term.
    rng = np.random.default_rng(0)
    trace = np.column_stack(
        [
            rng.normal(loc=-3.0, scale=0.1, size=400),  # beta Intercept
            rng.normal(loc=0.43, scale=0.05, size=400),  # beta rivers
            rng.normal(loc=30.0, scale=2.0, size=400),  # Vrho
            rng.normal(loc=9000.0, scale=10.0, size=400),  # Deviance
        ]
    )
    out = summarize_icar_mcmc(trace, ["Intercept", "scale(rivers)", "cell"])
    assert [b["name"] for b in out["betas"]] == ["Intercept", "scale(rivers)"]
    b = out["betas"][1]
    assert b["mean"] == pytest.approx(0.43, abs=0.02)
    assert b["ci_low"] < b["mean"] < b["ci_high"]
    assert out["vrho"]["mean"] == pytest.approx(30.0, abs=0.5)


def test_summarize_icar_mcmc_guards_the_cell_column():
    """summarize_icar_mcmc guards against a mislabeled or wrong-shaped trace."""
    # An off-by-one here would silently mislabel every coefficient — the
    # guard must raise, not guess (spec §2.2).
    trace = np.zeros((10, 4))
    with pytest.raises(ValueError, match="cell"):
        summarize_icar_mcmc(trace, ["Intercept", "scale(a)", "not_cell"])
    with pytest.raises(ValueError, match="expected"):
        summarize_icar_mcmc(np.zeros((10, 5)), ["Intercept", "a", "cell"])


def test_build_rmj_stats_keeps_the_discarded_fields(tmp_path):
    """build_rmj_stats keeps tot_def and perc_thresh from dist_edge_threshold."""
    result = {"tot_def": 316892.88, "dist_thresh": 2010.0, "perc_thresh": 99.5}
    s = build_rmj_stats(
        result,
        tab_dist_path=tmp_path / "tab_dist.csv",
        perc_dist_png=tmp_path / "perc_dist_p.png",
    )
    assert isinstance(s, MWStats) and not isinstance(s, JNRStats)
    assert s.dist_thresh == 2010.0 and s.perc_thresh == 99.5
    assert s.tot_defor_ha == 316892.88
    assert s.tab_dist_path == tmp_path / "tab_dist.csv"
    # n_rows/n_events stay None: no training table, and an events count
    # without a forest denominator would be meaningless (spec §2.4).
    assert s.n_rows is None and s.n_events is None


def test_build_rmj_stats_jnr_variant_counts_classes(tmp_path):
    """build_rmj_stats with n_classes returns JNRStats with class count."""
    s = build_rmj_stats(
        {"tot_def": 1.0, "dist_thresh": 270.0, "perc_thresh": 99.5},
        tab_dist_path=tmp_path / "t.csv",
        perc_dist_png=tmp_path / "p.png",
        n_classes=29,
    )
    assert isinstance(s, JNRStats) and s.n_classes == 29


def test_build_rmj_stats_tolerates_a_partial_result(tmp_path):
    """A dist_edge_threshold result missing keys yields None fields, not error.

    Fix round 1 regression: stub_rmj in test_jnr_untagged_defor_var.py
    returns only dist_thresh, which used to raise
    ``KeyError: 'perc_thresh'`` and abort JNRBenchmarkModel.fit().
    """
    s = build_rmj_stats(
        {"dist_thresh": 120.0},
        tab_dist_path=tmp_path / "tab_dist.csv",
        perc_dist_png=tmp_path / "perc_dist_p.png",
    )
    assert s.dist_thresh == 120.0
    assert s.perc_thresh is None
    assert s.tot_defor_ha is None


def test_build_rmj_stats_missing_and_non_finite_are_equivalent(tmp_path):
    """A missing key and an unrecoverable value both read back as None.

    This is the behaviour Task A7's disk-recovery path relies on: it calls
    build_rmj_stats with float("nan") for fields it cannot reconstruct.
    """
    missing = build_rmj_stats(
        {"dist_thresh": 120.0},
        tab_dist_path=tmp_path / "a.csv",
        perc_dist_png=tmp_path / "a.png",
    )
    nan_valued = build_rmj_stats(
        {"dist_thresh": 120.0, "perc_thresh": float("nan"), "tot_def": float("nan")},
        tab_dist_path=tmp_path / "a.csv",
        perc_dist_png=tmp_path / "a.png",
    )
    assert missing.perc_thresh is None and missing.tot_defor_ha is None
    assert nan_valued.perc_thresh is None and nan_valued.tot_defor_ha is None

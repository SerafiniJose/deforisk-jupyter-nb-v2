"""iCAR posterior summary handling (Spec A §2.2) — no MCMC run needed."""

import numpy as np
import pytest

from spatialrisk.mlmodels.icar_model import build_icar_stats


def _posteriors(with_summary=True):
    p = {
        "betas": np.array([-3.11, 0.43]),
        "rho": np.array([0.5, -0.5, 1.5, np.nan]),
        "Vrho": 31.78,
        "deviance": float("nan"),
        "posterior_summary": None,
    }
    if with_summary:
        p["posterior_summary"] = {
            "betas": [
                {
                    "name": "Intercept",
                    "mean": -3.11,
                    "std": 0.1,
                    "ci_low": -3.3,
                    "ci_high": -2.9,
                },
                {
                    "name": "scale(rivers)",
                    "mean": 0.43,
                    "std": 0.05,
                    "ci_low": 0.33,
                    "ci_high": 0.53,
                },
            ],
            "vrho": {"mean": 31.78, "std": 2.0, "ci_low": 28.0, "ci_high": 36.0},
        }
    return p


def test_build_icar_stats_keeps_diagnostics_and_the_deviance_summary():
    """rhat/ess ride each coefficient; the deviance posterior gets its row."""
    p = _posteriors()
    p["posterior_summary"]["betas"][0].update(rhat=1.01, ess=850.0)
    p["posterior_summary"]["vrho"].update(rhat=1.08, ess=300.0)
    p["posterior_summary"]["deviance"] = {
        "mean": 9000.0,
        "std": 10.0,
        "ci_low": 8980.0,
        "ci_high": 9020.0,
        "rhat": 1.0,
        "ess": 400.0,
    }
    s = build_icar_stats(
        p,
        ["Intercept", "scale(rivers)", "cell"],
        n_rows=100,
        n_events=40,
        sample_design=None,
    )
    assert s.coefficients[0].rhat == 1.01 and s.coefficients[0].ess == 850.0
    assert s.coefficients[1].rhat is None  # an old summary without them is fine
    assert s.vrho.rhat == 1.08
    assert s.deviance_summary.estimate == 9000.0
    assert s.deviance_summary.ci_high == 9020.0


def test_build_icar_stats_computes_the_null_deviance():
    """The intercept-only reference comes from the sample counts alone.

    So both the summary path and the point-estimate fallback (= the recovery
    path for old models) provide it.
    """
    import math

    expected = -2.0 * (40 * math.log(0.4) + 60 * math.log(0.6))
    for with_summary in (True, False):
        s = build_icar_stats(
            _posteriors(with_summary=with_summary),
            ["Intercept", "scale(rivers)", "cell"],
            n_rows=100,
            n_events=40,
            sample_design=None,
        )
        assert s.null_deviance == pytest.approx(expected)


def test_build_icar_stats_with_summary():
    """The worker's posterior summary supplies mean/SD/CI per coefficient."""
    s = build_icar_stats(
        _posteriors(),
        ["Intercept", "scale(rivers)", "cell"],
        n_rows=100,
        n_events=40,
        sample_design="random (r1)",
    )
    assert [c.name for c in s.coefficients] == ["Intercept", "scale(rivers)"]
    assert s.coefficients[1].ci_low == 0.33
    assert s.vrho.name == "Vrho" and s.vrho.std == 2.0
    # rho stats ignore non-finite entries
    assert s.rho_min == -0.5 and s.rho_max == 1.5
    assert s.rho_mean == pytest.approx(0.5)


def test_build_icar_stats_falls_back_to_point_estimates():
    """Without a summary, point estimates are zipped against the design names."""
    s = build_icar_stats(
        _posteriors(with_summary=False),
        ["Intercept", "scale(rivers)", "cell"],
        n_rows=100,
        n_events=40,
        sample_design=None,
    )
    assert [c.name for c in s.coefficients] == ["Intercept", "scale(rivers)"]
    assert s.coefficients[0].estimate == -3.11
    assert s.coefficients[0].std is None  # posterior gone: no intervals
    assert s.vrho.estimate == 31.78 and s.vrho.std is None


def test_build_icar_stats_refuses_mislabelled_fallback():
    """Fallback zip must obey the same 'cell' guard as the summary path."""
    with pytest.raises(ValueError, match="cell"):
        build_icar_stats(
            _posteriors(with_summary=False),
            ["Intercept", "scale(rivers)"],
            n_rows=100,
            n_events=40,
            sample_design=None,
        )

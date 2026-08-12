"""Pure view-model + chart options for the Statistics tab (Spec A §4)."""

import math

import pandas as pd
import pytest

from gui.scripts.model_stats_charts import dist_curve_option, importance_bars_option
from gui.scripts.model_stats_view import (
    coefficient_rows,
    glm_convergence_line,
    importance_entries,
    load_tab_dist,
    stat_cards,
)
from spatialrisk.mlmodels import GLMModel, MWModel
from spatialrisk.mlmodels.stats import (
    Coefficient,
    GLMStats,
    ICARStats,
    Importance,
    MWStats,
    RFStats,
)


def _glm(deviance=100.0):
    return GLMModel(
        name="m",
        deviance=deviance,
        n_samples=19997,
        trained_at="2026-08-04T13:40:05",
        stats=GLMStats(
            n_rows=19997,
            n_events=10000,
            sample_design="random (random_1)",
            coefficients=[Coefficient(name="scale(towns_dist)", estimate=-0.4746)],
            intercept_design=-0.0565,
            intercept_fitted=-0.0572,
            n_iter=22,
            max_iter=1000,
        ),
    )


def test_stat_cards_omit_none_and_flag_non_finite_deviance():
    """None-valued cards are omitted; a non-finite deviance is flagged."""
    cards = stat_cards(_glm(deviance=float("nan")))
    keys = [c["key"] for c in cards]
    assert "card_rows" in keys and "card_events" in keys
    dev = next(c for c in cards if c["key"] == "card_deviance")
    assert dev["value"] == "—" and dev.get("warn") is True
    # MW: no rows/events/deviance cards at all — only threshold figures
    mw_cards = stat_cards(
        MWModel(
            name="w",
            stats=MWStats(dist_thresh=270.0, perc_thresh=99.5, tot_defor_ha=316892.88),
        )
    )
    mw_keys = [c["key"] for c in mw_cards]
    assert "card_rows" not in mw_keys and "card_dist_thresh" in mw_keys


def test_stat_cards_omit_family_irrelevant_cards():
    """A GLM model never renders the MW-only threshold/class cards."""
    keys = [c["key"] for c in stat_cards(_glm())]
    assert "card_dist_thresh" not in keys
    assert "card_perc_thresh" not in keys
    assert "card_tot_defor" not in keys
    assert "card_n_classes" not in keys


def test_stat_cards_accept_recovered_stats_the_model_does_not_carry():
    """An explicit ``stats`` argument overrides ``model.stats``.

    Recovered stats (spec §3) are never written back onto the model, so the
    cards can only reflect them if they can be passed in alongside the model
    that still owns deviance/trained_at.
    """
    bare = GLMModel(name="m", deviance=100.0, trained_at="2026-08-04T13:40:05")
    assert [c["key"] for c in stat_cards(bare)] == ["card_deviance", "card_trained_at"]

    recovered = GLMStats(n_rows=42, n_events=7)
    cards = {c["key"]: c["value"] for c in stat_cards(bare, recovered)}
    assert cards["card_rows"] == "42" and cards["card_events"] == "7"
    # the model's own fields still come from the model
    assert cards["card_trained_at"] == "2026-08-04T13:40:05"
    assert bare.stats is None  # the view-model writes nothing back


def test_coefficient_rows_compute_odds_ratio_at_display_time():
    """odds_ratio = exp(estimate), computed at display time, not stored."""
    rows = coefficient_rows(_glm().stats)
    r = rows[0]
    assert r["name"] == "scale(towns_dist)"
    assert float(r["odds_ratio"]) == pytest.approx(math.exp(-0.4746), rel=1e-3)
    assert r["std"] == "—"  # GLM has no posterior SD
    # a small coefficient keeps its significant figures, comma-format or not
    assert r["estimate"] == "-0.4746"


def test_glm_convergence_line():
    """Formats as 'n_iter / max_iter', or None when either is unknown."""
    assert glm_convergence_line(_glm().stats) == "22 / 1000"
    assert glm_convergence_line(GLMStats()) is None


def test_importance_entries_capped_and_ordered():
    """Entries stay in stored descending order and are capped at ``top``."""
    s = RFStats(
        importances=[Importance(name=f"v{i}", value=1.0 - i / 100) for i in range(20)]
    )
    entries = importance_entries(s, top=15)
    assert len(entries) == 15
    assert entries[0] == ("v0", 1.0)


def test_load_tab_dist_none_when_missing(tmp_path):
    """A missing tab_dist.csv yields None; a present one yields its rows."""
    assert load_tab_dist(MWStats(tab_dist_path=tmp_path / "gone.csv")) is None
    pd.DataFrame(
        {
            "distance": [30, 60],
            "npix": [1, 1],
            "area": [0.1, 0.1],
            "cum": [0.1, 0.2],
            "perc": [50.0, 100.0],
        }
    ).to_csv(tmp_path / "tab_dist.csv", index=False)
    rows = load_tab_dist(MWStats(tab_dist_path=tmp_path / "tab_dist.csv"))
    assert rows[0]["distance"] == 30 and rows[1]["perc"] == 100.0


def test_importance_bars_option_structure():
    """Bars render top-down in descending order; empty entries yield None."""
    opt = importance_bars_option([("towns_dist", 0.28), ("rivers", 0.19)])
    assert opt["series"][0]["type"] == "bar"
    assert opt["yAxis"]["type"] == "category"
    # top importance renders at the TOP: category axis is reversed input order
    assert opt["yAxis"]["data"] == ["rivers", "towns_dist"]
    assert opt["series"][0]["data"] == [0.19, 0.28]
    assert importance_bars_option([]) is None


def test_dist_curve_option_marks_the_threshold():
    """The line chart carries a markLine at the fitted distance threshold."""
    rows = [{"distance": 30, "perc": 58.8}, {"distance": 2010, "perc": 99.5}]
    opt = dist_curve_option(rows, dist_thresh=2010.0, perc_thresh=99.5)
    assert opt["series"][0]["type"] == "line"
    mark = opt["series"][0]["markLine"]["data"][0]
    assert mark["xAxis"] == 2010.0
    assert dist_curve_option([], 1.0, 1.0) is None


def test_stat_cards_format_large_magnitudes_without_scientific_notation():
    """Large hectare/deviance magnitudes render comma-grouped, never scientific.

    ``f"{316892.88:.4g}"`` alone gives ``"3.169e+05"`` — unreadable.
    """
    mw_cards = stat_cards(
        MWModel(name="w", stats=MWStats(tot_defor_ha=316892.88)),
    )
    tot = next(c for c in mw_cards if c["key"] == "card_tot_defor")
    assert tot["value"] == "316,893"

    dev = next(
        c for c in stat_cards(_glm(deviance=267845.311)) if c["key"] == "card_deviance"
    )
    assert dev["value"] == "267,845"


def test_coefficient_rows_odds_ratio_overflow_degrades_to_dash():
    """An extreme-but-finite estimate never crashes the coefficients table.

    Quasi/perfect separation is exactly where this happens on rare-event
    data; math.exp overflow degrades to the dash instead of raising.
    """
    stats = GLMStats(
        coefficients=[Coefficient(name="near_perfect_split", estimate=750.0)]
    )
    rows = coefficient_rows(stats)
    assert rows[0]["odds_ratio"] == "—"
    assert rows[0]["estimate_raw"] == 750.0
    assert rows[0]["estimate"] == "750"


def test_coefficient_rows_populated_ci_and_std_raw_and_formatted():
    """The iCAR case: populated std/CI render as strings AND raw floats.

    A11's effect bar does arithmetic directly on the ``_raw`` fields.
    """
    stats = ICARStats(
        coefficients=[
            Coefficient(
                name="scale(towns_dist)",
                estimate=-0.4746,
                std=0.0512,
                ci_low=-0.5749,
                ci_high=-0.3743,
            )
        ]
    )
    r = coefficient_rows(stats)[0]
    assert r["std"] == "0.0512"
    assert r["ci_low"] == "-0.5749"
    assert r["ci_high"] == "-0.3743"
    assert r["estimate_raw"] == pytest.approx(-0.4746)
    assert r["ci_low_raw"] == pytest.approx(-0.5749)
    assert r["ci_high_raw"] == pytest.approx(-0.3743)

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


def test_coefficient_rows_compute_odds_ratio_at_display_time():
    """odds_ratio = exp(estimate), computed at display time, not stored."""
    rows = coefficient_rows(_glm().stats)
    r = rows[0]
    assert r["name"] == "scale(towns_dist)"
    assert float(r["odds_ratio"]) == pytest.approx(math.exp(-0.4746), rel=1e-3)
    assert r["std"] == "—"  # GLM has no posterior SD


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

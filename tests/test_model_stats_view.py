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


def test_importance_entries_prettify_categorical_wrapper():
    """`C(subj, levels=[...])` displays as plain `subj`; others untouched."""
    s = RFStats(
        importances=[
            Importance(name="scale(altitude)", value=0.4),
            Importance(name="C(subj, levels=[1, 2, 3])", value=0.3),
            Importance(name="C(protected_area, levels=[0, 1])", value=0.1),
        ]
    )
    assert importance_entries(s) == [
        ("scale(altitude)", 0.4),
        ("subj", 0.3),
        ("protected_area", 0.1),
    ]


def test_importance_entries_aggregates_legacy_per_level_rows():
    """Stats saved before term aggregation carry one row per dummy column.

    Those `[T.k]` rows must collapse into one summed row at display time,
    re-sorted descending, so already-trained models don't need a retrain.
    """
    s = RFStats(
        importances=[
            Importance(name="scale(altitude)", value=0.26),
            Importance(name="C(subj, levels=[1, 2, 3])[T.2]", value=0.15),
            Importance(name="C(subj, levels=[1, 2, 3])[T.3]", value=0.14),
        ]
    )
    assert importance_entries(s) == [
        ("subj", pytest.approx(0.29)),
        ("scale(altitude)", 0.26),
    ]


def test_importance_entries_disaggregated_keeps_levels():
    """aggregate=False keeps one row per level, named 'variable = level'.

    This is the drill-down view: it shows WHICH category carries the
    importance instead of the variable's summed total.
    """
    s = RFStats(
        importances=[
            Importance(name="scale(altitude)", value=0.4),
            Importance(name="C(subj, levels=[1, 2, 3])[T.3]", value=0.2),
            Importance(name="C(subj, levels=[1, 2, 3])[T.2]", value=0.1),
        ]
    )
    assert importance_entries(s, aggregate=False) == [
        ("scale(altitude)", 0.4),
        ("subj = 3", 0.2),
        ("subj = 2", 0.1),
    ]


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


def test_importance_bars_take_the_app_accent():
    """One series, so it is the app's "primary" itself — not a colour of its own."""
    from gui.scripts.echarts_options import accent_color

    opt = importance_bars_option([("towns_dist", 0.28)], accent="#5BB624")
    assert opt["series"][0]["itemStyle"]["color"] == accent_color("#5BB624")


def test_dist_curve_paints_every_mark_in_the_accent():
    """Line, fill and threshold marker.

    Any of them left unset would fall back to ECharts' own first palette colour
    — a blue belonging to no theme, which is what this replaced.
    """
    from gui.scripts.echarts_options import accent_color

    rows = [{"distance": 30, "perc": 58.8}, {"distance": 2010, "perc": 99.5}]
    accent = "#5BB624"
    series = dist_curve_option(rows, 2010.0, 99.5, accent=accent)["series"][0]
    expected = accent_color(accent)
    assert series["lineStyle"]["color"] == expected
    assert series["areaStyle"]["color"] == expected
    assert series["markLine"]["lineStyle"]["color"] == expected


def test_model_stats_charts_follow_a_changed_accent():
    """Recolour the app's primary and the charts move with it."""
    entries = [("towns_dist", 0.28)]
    green = importance_bars_option(entries, accent="#5BB624")
    gold = importance_bars_option(entries, accent="#76591e")
    assert (
        green["series"][0]["itemStyle"]["color"]
        != gold["series"][0]["itemStyle"]["color"]
    )


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


def test_stat_cards_summarize_the_cell_level_rho_vector():
    """An iCAR model's rho summary reaches the cards as a range and an SD.

    The whole reason to run iCAR over GLM is the spatial random effect, and
    these are the numbers that say whether it did anything. They were collected
    and persisted from the first task on and displayed nowhere.
    """
    from spatialrisk.mlmodels import ICARModel

    model = ICARModel(
        name="i",
        stats=ICARStats(
            coefficients=[Coefficient(name="scale(rivers)", estimate=0.63)],
            vrho=Coefficient(name="Vrho", estimate=0.0021),
            rho_min=-0.0060,
            rho_max=0.0057,
            rho_mean=-0.0001,
            rho_std=0.0051,
        ),
    )
    cards = {c["key"]: c["value"] for c in stat_cards(model)}
    assert cards["card_rho_min"] == "-0.006"
    assert cards["card_rho_max"] == "0.0057"
    assert cards["card_rho_sd"] == "0.0051"
    assert cards["card_vrho"] == "0.0021"
    # rho_mean is ~0 by construction (the intercept absorbs the level), so it
    # is deliberately not a card of its own.
    assert "card_rho_mean" not in cards


def test_stat_cards_omit_rho_cards_when_the_vector_was_not_summarized():
    """A model without rho values renders neither rho card.

    Legacy iCAR models and every non-iCAR family go down this path; a card
    reading "—" would claim a spatial effect was measured and found empty.
    """
    from spatialrisk.mlmodels import ICARModel

    rho_keys = {"card_rho_min", "card_rho_max", "card_rho_sd"}
    keys = {c["key"] for c in stat_cards(ICARModel(name="i", stats=ICARStats()))}
    assert not (keys & rho_keys), keys
    # a single-cell rho gives min/max but no SD (ddof=1 needs two values)
    single = ICARStats(rho_min=0.5, rho_max=0.5, rho_mean=0.5)
    keys = {c["key"] for c in stat_cards(ICARModel(name="i", stats=single))}
    assert keys & rho_keys == {"card_rho_min", "card_rho_max"}, keys
    # and the GLM family never grows them
    assert not ({c["key"] for c in stat_cards(_glm())} & rho_keys)


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

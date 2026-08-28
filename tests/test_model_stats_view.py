"""Pure view-model + chart options for the Statistics tab (Spec A §4)."""

import math

import pandas as pd
import pytest

from gui.scripts.model_stats_charts import dist_curve_option, importance_bars_option
from gui.scripts.model_stats_view import (
    categorical_references,
    coefficient_rows,
    glm_convergence_line,
    icar_convergence_summary,
    importance_entries,
    load_tab_dist,
    stat_cards,
)
from spatialrisk.mlmodels import GLMModel, ICARModel, MWModel
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
    assert "card_samples" in keys and "card_events" in keys
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
    assert "card_samples" not in mw_keys and "card_dist_thresh" in mw_keys


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
    assert cards["card_samples"] == "42" and cards["card_events"] == "7"
    # the model's own fields still come from the model
    assert cards["card_trained_at"] == "2026-08-04 13:40"
    assert bare.stats is None  # the view-model writes nothing back


def test_trained_at_card_drops_seconds_and_microseconds():
    """The fit-time ISO stamp renders as a short date + time, not raw ISO."""
    m = GLMModel(name="m", trained_at="2026-07-31T17:09:19.272928")
    cards = {c["key"]: c["value"] for c in stat_cards(m)}
    assert cards["card_trained_at"] == "2026-07-31 17:09"


def test_trained_at_card_passes_through_a_non_iso_stamp():
    """A stamp the formatter cannot parse is shown as stored, never dropped."""
    m = GLMModel(name="m", trained_at="last tuesday")
    cards = {c["key"]: c["value"] for c in stat_cards(m)}
    assert cards["card_trained_at"] == "last tuesday"


def test_coefficient_rows_compute_odds_ratio_at_display_time():
    """odds_ratio = exp(estimate), computed at display time, not stored."""
    rows = coefficient_rows(_glm().stats)
    r = rows[0]
    assert r["name"] == "scale(towns_dist)"
    assert float(r["odds_ratio"]) == pytest.approx(math.exp(-0.4746), rel=1e-3)
    assert r["std"] == "—"  # GLM has no posterior SD
    # a small coefficient keeps its significant figures, comma-format or not
    assert r["estimate"] == "-0.4746"


def _categorical_glm_stats():
    """A design with one continuous term and one three-level categorical.

    Level 3 carries the largest |estimate| but is stored last and is NOT the
    largest signed value, so a test that picks it has picked it on magnitude.
    """
    return GLMStats(
        coefficients=[
            Coefficient(name="scale(altitude)", estimate=0.31, ci_low=0.2, ci_high=0.4),
            Coefficient(
                name="C(subj, levels=[1, 2, 3])[T.2]",
                estimate=0.62,
                ci_low=0.4,
                ci_high=0.8,
            ),
            Coefficient(
                name="C(subj, levels=[1, 2, 3])[T.3]",
                estimate=-1.45,
                ci_low=-1.8,
                ci_high=-1.1,
            ),
        ]
    )


def test_coefficient_rows_collapse_a_categorical_to_its_strongest_contrast():
    """One row per variable, the categorical represented by its biggest |beta|.

    The surviving row names the level, because "subj" alone would claim to
    describe the whole variable when it is one contrast out of several.
    """
    rows = coefficient_rows(_categorical_glm_stats())
    assert [r["name"] for r in rows] == ["scale(altitude)", "subj (= 3)"]
    assert rows[1]["estimate_raw"] == -1.45


def test_the_collapsed_row_keeps_its_own_level_statistics():
    """It is a real row, not a synthesized one: SD and interval come with it.

    This is the whole reason the aggregation picks instead of summing — the
    numbers on screen still belong to something the model estimated.
    """
    rows = coefficient_rows(_categorical_glm_stats())
    assert (rows[1]["ci_low_raw"], rows[1]["ci_high_raw"]) == (-1.8, -1.1)
    assert float(rows[1]["odds_ratio"]) == pytest.approx(math.exp(-1.45), rel=1e-3)


def test_coefficient_rows_disaggregated_keep_every_level():
    """aggregate=False is the drill-down: one row per level, 'variable = level'."""
    rows = coefficient_rows(_categorical_glm_stats(), aggregate=False)
    assert [r["name"] for r in rows] == [
        "scale(altitude)",
        "subj = 2",
        "subj = 3",
    ]


def test_coefficient_rows_keep_first_appearance_order_in_both_views():
    """The two views are one table at two depths, not two different tables."""
    stats = _categorical_glm_stats()
    agg = [r["variable"] for r in coefficient_rows(stats)]
    per_level = [r["variable"] for r in coefficient_rows(stats, aggregate=False)]
    assert agg == ["scale(altitude)", "subj"]
    assert per_level[0] == "scale(altitude)"


def test_a_single_column_variable_is_never_relabelled():
    """Nothing was collapsed, so there is no contrast to announce."""
    rows = coefficient_rows(
        GLMStats(
            coefficients=[
                Coefficient(name="scale(altitude)", estimate=0.31),
                Coefficient(name="C(subj, levels=[1, 2])[T.2]", estimate=0.62),
            ]
        )
    )
    assert [r["name"] for r in rows] == ["scale(altitude)", "subj = 2"]


def test_a_variable_with_no_estimate_survives_aggregation():
    """A None estimate loses to any real number but must not vanish."""
    rows = coefficient_rows(
        GLMStats(
            coefficients=[
                Coefficient(name="C(subj, levels=[1, 2, 3])[T.2]", estimate=None),
                Coefficient(name="C(subj, levels=[1, 2, 3])[T.3]", estimate=None),
            ]
        )
    )
    assert len(rows) == 1
    assert rows[0]["estimate"] == "—"


def test_coefficient_aggregation_never_sums_the_levels():
    """The regression this design exists to prevent.

    0.62 + -1.45 = -0.83 is not a quantity the model estimates, and exp() of it
    is not an odds ratio — no row may carry either number.
    """
    for aggregate in (True, False):
        estimates = [
            r["estimate_raw"]
            for r in coefficient_rows(_categorical_glm_stats(), aggregate=aggregate)
        ]
        assert all(e in (0.31, 0.62, -1.45) for e in estimates), estimates


def test_icar_coefficients_aggregate_the_same_way():
    """The GLM and iCAR tables share one view-model, so they share the rule."""
    stats = ICARStats(
        coefficients=[
            Coefficient(name="C(subj, levels=[1, 2, 3])[T.2]", estimate=0.5, std=0.1),
            Coefficient(name="C(subj, levels=[1, 2, 3])[T.3]", estimate=-2.0, std=0.3),
        ]
    )
    rows = coefficient_rows(stats)
    assert [r["name"] for r in rows] == ["subj (= 3)"]
    assert rows[0]["std"] == "0.3"


def test_strongest_contrast_prefers_a_sign_resolved_level():
    """A big estimate whose interval crosses zero loses to a smaller clear one.

    On rare-event data a sparse level is exactly where a huge, unstable
    estimate appears — picking on |estimate| alone would headline the noisiest
    contrast (and draw it as a grey, sign-unresolved bar). The collapsed slot
    goes to the largest estimate the model actually resolved.
    """
    stats = GLMStats(
        coefficients=[
            Coefficient(
                name="C(subj, levels=[1, 2, 3])[T.2]",
                estimate=0.5,
                ci_low=0.2,
                ci_high=0.8,
            ),
            Coefficient(
                name="C(subj, levels=[1, 2, 3])[T.3]",
                estimate=-3.0,
                ci_low=-6.5,
                ci_high=0.5,
            ),
        ]
    )
    rows = coefficient_rows(stats)
    assert [r["name"] for r in rows] == ["subj (= 2)"]
    assert rows[0]["estimate_raw"] == 0.5


def test_strongest_contrast_falls_back_to_magnitude_when_nothing_is_resolved():
    """No interval excludes zero (or none is stored): biggest |estimate| wins.

    This is the boundary of the resolution preference — a model without
    intervals (iCAR test fixtures, GLM without stored CIs) must keep the old
    behaviour rather than drop or misorder the variable.
    """
    stats = GLMStats(
        coefficients=[
            Coefficient(
                name="C(subj, levels=[1, 2, 3])[T.2]",
                estimate=0.5,
                ci_low=-0.1,
                ci_high=1.1,
            ),
            Coefficient(
                name="C(subj, levels=[1, 2, 3])[T.3]",
                estimate=-3.0,
                ci_low=-6.5,
                ci_high=0.5,
            ),
        ]
    )
    rows = coefficient_rows(stats)
    assert [r["name"] for r in rows] == ["subj (= 3)"]


def test_categorical_references_name_the_baseline_level():
    """Reference = first entry of the stored levels list (treatment coding).

    Every estimate and odds ratio in the table is a contrast against this
    level, and nothing else on screen says which level that is.
    """
    assert categorical_references(_categorical_glm_stats()) == [("subj", "1")]


def test_categorical_references_keep_order_and_skip_continuous_terms():
    """One entry per categorical, first-appearance order, continuous ignored."""
    stats = GLMStats(
        coefficients=[
            Coefficient(name="C(pa, levels=[0, 1])[T.1]", estimate=0.2),
            Coefficient(name="scale(altitude)", estimate=0.31),
            Coefficient(name="C(subj, levels=[1, 2, 3])[T.2]", estimate=0.62),
            Coefficient(name="C(subj, levels=[1, 2, 3])[T.3]", estimate=-1.45),
        ]
    )
    assert categorical_references(stats) == [("pa", "0"), ("subj", "1")]


def test_categorical_references_strip_quotes_from_string_levels():
    """String levels are stored quoted (`levels=['crop', ...]`); names are not."""
    stats = GLMStats(
        coefficients=[
            Coefficient(name="C(landuse, levels=['crop', 'forest'])[T.forest]"),
        ]
    )
    assert categorical_references(stats) == [("landuse", "crop")]


def test_categorical_references_need_a_stored_levels_list():
    """A bare ``C(x)`` term carries no domain, so no reference can be claimed."""
    stats = GLMStats(
        coefficients=[
            Coefficient(name="C(subj)[T.2]", estimate=0.62),
            Coefficient(name="scale(altitude)", estimate=0.31),
        ]
    )
    assert categorical_references(stats) == []


def test_glm_convergence_line():
    """Formats as 'n_iter / max_iter', or None when either is unknown."""
    assert glm_convergence_line(_glm().stats) == "22 / 1000"
    assert glm_convergence_line(GLMStats()) is None


def test_icar_convergence_scopes_the_verdict_to_the_coefficients():
    """Betas and Vrho are judged separately — Vrho never condemns the table.

    Vrho mixes slowly in virtually every affordable iCAR run (deforisk's
    chains were no different, just unshown), so a worst-case over ALL
    parameters would warn on every fit and train users to ignore it. The
    coefficient group carries the warning; Vrho gets its own neutral flag.
    """
    stats = ICARStats(
        coefficients=[
            Coefficient(name="a", rhat=1.005, ess=1200.0),
            Coefficient(name="b", rhat=1.03, ess=900.0),
        ],
        vrho=Coefficient(name="Vrho", rhat=1.48, ess=3.92),
    )
    assert icar_convergence_summary(stats) == {
        "coef": {"rhat": "1.03", "ess": "900", "warn": False},
        "vrho": {"rhat": "1.48", "ess": "3.92", "slow": True},
    }


def test_icar_convergence_warns_on_a_bad_coefficient():
    """R-hat above 1.1 or a two-digit ESS on a BETA flips the real warning."""
    bad_rhat = ICARStats(coefficients=[Coefficient(name="a", rhat=1.25, ess=500.0)])
    assert icar_convergence_summary(bad_rhat)["coef"]["warn"] is True
    bad_ess = ICARStats(coefficients=[Coefficient(name="a", rhat=1.0, ess=50.0)])
    assert icar_convergence_summary(bad_ess)["coef"]["warn"] is True
    # no Vrho diagnostics -> no Vrho entry, not a fabricated one
    assert icar_convergence_summary(bad_ess)["vrho"] is None


def test_icar_convergence_reports_a_healthy_vrho_without_the_slow_flag():
    """A Vrho that actually mixed keeps its numbers and stays un-flagged."""
    stats = ICARStats(
        coefficients=[Coefficient(name="a", rhat=1.02, ess=800.0)],
        vrho=Coefficient(name="Vrho", rhat=1.08, ess=300.0),
    )
    assert icar_convergence_summary(stats)["vrho"] == {
        "rhat": "1.08",
        "ess": "300",
        "slow": False,
    }


def test_icar_convergence_absent_without_diagnostics():
    """A recovered or pre-diagnostics model has no rhat/ess — no line, no lie."""
    stats = ICARStats(coefficients=[Coefficient(name="a", estimate=0.5)])
    assert icar_convergence_summary(stats) is None
    # Vrho-only diagnostics still summarise: coef group absent, Vrho present
    vrho_only = ICARStats(
        coefficients=[Coefficient(name="a", estimate=0.5)],
        vrho=Coefficient(name="Vrho", rhat=1.2, ess=40.0),
    )
    summary = icar_convergence_summary(vrho_only)
    assert summary["coef"] is None
    assert summary["vrho"]["slow"] is True


def _icar(deviance=9000.0, **stats_kw):
    return ICARModel(name="m", deviance=deviance, stats=ICARStats(**stats_kw))


def _card(cards, key):
    return next((c for c in cards if c["key"] == key), None)


def test_stat_cards_show_vrho_with_its_credible_interval():
    """Vrho is a posterior like any other; its card says so, not just a point."""
    model = _icar(
        vrho=Coefficient(name="Vrho", estimate=31.78, ci_low=28.0, ci_high=36.0)
    )
    assert _card(stat_cards(model), "card_vrho")["value"] == "31.78 (28 — 36)"
    # a point-only Vrho (recovered model) keeps the old single-value card
    bare = _icar(vrho=Coefficient(name="Vrho", estimate=31.78))
    assert _card(stat_cards(bare), "card_vrho")["value"] == "31.78"


def test_stat_cards_deviance_shows_the_posterior_spread_for_icar():
    """With a stored deviance posterior, the card carries its interval too."""
    model = _icar(
        deviance_summary=Coefficient(
            name="Deviance", estimate=9000.0, ci_low=8980.0, ci_high=9020.0
        )
    )
    card = _card(stat_cards(model), "card_deviance")
    assert card["value"] == "9,000 (8,980 — 9,020)"


def test_stat_cards_show_percent_of_null_deviance_explained():
    """The deforisk model_deviances.csv figure: 100 * (1 - deviance / null)."""
    model = _icar(deviance=600.0, null_deviance=1000.0)
    assert _card(stat_cards(model), "card_dev_explained")["value"] == "40%"
    # without the reference there is no percentage to claim
    assert _card(stat_cards(_icar(deviance=600.0)), "card_dev_explained") is None


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

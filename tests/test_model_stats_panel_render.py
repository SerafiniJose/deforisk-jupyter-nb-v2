"""Statistics panel renders through reacton, with real content assertions.

These catch the plain-ipyvuetify failure mode a substring test cannot see.
``reacton.render`` hands back a container for essentially any element, so an
``is not None`` assertion proves nothing: a component that silently renders
*nothing* — the plain ``import ipyvuetify as rv`` failure mode these tests
exist to catch — satisfies it just as well as a correct one. So every test
here walks the rendered widget tree and asserts on the strings that actually
reached it.
"""

import re
import threading
import time

import reacton
import solara

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

import spatialrisk.mlmodels.stats_recovery as stats_recovery  # noqa: E402
from gui.widget.model_form_dialog import ModelDetailsDialog  # noqa: E402
from gui.widget.model_stats_panel import ModelStatsPanel  # noqa: E402
from spatialrisk.mlmodels import (  # noqa: E402
    GLMModel,
    ICARModel,
    JNRBenchmarkModel,
    MWModel,
    RFModel,
)
from spatialrisk.mlmodels.stats import (  # noqa: E402
    Coefficient,
    GLMStats,
    ICARStats,
    Importance,
    JNRStats,
    RFStats,
)
from spatialrisk.project import Project  # noqa: E402


def _render(el):
    box, rc = reacton.render(el)
    return box


def _texts(widget, out=None):
    """Every string that reached the rendered widget tree.

    Strings are the leaves of a reacton tree (``solara.Text`` renders a
    ``v.Html`` whose only child is the text, ``solara.Info`` puts its label
    straight into the alert's children), so collecting them is what makes an
    empty render distinguishable from a populated one.
    """
    out = [] if out is None else out
    for child in getattr(widget, "children", []) or []:
        if isinstance(child, str):
            out.append(child)
        else:
            _texts(child, out)
    return out


def _wait_for_text(widget, needle, timeout=5.0):
    """Collect the tree's strings, polling until ``needle`` shows up.

    Recovery deliberately runs in a background thread, so the first render
    completes while the task is still pending — the panel shows its spinner and
    only swaps in the outcome once the thread finishes. Without this wait the
    test raced the thread and failed roughly half the time. On a panel that
    renders nothing the needle never arrives and the caller's assertion still
    fails, just ``timeout`` seconds later.
    """
    deadline = time.monotonic() + timeout
    while True:
        texts = _texts(widget)
        if needle in texts or time.monotonic() > deadline:
            return texts
        time.sleep(0.02)


def _styles(widget, out=None):
    """Every ``style_`` trait in the tree.

    The effect bar is the one piece of the family panels with no text of its
    own: its whole meaning is carried by the inline style (which side of the
    centre line it grows from, and its colour). Reading the styles is the only
    way to assert that a credible interval crossing zero really is muted.
    """
    out = [] if out is None else out
    style = getattr(widget, "style_", None)
    if isinstance(style, str) and style:
        out.append(style)
    for child in getattr(widget, "children", []) or []:
        if not isinstance(child, str):
            _styles(child, out)
    return out


def _labels(widget, out=None):
    """Every ``label`` trait in the tree (the read-only fields carry theirs)."""
    out = [] if out is None else out
    label = getattr(widget, "label", None)
    if isinstance(label, str) and label:
        out.append(label)
    for child in getattr(widget, "children", []) or []:
        if not isinstance(child, str):
            _labels(child, out)
    return out


def _glm_model():
    return GLMModel(
        name="m",
        deviance=100.0,
        trained_at="2026-08-04T13:40:05",
        stats=GLMStats(
            n_rows=10,
            n_events=5,
            coefficients=[Coefficient(name="scale(a)", estimate=0.5)],
            n_iter=22,
            max_iter=1000,
        ),
    )


def test_panel_renders_with_stats():
    """Stored stats render the caveat strip and the stat cards."""
    texts = _texts(_render(ModelStatsPanel(model=_glm_model())))
    assert t("tiles.train.stats.caveat_training_fit") in texts
    # label + value of the first card, so a card that renders only its
    # chrome fails too
    assert t("tiles.train.stats.card_rows") in texts
    assert "10" in texts
    assert t("tiles.train.stats.card_trained_at") in texts
    assert "2026-08-04T13:40:05" in texts


def test_panel_renders_empty_state_without_stats_or_paths():
    """No stats, no model_path/samples_path -> recovery returns None.

    The empty state must render (and the caveat strip stays), not raise.
    """
    model = MWModel(name="w")
    box = _render(ModelStatsPanel(model=model))
    texts = _wait_for_text(box, t("tiles.train.stats.empty_state"))
    assert t("tiles.train.stats.caveat_training_fit") in texts
    assert t("tiles.train.stats.empty_state") in texts
    # Non-negotiable: opening the dialog is read-only. Recovery has run to
    # completion by now and must not have written its outcome back.
    assert model.stats is None


def test_panel_renders_recovered_stats_in_the_cards(monkeypatch):
    """A recovered model's numbers reach the card strip, not just the body.

    ``model.stats`` stays None by the read-only contract, so the cards can only
    show anything if the panel forwards the recovered stats explicitly.
    """
    monkeypatch.setattr(
        stats_recovery,
        "recover_stats",
        lambda model: GLMStats(n_rows=42, n_events=7),
    )
    model = GLMModel(name="legacy", trained_at="2026-08-04T13:40:05")
    box = _render(ModelStatsPanel(model=model))
    texts = _wait_for_text(box, "42")
    assert t("tiles.train.stats.card_rows") in texts
    assert "42" in texts
    assert t("tiles.train.stats.card_events") in texts
    assert "7" in texts
    assert model.stats is None  # recovery stayed read-only


def test_panel_shows_the_recovering_state_while_the_task_runs(monkeypatch):
    """The pending body renders while the background recovery is in flight."""
    release = threading.Event()

    def _blocking_recovery(model):
        release.wait(5.0)
        return None

    monkeypatch.setattr(stats_recovery, "recover_stats", _blocking_recovery)
    box = _render(ModelStatsPanel(model=MWModel(name="w")))
    try:
        texts = _wait_for_text(box, t("tiles.train.stats.recovering"))
        assert t("tiles.train.stats.recovering") in texts
        assert t("tiles.train.stats.empty_state") not in texts
    finally:
        release.set()


def test_panel_hints_at_retraining_for_icar_without_stats(monkeypatch):
    """An unrecoverable iCAR model explains why its intervals are missing."""
    monkeypatch.setattr(stats_recovery, "recover_stats", lambda model: None)
    box = _render(ModelStatsPanel(model=ICARModel(name="i")))
    texts = _wait_for_text(box, t("tiles.train.stats.icar_retrain_hint"))
    assert t("tiles.train.stats.empty_state") in texts
    assert t("tiles.train.stats.icar_retrain_hint") in texts


def test_glm_panel_renders_coefficients_intercepts_and_convergence():
    """GLM body: the coefficient table, both intercepts, the solver line.

    The odds ratio is asserted as a value, not as a column header: it is
    computed at display time (exp(estimate)) rather than stored, so a table
    that rendered the header and dropped the transform would still pass a
    header-only check.
    """
    model = GLMModel(
        name="m",
        stats=GLMStats(
            coefficients=[Coefficient(name="scale(rivers)", estimate=0.5)],
            intercept_design=-1.25,
            intercept_fitted=0.75,
            n_iter=22,
            max_iter=1000,
        ),
    )
    texts = _texts(_render(ModelStatsPanel(model=model)))
    assert t("tiles.train.stats.col_predictor") in texts
    assert t("tiles.train.stats.col_odds_ratio") in texts
    assert "scale(rivers)" in texts
    assert "0.5" in texts  # estimate
    assert "1.649" in texts  # exp(0.5)
    assert t("tiles.train.stats.intercept_design", value="-1.25") in texts
    assert t("tiles.train.stats.intercept_fitted", value="0.75") in texts
    assert t("tiles.train.stats.converged_line", line="22 / 1000") in texts
    # The credible-interval columns belong to the iCAR table alone.
    assert t("tiles.train.stats.col_ci_low") not in texts


def test_rf_panel_renders_importances():
    """RF body: the importance chart really receives the named importances.

    The bars live inside an ipecharts widget, so the chart's own content is
    unreachable from the text tree — the widget's option is read directly
    instead, which is what proves the importances (not just the header) made
    it through.
    """
    import ipecharts

    model = RFModel(
        name="m",
        stats=RFStats(
            importances=[Importance(name="towns_dist", value=0.28)],
            oob_accuracy=0.81,
        ),
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        texts = _texts(box)
        assert t("tiles.train.stats.importance_header") in texts
        assert t("tiles.train.stats.exploratory_chip") in texts
        assert t("tiles.train.stats.oob_line", value="0.81") in texts
        assert t("tiles.train.stats.importance_bias_note") in texts
        charts = rc.find(ipecharts.EChartsRawWidget).widgets
        assert len(charts) == 1
        assert charts[0].option["yAxis"]["data"] == ["towns_dist"]
    finally:
        rc.close()


def test_rf_panel_toggles_between_aggregated_and_per_level_importances():
    """The split switch swaps the chart between per-variable and per-level.

    Default is aggregated (one summed bar per variable, comparable across
    variables); flipping the switch re-shapes the same stored stats into
    one bar per category level, so the user can see WHICH level carries
    the importance.
    """
    import ipecharts
    import ipyvuetify as vw

    model = RFModel(
        name="m",
        stats=RFStats(
            importances=[
                Importance(name="scale(altitude)", value=0.4),
                Importance(name="C(subj, levels=[1, 2, 3])[T.3]", value=0.2),
                Importance(name="C(subj, levels=[1, 2, 3])[T.2]", value=0.1),
            ]
        ),
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        texts = _texts(box)
        assert t("tiles.train.stats.importance_split_toggle") in _labels(box) + texts
        charts = rc.find(ipecharts.EChartsRawWidget).widgets
        # aggregated by default; the category axis is reversed input order
        assert charts[0].option["yAxis"]["data"] == ["subj", "scale(altitude)"]

        switch = rc.find(vw.Switch).widgets[0]
        switch.v_model = True
        charts = rc.find(ipecharts.EChartsRawWidget).widgets
        assert charts[0].option["yAxis"]["data"] == [
            "subj = 2",
            "subj = 3",
            "scale(altitude)",
        ]
    finally:
        rc.close()


def test_rf_panel_hides_the_split_switch_without_categorical_terms():
    """All-continuous importances render no switch — nothing to split."""
    import ipyvuetify as vw

    model = RFModel(
        name="m",
        stats=RFStats(importances=[Importance(name="towns_dist", value=0.28)]),
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        assert len(rc.find(vw.Switch).widgets) == 0
    finally:
        rc.close()


def test_icar_panel_renders_ci_table():
    """Posterior table with SD/CI columns, and the muted crossing-zero rule."""
    model = ICARModel(
        name="m",
        stats=ICARStats(
            coefficients=[
                Coefficient(
                    name="scale(rivers)",
                    estimate=0.43,
                    std=0.05,
                    ci_low=0.33,
                    ci_high=0.53,
                ),
                Coefficient(
                    name="scale(edge)",
                    estimate=-0.05,
                    std=0.05,
                    ci_low=-0.15,
                    ci_high=0.06,  # crosses zero -> muted bar
                ),
            ]
        ),
    )
    box = _render(ModelStatsPanel(model=model))
    texts = _texts(box)
    assert t("tiles.train.stats.col_std") in texts
    assert t("tiles.train.stats.col_ci_low") in texts
    assert t("tiles.train.stats.col_ci_high") in texts
    assert "scale(rivers)" in texts
    assert "0.33" in texts  # ci_low
    assert "0.53" in texts  # ci_high
    assert t("tiles.train.stats.icar_ci_note") in texts
    # Vrho is a variance parameter, not a log-odds coefficient: no odds-ratio
    # column on a posterior table.
    assert t("tiles.train.stats.col_odds_ratio") not in texts
    bars = [s for s in _styles(box) if "border-radius:1px;" in s]
    assert any("background:grey;" in s for s in bars), bars
    assert any("background:var(--v-error-base);" in s for s in bars), bars


def test_icar_panel_renders_the_cell_level_rho_summary():
    """The rho cards reach the strip, labelled as cell-level, never as raster.

    A5 summarises ``posteriors["rho"]`` — one value per native-csize spatial
    cell — not the interpolated rho GeoTIFF, so the label must not imply the
    raster.
    """
    model = ICARModel(
        name="m",
        stats=ICARStats(
            coefficients=[Coefficient(name="scale(rivers)", estimate=0.63)],
            vrho=Coefficient(name="Vrho", estimate=0.0021),
            rho_min=-0.0060,
            rho_max=0.0057,
            rho_mean=-0.0001,
            rho_std=0.0051,
        ),
    )
    texts = _texts(_render(ModelStatsPanel(model=model)))
    for key, value in (
        ("card_rho_min", "-0.006"),
        ("card_rho_max", "0.0057"),
        ("card_rho_sd", "0.0051"),
    ):
        assert t(f"tiles.train.stats.{key}") in texts, key
        assert value in texts, key
        # the label describes the cell vector, not the interpolated GeoTIFF
        assert "raster" not in t(f"tiles.train.stats.{key}").lower(), key


def test_icar_panel_omits_rho_cards_for_a_model_without_them():
    """No rho values -> no rho cards (legacy models, and every other family)."""
    model = ICARModel(
        name="m",
        stats=ICARStats(coefficients=[Coefficient(name="scale(rivers)", estimate=0.6)]),
    )
    texts = _texts(_render(ModelStatsPanel(model=model)))
    for key in ("card_rho_min", "card_rho_max", "card_rho_sd"):
        assert t(f"tiles.train.stats.{key}") not in texts, key


def _bar_widths(box):
    """Width percentages of the effect bars, in render order."""
    bars = [s for s in _styles(box) if "border-radius:1px;" in s]
    return [int(re.search(r"width:(\d+)%", s).group(1)) for s in bars]


def test_effect_bars_distinguish_coefficient_magnitudes():
    """Two coefficients of different size draw different bars.

    Regression: the bar used a FIXED full scale of 0.5, so every |estimate|
    >= 0.5 clamped to the same 46% width — beta=0.63 and beta=3.0 (both real
    magnitudes from this branch's own MCMC runs) were pixel-identical, which is
    an active claim that unequal effects are equal. The scale is now the
    table's own largest magnitude, so this asserts a strict inequality that the
    old code could not satisfy: under the fixed scale both widths were 46.
    """
    model = ICARModel(
        name="m",
        stats=ICARStats(
            coefficients=[
                Coefficient(name="big", estimate=3.0),
                Coefficient(name="small", estimate=0.63),
            ]
        ),
    )
    widths = _bar_widths(_render(ModelStatsPanel(model=model)))
    assert len(widths) == 2, widths
    assert widths[0] == 46, widths  # the table's largest fills the bar
    assert widths[1] < widths[0], widths  # and the smaller one does NOT
    assert widths[1] == round(0.63 / 3.0 * 46), widths


def test_effect_bar_keeps_a_tiny_coefficient_visible():
    """A nonzero estimate never floors to a 0%-wide (i.e. invisible) bar.

    ``width:{w:.0f}%`` renders anything under 0.5% as "0%", making a small
    coefficient indistinguishable from a missing one.
    """
    model = ICARModel(
        name="m",
        stats=ICARStats(
            coefficients=[
                Coefficient(name="huge", estimate=1000.0),
                Coefficient(name="tiny", estimate=0.001),
            ]
        ),
    )
    widths = _bar_widths(_render(ModelStatsPanel(model=model)))
    assert widths == [46, 1], widths


def test_jnr_panel_renders_without_tab_dist_on_disk(tmp_path):
    """A missing tab_dist.csv degrades to the fallback text, never an error."""
    model = JNRBenchmarkModel(
        name="j",
        stats=JNRStats(
            dist_thresh=2010.0,
            perc_thresh=99.5,
            tot_defor_ha=316892.9,
            tab_dist_path=tmp_path / "gone.csv",
            n_classes=29,
        ),
    )
    texts = _texts(_render(ModelStatsPanel(model=model)))
    assert t("tiles.train.stats.dist_curve_header") in texts
    assert t("tiles.train.stats.tab_dist_missing") in texts
    # the threshold cards still render alongside the fallback
    assert t("tiles.train.stats.card_n_classes") in texts
    assert "29" in texts


def test_details_dialog_keeps_configuration_content_under_tabs():
    """Both tab headers render and tab 1 hosts the statistics panel."""
    project = solara.reactive(Project(project_name="p"))
    project.value.models["m"] = _glm_model()
    texts = _texts(
        _render(
            ModelDetailsDialog(project=project, model_key="m", on_close=lambda: None)
        )
    )
    assert t("tiles.train.stats.tab_config") in texts
    assert t("tiles.train.stats.tab_statistics") in texts
    assert t("tiles.train.stats.caveat_training_fit") in texts


def test_details_dialog_configuration_fields_survive_the_restructure():
    """The model/dataset/name read-only fields are still rendered."""
    project = solara.reactive(Project(project_name="p"))
    project.value.models["m"] = _glm_model()
    labels = _labels(
        _render(
            ModelDetailsDialog(project=project, model_key="m", on_close=lambda: None)
        )
    )
    for key in (
        "tiles.train.model_select_label",
        "tiles.train.dataset_select_label",
        "tiles.train.model_name_label",
    ):
        assert t(key) in labels, key

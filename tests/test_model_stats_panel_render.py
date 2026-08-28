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


def _markdowns(widget, out=None):
    """Every rendered-markdown body in the tree (the info popups' content).

    ``solara.Markdown`` renders through a VuetifyTemplate whose ``template``
    trait embeds the converted HTML, so popup text never appears as a string
    child — ``_texts`` cannot see it.
    """
    out = [] if out is None else out
    template = getattr(widget, "template", None)
    if isinstance(template, str) and template:
        out.append(template)
    for child in getattr(widget, "children", []) or []:
        if not isinstance(child, str):
            _markdowns(child, out)
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
    """Stored stats render the stat cards and the caveat footnote."""
    texts = _texts(_render(ModelStatsPanel(model=_glm_model())))
    assert t("tiles.train.stats.caveat_training_fit") in texts
    # label + value of the first card, so a card that renders only its
    # chrome fails too
    assert t("tiles.train.stats.card_samples") in texts
    assert "10" in texts
    assert t("tiles.train.stats.card_trained_at") in texts
    assert "2026-08-04 13:40" in texts


def test_panel_renders_empty_state_without_stats_or_paths():
    """No stats, no model_path/samples_path -> recovery returns None.

    The empty state must render (and the caveat footnote stays), not raise.
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
    assert t("tiles.train.stats.card_samples") in texts
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
        charts = rc.find(ipecharts.EChartsRawWidget).widgets
        assert len(charts) == 1
        assert charts[0].option["yAxis"]["data"] == ["towns_dist"]
    finally:
        rc.close()


def _categorical_coefficients():
    """One continuous term plus a three-level categorical, level 3 strongest."""
    return [
        Coefficient(name="scale(altitude)", estimate=0.31),
        Coefficient(name="C(subj, levels=[1, 2, 3])[T.2]", estimate=0.62),
        Coefficient(name="C(subj, levels=[1, 2, 3])[T.3]", estimate=-1.45),
    ]


def test_glm_panel_toggles_between_the_strongest_contrast_and_every_level():
    """The switch swaps the coefficient table between one row per variable...

    ...and one row per design column. The default names the level it kept,
    because that row is one contrast out of several rather than the variable's
    whole effect. The explanatory note lives in the info popup (eager, so its
    text is in the tree in both views even while the dialog is closed).
    """
    import ipyvuetify as vw

    model = GLMModel(name="m", stats=GLMStats(coefficients=_categorical_coefficients()))
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        texts = _texts(box)
        assert t("tiles.train.stats.coef_split_toggle") in _labels(box) + texts
        assert t("tiles.train.stats.coef_info_title") in texts
        note = t("tiles.train.stats.coef_strongest_note")
        assert any(note in md for md in _markdowns(box))
        assert "subj (= 3)" in texts
        assert "subj = 2" not in texts

        rc.find(vw.Switch).widgets[0].v_model = True
        texts = _texts(box)
        assert {"subj = 2", "subj = 3"} <= set(texts)
        assert "subj (= 3)" not in texts
    finally:
        rc.close()


def test_icar_panel_offers_the_same_level_toggle():
    """The GLM and iCAR tables share the view-model, so they share the switch."""
    import ipyvuetify as vw

    model = ICARModel(
        name="m", stats=ICARStats(coefficients=_categorical_coefficients())
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        assert "subj (= 3)" in _texts(box)
        rc.find(vw.Switch).widgets[0].v_model = True
        assert "subj = 2" in _texts(box)
    finally:
        rc.close()


def test_coefficient_panels_hide_the_switch_without_categorical_levels():
    """Nothing was collapsed, so there is nothing the switch could reveal.

    The single-level categorical is the case a list comparison would get wrong:
    its row is already per-level, it just reads as ``subj = 2``.
    """
    import ipyvuetify as vw

    model = GLMModel(
        name="m",
        stats=GLMStats(
            coefficients=[
                Coefficient(name="towns_dist", estimate=0.28),
                Coefficient(name="C(subj, levels=[1, 2])[T.2]", estimate=0.62),
            ]
        ),
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        assert len(rc.find(vw.Switch).widgets) == 0
        assert "subj = 2" in _texts(box)
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


def test_effect_bar_scale_stays_pinned_across_the_level_toggle():
    """Flipping the level switch must not resize the bars that stay on screen.

    The scale comes from the per-level rows in BOTH views. It used to hold by
    coincidence — the magnitude-picked row was always the global max — but the
    resolution-preferring selection can collapse a categorical to a smaller row
    than its noisiest level, and a scale computed per-view would then inflate
    every bar in the collapsed table and shrink them on toggle.
    """
    import ipyvuetify as vw

    model = GLMModel(
        name="m",
        stats=GLMStats(
            coefficients=[
                Coefficient(
                    name="scale(altitude)", estimate=1.0, ci_low=0.5, ci_high=1.5
                ),
                Coefficient(
                    name="C(subj, levels=[1, 2, 3])[T.2]",
                    estimate=0.5,
                    ci_low=0.2,
                    ci_high=0.8,
                ),
                Coefficient(
                    name="C(subj, levels=[1, 2, 3])[T.3]",
                    estimate=4.0,
                    ci_low=-1.0,
                    ci_high=9.0,
                ),
            ]
        ),
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        # collapsed: altitude and the RESOLVED subj contrast, drawn against the
        # per-level maximum of 4.0 — not against the collapsed table's own 1.0
        assert _bar_widths(box) == [12, 6], _bar_widths(box)
        rc.find(vw.Switch).widgets[0].v_model = True
        assert _bar_widths(box) == [12, 6, 46], _bar_widths(box)
    finally:
        rc.close()


def test_coefficient_tables_name_the_reference_level():
    """Every categorical estimate is a contrast against a level no row names.

    The reference note lives in the info popup, which renders in BOTH views
    (the per-level rows need it just as much) and even without the switch — a
    two-level categorical collapses to a single row whose odds ratio is
    unreadable without its baseline.
    """
    import ipyvuetify as vw

    note = t("tiles.train.stats.coef_reference_note", refs="subj = 1")
    model = GLMModel(name="m", stats=GLMStats(coefficients=_categorical_coefficients()))
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        assert any(note in md for md in _markdowns(box))
        rc.find(vw.Switch).widgets[0].v_model = True
        assert any(note in md for md in _markdowns(box))
    finally:
        rc.close()

    two_level = GLMModel(
        name="m",
        stats=GLMStats(
            coefficients=[Coefficient(name="C(subj, levels=[1, 2])[T.2]", estimate=0.6)]
        ),
    )
    two_level_box = _render(ModelStatsPanel(model=two_level))
    assert any(note in md for md in _markdowns(two_level_box))

    continuous_only = ICARModel(
        name="m",
        stats=ICARStats(coefficients=[Coefficient(name="scale(a)", estimate=0.5)]),
    )
    texts = _texts(_render(ModelStatsPanel(model=continuous_only)))
    assert not [s for s in texts if "subj" in s or " = " in s], texts
    # nothing to explain -> no info button, no popup
    assert t("tiles.train.stats.coef_info_title") not in texts


def test_coefficient_info_popup_omits_the_contrast_note_without_a_collapse():
    """A two-level categorical keeps the popup but drops the contrast note.

    Its single row is already per-level (no switch, nothing collapsed), so the
    strongest-contrast explanation would describe a view that does not exist —
    but the reference level still needs naming.
    """
    import ipyvuetify as vw

    model = GLMModel(
        name="m",
        stats=GLMStats(
            coefficients=[Coefficient(name="C(subj, levels=[1, 2])[T.2]", estimate=0.6)]
        ),
    )
    box, rc = reacton.render(ModelStatsPanel(model=model))
    try:
        assert t("tiles.train.stats.coef_info_title") in _texts(box)
        mds = _markdowns(box)
        ref = t("tiles.train.stats.coef_reference_note", refs="subj = 1")
        assert any(ref in md for md in mds)
        strongest = t("tiles.train.stats.coef_strongest_note")
        assert not any(strongest in md for md in mds)
        assert len(rc.find(vw.Switch).widgets) == 0
    finally:
        rc.close()


def _diagnosed_icar(rhat=1.08, ess=300.0):
    return ICARModel(
        name="m",
        deviance=600.0,
        stats=ICARStats(
            coefficients=[
                Coefficient(name="scale(a)", estimate=0.5, rhat=1.02, ess=800.0)
            ],
            vrho=Coefficient(
                name="Vrho",
                estimate=31.78,
                ci_low=28.0,
                ci_high=36.0,
                rhat=rhat,
                ess=ess,
            ),
            null_deviance=1000.0,
        ),
    )


def test_icar_panel_reports_mcmc_mixing_and_the_deforisk_cards():
    """The deforisk parity block: per-group mixing lines, Vrho interval, %.

    The mixing lines are the numeric stand-in for deforisk's mcmc.pdf traces;
    the Vrho interval and the %-of-null-deviance card are its summary table
    and model_deviances.csv figures.
    """
    texts = _texts(_render(ModelStatsPanel(model=_diagnosed_icar())))
    coef = t("tiles.train.stats.icar_conv_coef_line", rhat="1.02", ess="800")
    vrho = t("tiles.train.stats.icar_conv_vrho_line", rhat="1.08", ess="300")
    assert coef in texts
    assert vrho in texts
    assert t("tiles.train.stats.icar_conv_coef_warn") not in texts
    assert t("tiles.train.stats.card_dev_explained") in texts
    assert "40%" in texts
    assert "31.78 (28 — 36)" in texts


def test_icar_panel_keeps_calm_when_only_vrho_mixed_slowly():
    """A slow Vrho gets its own neutral note — never the coefficient alarm.

    This is the typical run: Vrho's slow mixing is a property of the sampler
    at affordable iteration counts, not evidence against the effects table
    the user is reading.
    """
    texts = _texts(_render(ModelStatsPanel(model=_diagnosed_icar(rhat=1.4, ess=3.92))))
    slow = t("tiles.train.stats.icar_conv_vrho_slow", rhat="1.4", ess="3.92")
    assert slow in texts
    assert t("tiles.train.stats.icar_conv_coef_warn") not in texts


def test_icar_panel_warns_when_a_coefficient_did_not_mix():
    """A bad beta R-hat raises the explicit coefficient warning."""
    model = _diagnosed_icar()
    model.stats.coefficients[0].rhat = 1.3
    texts = _texts(_render(ModelStatsPanel(model=model)))
    assert t("tiles.train.stats.icar_conv_coef_warn") in texts


def test_icar_mcmc_info_button_explains_the_diagnostics():
    """The diagnostics block carries an info popup interpreting R-hat/ESS.

    The numbers are meaningless to a non-statistician; the popup is where
    'R-hat 1.48' becomes 'the walk had not settled'.
    """
    box, rc = reacton.render(ModelStatsPanel(model=_diagnosed_icar()))
    try:
        assert t("tiles.train.stats.icar_mcmc_info_title") in _texts(box)
        mds = _markdowns(box)
        assert any("R-hat" in md for md in mds)
    finally:
        rc.close()


def _widget_tree_has(widget, cls):
    stack = [widget]
    while stack:
        w = stack.pop()
        if isinstance(w, cls):
            return True
        stack.extend(
            c for c in (getattr(w, "children", None) or []) if not isinstance(c, str)
        )
    return False


def test_icar_mcmc_info_button_sits_in_the_diagnostics_header():
    """The info button rides next to the 'MCMC diagnostics' title itself.

    In the header — not the body — so the explanation is reachable before
    the panel is ever expanded. (Its click must not toggle the collapsible;
    the .stop wrapper is what the widget asserts on here, the toggle
    behaviour itself is browser-only.)
    """
    import ipyvuetify as vw

    box, rc = reacton.render(ModelStatsPanel(model=_diagnosed_icar()))
    try:
        headers = rc.find(vw.ExpansionPanelHeader).widgets
        assert len(headers) == 1
        assert _widget_tree_has(headers[0], vw.Btn)
    finally:
        rc.close()


def test_icar_mcmc_diagnostics_render_in_a_collapsible():
    """The mixing block sits under its own MCMC header, collapsed by default.

    Healthy chains are background information — the panel starts closed
    (v_model None) and the header is what tells the reader it is there.
    """
    import ipyvuetify as vw

    box, rc = reacton.render(ModelStatsPanel(model=_diagnosed_icar()))
    try:
        texts = _texts(box)
        assert t("tiles.train.stats.icar_mcmc_header") in texts
        line = t("tiles.train.stats.icar_conv_coef_line", rhat="1.02", ess="800")
        assert line in texts
        panels = rc.find(vw.ExpansionPanels).widgets
        assert len(panels) == 1
        assert panels[0].v_model is None
    finally:
        rc.close()


def test_icar_mcmc_collapsible_opens_itself_on_a_warning():
    """Unconverged chains must not hide behind a closed panel.

    The warning invalidates the whole table above it, so the collapsible
    starts expanded (v_model 0) when a COEFFICIENT trips the threshold. A
    slow Vrho alone is the normal state of this sampler and stays closed.
    """
    import ipyvuetify as vw

    bad_beta = _diagnosed_icar()
    bad_beta.stats.coefficients[0].rhat = 1.3
    box, rc = reacton.render(ModelStatsPanel(model=bad_beta))
    try:
        assert t("tiles.train.stats.icar_conv_coef_warn") in _texts(box)
        panels = rc.find(vw.ExpansionPanels).widgets
        assert panels[0].v_model == 0
    finally:
        rc.close()

    slow_vrho = _diagnosed_icar(rhat=1.4, ess=3.92)
    box, rc = reacton.render(ModelStatsPanel(model=slow_vrho))
    try:
        panels = rc.find(vw.ExpansionPanels).widgets
        assert panels[0].v_model is None
    finally:
        rc.close()


def test_icar_panel_stays_silent_without_diagnostics():
    """A recovered / pre-diagnostics model must not claim convergence."""
    model = ICARModel(
        name="m",
        deviance=600.0,
        stats=ICARStats(coefficients=[Coefficient(name="scale(a)", estimate=0.5)]),
    )
    texts = _texts(_render(ModelStatsPanel(model=model)))
    assert not [s for s in texts if "R-hat" in s], texts
    # no diagnostics -> no MCMC section at all, not an empty collapsible
    assert t("tiles.train.stats.icar_mcmc_header") not in texts


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


def _find_widget(widget, cls, out=None):
    """Every widget of ``cls`` in the rendered tree."""
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if not isinstance(child, str):
            _find_widget(child, cls, out)
    return out


def test_details_dialog_tab_content_clears_the_tab_strip():
    """The tab-items container carries top padding.

    ``v-tabs-items`` starts flush against the tab strip, while an outlined
    field's floating label and fieldset legend sit ~6px *above* the box it
    labels. Without padding the first read-only field's label ("Model") is
    drawn over the tab slider — measured in a browser at label top 335 vs.
    container top 341.
    """
    import ipyvuetify as v

    project = solara.reactive(Project(project_name="p"))
    project.value.models["m"] = _glm_model()
    containers = _find_widget(
        _render(
            ModelDetailsDialog(project=project, model_key="m", on_close=lambda: None)
        ),
        v.TabsItems,
    )
    assert containers, "no v-tabs-items in the details dialog"
    assert re.search(r"\bpt-[1-9]\b", containers[0].class_), containers[0].class_

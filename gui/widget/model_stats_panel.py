"""Statistics tab content for the model details dialog (Spec A §4).

Every number here is in-sample — the caveat strip states that first. Stats
come from model.stats when present; otherwise a background recovery attempt
runs via solara.use_task (an RF pickle is tens of MB and rebuilding design
info re-reads the training CSV — never on the render thread, see
solara-event-handlers-block-websocket-loop). Recovery is read-only: this
panel NEVER assigns model.stats or saves the project.

Card labels are resolved by catalog convention under ``tiles.train.stats``.
The lookup is built with an f-string, exactly like the per-parameter hints in
model_form_dialog: tests/test_i18n.py scans for literal translator calls with a
regex, and a literal prefix concatenated with a variable is picked up as if the
prefix itself were a key.
"""

import reacton.ipyvuetify as rv
import solara
import solara.lab
from pysepal.solara import use_theme_dark

from gui.i18n import t
from gui.scripts.model_stats_charts import dist_curve_option, importance_bars_option
from gui.scripts.model_stats_view import (
    DASH,
    coefficient_rows,
    glm_convergence_line,
    importance_entries,
    load_tab_dist,
    stat_cards,
)

# Number formatting is the view-model's job ("the widget never formats
# numbers"): reimplementing it here would resurrect the scientific-notation
# rendering A9 removed. The intercept lines and the OOB number are the two
# values that reach the widget raw, so they borrow the same formatter.
from gui.scripts.model_stats_view import _fmt as fmt_stat
from gui.widget.echarts import RENDERER_SVG, EChartsChart
from gui.widget.product_table import ProductTable
from gui.widget.text_style import MUTED

_CARD_STYLE = (
    "border:1px solid rgba(128,128,128,0.25);border-radius:4px;"
    "padding:8px 12px;min-width:110px;"
)

# Half the cell is available on each side of the centre line; 46% leaves the
# bar a little air at full scale.
_BAR_MAX_PCT = 46
# Only used when a table has no usable estimate to scale against (every row
# None or 0.0). The real scale is per-table — see _bar_scale.
_BAR_FALLBACK_SCALE = 0.5
# Sub-1% bars would floor to "0%" and read as absent rather than small.
_BAR_MIN_PCT = 1.0


@solara.component
def _StatCards(model, stats):
    """Header cards — one per non-None statistic (the view-model omits the rest).

    ``stats`` is passed explicitly rather than read off the model: on the
    recovery path it comes from the background task and is never written back.
    """
    cards = stat_cards(model, stats)
    with solara.Row(gap="8px", style="flex-wrap:wrap;"):
        for card in cards:
            with solara.Column(style=_CARD_STYLE, gap="0px"):
                solara.Text(
                    t(f"tiles.train.stats.{card['key']}"),
                    style=MUTED + "font-size:0.68rem;text-transform:uppercase;"
                    "letter-spacing:0.06em;",
                )
                solara.Text(
                    str(card["value"]),
                    style=(
                        "font-size:1.0rem;font-weight:600;"
                        + ("color:var(--v-error-base);" if card.get("warn") else "")
                    ),
                )


@solara.component
def ModelStatsPanel(model, visible=True):
    """Caveat + stat cards + family panel with pending/empty states.

    Args:
        model: the registered model to describe.
        visible: whether this panel's tab is the one on screen, forwarded to
            the family charts. ipecharts measures its container once, at DOM
            attach, and a view attached while its tab is hidden or still
            transitioning measures width 0 and never recovers (see
            gui/widget/echarts.py) — this flag is what makes the adapter
            schedule its post-transition resize. The caller owns the tab
            index, because the caller owns the tab order; the default suits a
            bare mount (e.g. a test), which counts as shown.
    """
    # Hooks run unconditionally — the task itself decides whether to work.
    # use_task lives in solara.lab; .pending/.finished/.value per the
    # solara-task-pending-and-error-gotchas memory (.error is a bool, the
    # exception is on .exception).
    # raise_error=False keeps stats_recovery's own contract intact end to end:
    # it promises the caller an empty state rather than an error, and the
    # default (True) would re-raise inside the render instead, taking the whole
    # details dialog down. An errored task is neither pending nor finished, so
    # it falls through to the empty state below.
    recovered = solara.lab.use_task(
        lambda: _recover(model),
        dependencies=[
            getattr(model, "name", None),
            getattr(model, "trained_at", None),
            getattr(model, "stats", None) is not None,
        ],
        raise_error=False,
    )
    stats = getattr(model, "stats", None)
    if stats is None and recovered.finished and recovered.value is not None:
        stats = recovered.value

    with solara.Column(gap="12px"):
        with solara.Row(gap="8px", style="align-items:flex-start;"):
            solara.Info(t("tiles.train.stats.caveat_training_fit"), dense=True)
        _StatCards(model=model, stats=stats)
        if stats is not None:
            _FamilyPanel(model=model, stats=stats, visible=visible)
        elif recovered.pending:
            with solara.Row(gap="8px"):
                rv.ProgressCircular(indeterminate=True, size=18, width=2)
                solara.Text(t("tiles.train.stats.recovering"), style=MUTED)
        else:
            solara.Info(t("tiles.train.stats.empty_state"))
            if getattr(model, "model_type", "") == "icar":
                solara.Text(t("tiles.train.stats.icar_retrain_hint"), style=MUTED)


def _recover(model):
    """Best-effort disk recovery, off the render thread. Never writes."""
    if getattr(model, "stats", None) is not None:
        return None  # nothing to do — fit-time stats win
    from spatialrisk.mlmodels.stats_recovery import recover_stats

    return recover_stats(model)


@solara.component
def _FamilyPanel(model, stats, visible=True):
    """Family-specific body, dispatched on ``model.model_type``.

    An unknown family renders nothing rather than raising: the cards above are
    already family-agnostic, so a model type this panel has not learnt about
    still opens.
    """
    mt = getattr(model, "model_type", "")
    if mt == "glm":
        _GlmPanel(stats=stats)
    elif mt == "rf":
        _RfPanel(stats=stats, visible=visible)
    elif mt == "icar":
        _IcarPanel(stats=stats)
    elif mt in ("mw", "jnr"):
        _RmjPanel(stats=stats, visible=visible)


def _bar_scale(rows):
    """Estimate magnitude that fills the effect bar, per table.

    A fixed scale saturates: at the old 0.5 every |estimate| >= 0.5 drew at the
    same full width, so a real beta of 0.63 and one of 3.0 were pixel-identical
    — the column asserting they are equal effects when they are not. Scaling to
    the table's own largest magnitude keeps every bar distinct.

    The bar is a WITHIN-table comparator, not a cross-model one: the numeric
    estimate sits in the adjacent column for absolute reading, and two of these
    tables never appear side by side (one model per details dialog).
    """
    return (
        max((abs(r["estimate_raw"]) for r in rows if r["estimate_raw"]), default=0.0)
        or _BAR_FALLBACK_SCALE
    )


def _effect_bar(row, scale):
    """Centred effect bar for a coefficient row (the ``render`` CellSpec).

    Returns the cell callable ProductTable invokes inside the cell wrapper —
    see ``_render_cell`` in product_table.py. Positive estimates grow right
    from the centre line in the error colour (risk-raising), negative ones grow
    left in the success colour (protective). A credible interval that crosses
    zero is drawn grey: the sign is not resolved, so it must not be read as a
    direction.

    ``scale`` is the magnitude that fills the bar — see ``_bar_scale``.

    Arithmetic uses the row's ``*_raw`` floats — the sibling display strings are
    comma-grouped and would have to be parsed back.
    """
    est = row.get("estimate_raw")
    lo, hi = row.get("ci_low_raw"), row.get("ci_high_raw")

    def _cell():
        if est is None:
            solara.Text(DASH)
            return
        width = min(abs(est) / scale, 1.0) * _BAR_MAX_PCT
        # The width is rendered with :.0f, so anything under 0.5% would floor
        # to "0%" and be indistinguishable from a missing estimate. A nonzero
        # coefficient always gets a visible sliver.
        if est != 0:
            width = max(width, _BAR_MIN_PCT)
        crosses = lo is not None and hi is not None and lo < 0 < hi
        colour = (
            "grey"
            if crosses
            else ("var(--v-error-base)" if est > 0 else "var(--v-success-base)")
        )
        side = "left:50%;" if est > 0 else "right:50%;"
        with rv.Html(tag="div", style_="position:relative;height:9px;width:100%;"):
            rv.Html(
                tag="div",
                style_="position:absolute;left:50%;top:0;width:1px;height:9px;"
                "background:rgba(128,128,128,0.4);",
            )
            rv.Html(
                tag="div",
                style_=f"position:absolute;top:1px;height:7px;{side}"
                f"width:{width:.0f}%;background:{colour};border-radius:1px;",
            )

    return _cell


def _coefficient_table(columns, rows):
    """Read-only coefficient table shared by the GLM and iCAR panels."""
    ProductTable(
        title=t("tiles.train.stats.coef_header"),
        columns=columns,
        rows=rows,
        empty_text=t("tiles.train.stats.coef_empty"),
        show_actions=False,
    )


@solara.component
def _GlmPanel(stats):
    """Coefficients with display-time odds ratios, intercepts, solver line."""
    vm_rows = coefficient_rows(stats)
    scale = _bar_scale(vm_rows)
    rows = [
        {
            "key": r["name"],
            "cells": [
                {"type": "text", "value": r["name"]},
                {"type": "text", "value": r["estimate"]},
                {"type": "text", "value": r["odds_ratio"], "muted": True},
                {"type": "render", "fn": _effect_bar(r, scale)},
            ],
        }
        for r in vm_rows
    ]
    with solara.Column(gap="4px"):
        _coefficient_table(
            [
                {
                    "label": t("tiles.train.stats.col_predictor"),
                    "width": "minmax(0,2fr)",
                },
                {"label": t("tiles.train.stats.col_estimate"), "width": "90px"},
                {"label": t("tiles.train.stats.col_odds_ratio"), "width": "90px"},
                {"label": t("tiles.train.stats.col_effect"), "width": "minmax(0,1fr)"},
            ],
            rows,
        )
        # patsy's design 'Intercept' column and sklearn's own intercept_ are
        # two different numbers (spec §2.1); labelling them separately is what
        # stops a reader adding them up by accident.
        if stats.intercept_design is not None:
            solara.Text(
                t(
                    "tiles.train.stats.intercept_design",
                    value=fmt_stat(stats.intercept_design),
                ),
                style=MUTED,
            )
        if stats.intercept_fitted is not None:
            solara.Text(
                t(
                    "tiles.train.stats.intercept_fitted",
                    value=fmt_stat(stats.intercept_fitted),
                ),
                style=MUTED,
            )
        line = glm_convergence_line(stats)
        if line is not None:
            solara.Text(t("tiles.train.stats.converged_line", line=line), style=MUTED)


@solara.component
def _IcarPanel(stats):
    """Posterior summary table: estimate, SD and the 95% credible interval.

    A table rather than a forest plot on purpose: an ECharts whisker/forest
    series needs a JS ``renderItem`` callable, and callables cannot cross the
    widget wire. The centred effect bar carries the same shape information.

    Vrho is deliberately absent — it is a variance parameter, not a log-odds
    coefficient, so it neither belongs in this table nor takes the odds-ratio
    transform. It rides in ``stats.vrho`` and is already a stat card, as is the
    cell-level rho summary beside it.
    """
    vm_rows = coefficient_rows(stats)
    scale = _bar_scale(vm_rows)
    rows = [
        {
            "key": r["name"],
            "cells": [
                {"type": "text", "value": r["name"]},
                {"type": "text", "value": r["estimate"]},
                {"type": "text", "value": r["std"], "muted": True},
                {"type": "text", "value": r["ci_low"], "muted": True},
                {"type": "text", "value": r["ci_high"], "muted": True},
                {"type": "render", "fn": _effect_bar(r, scale)},
            ],
        }
        for r in vm_rows
    ]
    with solara.Column(gap="4px"):
        _coefficient_table(
            [
                {
                    "label": t("tiles.train.stats.col_predictor"),
                    "width": "minmax(0,2fr)",
                },
                {"label": t("tiles.train.stats.col_estimate"), "width": "80px"},
                {"label": t("tiles.train.stats.col_std"), "width": "70px"},
                {"label": t("tiles.train.stats.col_ci_low"), "width": "80px"},
                {"label": t("tiles.train.stats.col_ci_high"), "width": "80px"},
                {"label": t("tiles.train.stats.col_effect"), "width": "minmax(0,1fr)"},
            ],
            rows,
        )
        solara.Text(t("tiles.train.stats.icar_ci_note"), style=MUTED)


@solara.component
def _RfPanel(stats, visible=True):
    """Impurity importances as bars, plus the OOB number and its caveats.

    Both caveats are load-bearing rather than decorative: impurity importance
    is biased toward continuous and high-cardinality predictors and supports no
    causal reading (hence the note and the 'exploratory' chip), and sklearn's
    oob_score_ is plain accuracy on the training sample, never validation.
    """
    dark = use_theme_dark()  # hook: unconditional, before any branch
    entries = importance_entries(stats)
    option = importance_bars_option(entries, dark=dark)
    with solara.Column(gap="8px"):
        with solara.Row(gap="8px", style="align-items:center;"):
            solara.Text(t("tiles.train.stats.importance_header"))
            rv.Chip(
                children=[t("tiles.train.stats.exploratory_chip")],
                x_small=True,
                outlined=True,
            )
        if option is not None:
            EChartsChart(
                option=option,
                identity="model-stats-importance",
                dark=dark,
                renderer=RENDERER_SVG,
                height=f"{max(28 * len(entries), 120)}px",
                visible=visible,
            )
        if stats.oob_accuracy is not None:
            solara.Text(
                t("tiles.train.stats.oob_line", value=fmt_stat(stats.oob_accuracy)),
                style=MUTED,
            )
        solara.Text(t("tiles.train.stats.importance_bias_note"), style=MUTED)


@solara.component
def _RmjPanel(stats, visible=True):
    """MW/JNR distance curve, with the fitted threshold drawn on it.

    The curve comes from the tab_dist.csv the fit wrote. That file lives
    outside the project JSON, so it can be deleted independently — a missing
    or unreadable one degrades to a line of text (load_tab_dist returns None),
    never an error inside the dialog.
    """
    dark = use_theme_dark()  # hook: unconditional, before any branch
    # use_memo keeps the CSV read off every re-render (theme toggle, tab
    # switch); solara render bodies run on the websocket loop, so repeated
    # disk reads there are exactly what freezes the UI.
    path = getattr(stats, "tab_dist_path", None)
    rows = solara.use_memo(lambda: load_tab_dist(stats), [str(path)])
    option = dist_curve_option(rows, stats.dist_thresh, stats.perc_thresh, dark=dark)
    with solara.Column(gap="8px"):
        solara.Text(t("tiles.train.stats.dist_curve_header"))
        if option is not None:
            EChartsChart(
                option=option,
                identity="model-stats-dist-curve",
                dark=dark,
                renderer=RENDERER_SVG,
                height="260px",
                visible=visible,
            )
        else:
            solara.Text(t("tiles.train.stats.tab_dist_missing"), style=MUTED)

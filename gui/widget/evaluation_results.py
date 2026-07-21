# gui/widget/evaluation_results.py
"""Step 8 evaluation output: job status, saved-runs list, and table popup.

Saved evaluations are persisted on the project (project.evaluations). The list
rows only set a pending key; a single top-level EvaluationTableDialog renders the
selected run's table — the proven ConfirmDialog pattern (nested row->Dialog
toggles are unreliable).
"""

import pandas as pd
import reacton.ipyvuetify as rv
import solara
from solara.lab.components.theming import theme

from gui.i18n import t
from gui.scripts.echarts_options import RENDERER_SVG
from gui.scripts.evaluation_charts import (
    figure_entries, map_label, metric_bar_option, record_csizes, record_metrics)
from gui.scripts.evaluation_echarts import (
    PRED_OBS_SQUARE_HEIGHT, load_pred_obs_plot_data, points_csv_is_expected,
    pred_obs_chart_identity, pred_obs_renderer, pred_obs_scatter_option)
from gui.scripts.product_rows import evaluation_tab_rows
from gui.tile.evaluation_helpers import rows_for_record
from gui.widget.echarts import EChartsChart
from gui.widget.product_table import ProductTable

# One chart per metric, two per row — the height Plotly gave each subplot row.
_CHART_HEIGHT = "260px"

# Dialog tab order: table (0), charts (1), predicted-vs-observed (2). The figures
# tab loads its point CSVs only when it is the active one, so the scatter for a
# 200k-point run is never parsed just because the dialog opened on the table.
_FIGURES_TAB_INDEX = 2


@solara.component
def EvaluationResults(eval_jobs, project, on_open, on_delete, on_dismiss=None):
    """Evaluations table: saved runs (registry) plus in-flight/failed jobs.

    Args:
        eval_jobs: solara.Reactive[list] — transient per-run job dicts.
        project: solara.Reactive[Project] — source of project.evaluations.
        on_open: callback(key) — open the table popup for a saved run.
        on_delete: callback(key) — delete a saved run.
        on_dismiss: callback(job_id) — discard a failed job row.
    """
    p = project.value
    data = evaluation_tab_rows(p, eval_jobs.value)

    rows = []
    for r in data:
        if r["kind"] == "evaluation":
            actions = [
                {"kind": "open", "on_click": lambda *_, k=r["key"]: on_open(k)},
                {"kind": "delete", "on_click": lambda *_, k=r["key"]: on_delete(k)},
            ]
        elif r["status"] != "running" and on_dismiss is not None:
            actions = [{"kind": "dismiss", "on_click": lambda *_, i=r["job_id"]: on_dismiss(i)}]
        else:
            actions = []

        error = r.get("error")
        if r["status"] == "failed" and not error:
            error = t("widgets.evaluation_results.unknown_error")
        rows.append(
            {
                "key": r["key"],
                "cells": [
                    {"type": "text", "value": r["truth_tag"]},
                    {"type": "chip", "value": str(r["n_maps"])},
                    {"type": "text", "value": r["created_at"], "size": "0.78rem", "muted": True},
                    {"type": "status", "status": r["status"]},
                ],
                "actions": actions,
                "error": error,
            }
        )

    ProductTable(
        title=t("widgets.evaluation_results.evaluations_title"),
        columns=[
            {"label": t("widgets.evaluation_results.col_truth"), "width": "minmax(0,1fr)"},
            {"label": t("widgets.evaluation_results.col_maps"), "width": "55px"},
            {"label": t("widgets.evaluation_results.col_created"), "width": "minmax(0,1fr)"},
            {"label": t("widgets.evaluation_results.col_status"), "width": "95px"},
        ],
        rows=rows,
        empty_text=t("widgets.evaluation_results.empty"),
    )


@solara.component
def _ChartsTab(record, eval_key, active_tab=None):
    """Grouped-bar comparison of the run's maps, one chart per metric.

    ECharts has no subplots, so the single Plotly figure becomes a CSS grid of
    independent charts — two columns when there is more than one metric, which
    is the layout Plotly's ``ncols = 2 if len(metrics) > 1 else 1`` produced.

    The options are rebuilt on every render rather than memoized: they are a
    few small dicts, and EChartsChart hashes the option it is handed, so an
    equal option reuses the same widget instead of tearing the chart down.

    The identity carries only what the options do NOT show: ``active_tab`` (the
    dialog's selected tab index) so the charts are rebuilt when the tab is
    (re-)entered, and ``eval_key`` so switching runs starts from a fresh chart
    rather than inheriting the previous one's legend toggles — see the contract
    on EChartsChart.
    """
    indices = getattr(record, "indices", None) or []
    metrics = record_metrics(indices, getattr(record, "metrics", None))
    charts = [(m, metric_bar_option(indices, m, dark=theme.dark))
              for m in metrics]
    charts = [(m, option) for m, option in charts if option is not None]
    if not charts:
        solara.Info(t("widgets.evaluation_results.no_indices_info"))
        return

    ncols = 2 if len(charts) > 1 else 1
    with solara.Div(
        style=f"display: grid; grid-template-columns: repeat({ncols},"
              " minmax(0, 1fr)); gap: 12px; width: 100%;"
    ):
        for _metric, option in charts:
            EChartsChart(
                option=option,
                identity=f"{eval_key}|tab{active_tab}",
                dark=theme.dark,
                renderer=RENDERER_SVG,
                height=_CHART_HEIGHT,
            )


def _scatter_labels():
    """Translated word-labels for the predicted-vs-observed scatter option.

    The chart builder (``pred_obs_scatter_option``) owns English defaults; this
    is where translation happens, per the app's i18n rule. Only the words that
    differ by language are overridden — the metric symbols (MedAE, R2, n, ha) are
    identical across locales and stay on the builder's defaults. Whatever ends up
    here MUST also be handed to ``pred_obs_chart_identity`` so the option digest
    reflects a language switch.
    """
    return {
        "x_axis": t("widgets.evaluation_results.chart_x_axis"),
        "y_axis": t("widgets.evaluation_results.chart_y_axis"),
        "series": t("widgets.evaluation_results.chart_series"),
        "cell": t("widgets.evaluation_results.chart_cell"),
        "observed": t("widgets.evaluation_results.chart_observed"),
        "predicted": t("widgets.evaluation_results.chart_predicted"),
        "forest": t("widgets.evaluation_results.chart_forest"),
        "residual": t("widgets.evaluation_results.chart_residual"),
    }


@solara.component
def _PredObsCard(record, row, label, csize, png_path, fig_dir, labels,
                 eval_key, active_tab):
    """One map's card: the interactive scatter, or the PNG fallback ladder.

    The card header always names the map and, when the PNG exists, offers it as
    a download — the PNG stays the offline/report artifact. Below it, the
    fallback ladder (all non-fatal):

    * a DRAWABLE option -> the interactive ECharts scatter;
    * (a) a point CSV this run RECORDED that is gone or unreadable -> PNG
      **and** a warning;
    * (b) a map with no recorded point CSV (a legacy record, or a map this run
      never wrote a table for) and only a PNG -> the PNG, no warning;
    * (c) neither artifact on disk -> the missing-figure message with the path.

    The option is built BEFORE the branch, not inside it, because "point data
    loaded" and "there is something to draw" are not the same thing: a table
    whose plotted rows are all non-finite loads fine and yields ``None`` from
    ``pred_obs_scatter_option`` (see ``finite_points``). Handing that ``None``
    to the chart adapter would raise out of this render — killing the sibling
    maps' charts and both PNGs with it — so an unrenderable option simply falls
    through to the PNG rung like any other missing artifact.

    ``model``/``period`` come from the record's index row, so a row that lacks
    them (``row is None``) skips straight to the PNG rungs rather than crashing.
    """
    model = row.get("model") if row else None
    period = row.get("period") if row else None
    dark = theme.dark
    # The dialog always passes an index; a bare mount (None) counts as active so
    # a direct render still draws the chart.
    tab_active = active_tab is None or active_tab == _FIGURES_TAB_INDEX

    # One cheap identity (a single stat, no read) serving two purposes: the
    # load memo's key here, and the chart's `option_digest` below. Computed
    # unconditionally — it is None for a row with no model/period, which is a
    # perfectly stable key.
    digest = pred_obs_chart_identity(
        record, model, period, csize, dark=dark, labels=labels, title=None,
        fig_dir=fig_dir)

    def load_points():
        # Parse the point CSV only when this tab is the active one (see the
        # loader's lru_cache — the parse is memoized, but it must not run on
        # dialog open).
        if not (tab_active and model and period):
            return None
        return load_pred_obs_plot_data(
            record, model, period, csize, fig_dir=fig_dir)

    # The load lives in a use_memo, not in the render body, because it is I/O
    # with a side effect: a missing artifact makes the loader emit a
    # `logger.warning`, and the app pipes that logger into a Solara reactive
    # (gui/scripts/log_bridge.py). A render that logs on EVERY pass turns that
    # into per-render reactive traffic — which is exactly how this card used to
    # spin forever. Keyed on `digest` (record + resolved path + that file's
    # size/mtime + cell size + theme + label text) plus `tab_active`, so a
    # rewritten or re-pointed artifact still reloads.
    plot_data = solara.use_memo(load_points, [digest, tab_active])

    # Same labels/title into the option AND the digest (computed above): the
    # digest stands in for the adapter's content hash, so an input it misses is
    # stale. None here means "nothing drawable" — no data, or data with no
    # finite rows — and is handled by the ladder below, never passed on.
    #
    # Memoized on the SAME key as the load, and for a measured reason. Building
    # the option materializes one boxed [obs, pred, cell, forest, residual] list
    # per point; in the render body that ran on every pass, and
    # ``_scatter_rows``' module-level LRU (size 2) cannot absorb it because this
    # tab draws one card per map — a third card evicts the first, so a 3-map
    # dialog scored ZERO cache hits and paid the full rebuild every render
    # (measured 2026-07-21: 173 ms per pass for 200k/50k/25k-point maps, 0 hits
    # in 9 calls). The digest already names every input of the option, so
    # memoizing on it is exact: a theme flip, a language switch, a rewritten
    # artifact or a cell-size change all move the key.
    def build_option():
        if plot_data is None:
            return None
        return pred_obs_scatter_option(plot_data, dark=dark, labels=labels,
                                       title=None)

    option = solara.use_memo(build_option, [digest, tab_active])
    # One stat() for the whole render, not one per rung.
    png_exists = png_path.exists()

    with solara.Column(style=f"gap: 6px; width: {PRED_OBS_SQUARE_HEIGHT};"
                             " max-width: 100%;"):
        with solara.Row(style="justify-content: space-between;"
                              " align-items: center; gap: 8px;"):
            solara.Text(label, style="font-size: 0.85rem; font-weight: 600;")
            if png_exists:
                solara.FileDownload(
                    lambda p=png_path: p.read_bytes(),
                    filename=png_path.name,
                    label=t("widgets.evaluation_results.download_png"),
                    mime_type="image/png",
                )
        if option is not None:
            EChartsChart(
                option=option,
                # Extrinsic rebuild triggers: the run (fresh chart per subject),
                # the map + cell size, and the active tab — ipecharts sizes its
                # chart on attach and does not watch the container afterwards,
                # so a tab re-entry rebuilds rather than risking a mis-sized
                # chart. (Mitigation only — NOT verified in a browser.)
                identity=f"{eval_key}|{label}|{csize}|tab{active_tab}",
                dark=dark,
                renderer=pred_obs_renderer(plot_data),
                height=PRED_OBS_SQUARE_HEIGHT,
                # The digest above stands in for the adapter's content hash,
                # which at 50k points costs ~63 ms of JSON+sha1 per render.
                option_digest=digest,
            )
        elif png_exists:
            # Per-ARTIFACT, not per-record: only a map whose point table this
            # run actually recorded is one whose absence is a fault worth
            # reporting (rung a vs b).
            if tab_active and points_csv_is_expected(record, model, period,
                                                     csize):
                solara.Warning(
                    t("widgets.evaluation_results.chart_unavailable_warning"))
            solara.Image(png_path, width=PRED_OBS_SQUARE_HEIGHT)
        else:
            solara.Info(t("widgets.evaluation_results.missing_figure",
                          path=str(png_path)))


@solara.component
def _FiguresTab(record, eval_key=None, active_tab=None):
    """Interactive predicted-vs-observed scatter (one card per map).

    One chart card per prediction at a user-chosen cell size — the same
    comparison layout the image-only tab had, now explorable. The point CSV is
    loaded only when this tab is active (``active_tab == _FIGURES_TAB_INDEX``);
    each card degrades to the saved PNG (see ``_PredObsCard``) when its CSV is
    gone, so a moved or legacy run never blocks the table or the image.
    """
    from pathlib import Path

    indices = getattr(record, "indices", None) or []
    # The figures live beside the run's indices_all.csv (evaluation/<truth_tag>/
    # for legacy records, the run sub-folder for run-scoped ones).
    csv_path = getattr(record, "csv_path", None)
    fig_dir = Path(csv_path).parent if csv_path else None
    csizes = record_csizes(indices)
    selected, set_selected = solara.use_state(None)
    # A stale selection (from a previously opened run) falls back to the first
    # cell size instead of writing state during render.
    csize = selected if selected in csizes else (csizes[0] if csizes else None)
    if csize is None:
        solara.Info(t("widgets.evaluation_results.no_indices_info"))
        return

    if len(csizes) > 1:
        rv.Select(
            label=t("widgets.evaluation_results.csize_select_label"),
            items=csizes, v_model=csize, on_v_model=set_selected,
            dense=True, outlined=True, style_="max-width: 260px;",
        )

    labels = _scatter_labels()
    # figure_entries resolves the canonical PNG path per map (reused, not
    # reinvented); the index row supplies the model/period the loader needs.
    entries = figure_entries(indices, csize, fig_dir=fig_dir)
    rows_by_label = {
        map_label(r): r for r in indices
        if r.get("csize_coarse_grid") == csize
        and r.get("model") and r.get("period")
    }
    with solara.Row(style="flex-wrap: wrap; gap: 16px; align-items: flex-start;"):
        for label, png_path in entries:
            _PredObsCard(
                record=record, row=rows_by_label.get(label), label=label,
                csize=csize, png_path=png_path, fig_dir=fig_dir, labels=labels,
                eval_key=eval_key, active_tab=active_tab,
            )
    if not entries:
        solara.Info(t("widgets.evaluation_results.no_figures_info"))


@solara.component
def EvaluationTableDialog(project, eval_key, on_close):
    """Single popup showing the selected saved run: table, charts, figures.

    Three tabs: the indices table (only the metric columns the run selected —
    ``displayed_indices``), a grouped-bar chart comparing the maps per metric,
    and the predicted-vs-observed figures at a user-chosen cell size. The
    dialog is wide and table pagination is raised high enough that every row
    fits on one page.
    """
    p = project.value
    record = p.evaluations.get(eval_key) if (p is not None and eval_key) else None
    active_tab, set_active_tab = solara.use_state(0)
    # solara's DataFrame unsets Vuetify's `table {width: 100%}` so the table
    # shrink-wraps to its columns while the footer spans the card — restore it
    # here so the columns spread across the (fixed-width) dialog.
    solara.Style(
        ".evaluation-table-dialog .solara-data-table.v-data-table table"
        " { width: 100%; }"
    )
    with rv.Dialog(
        v_model=eval_key is not None,
        on_v_model=lambda v: None if v else on_close(),
        # An explicit width: a v-dialog otherwise shrink-wraps to its content,
        # and the tabbed layout no longer pushes it wide like the bare table did.
        width="90vw",
        max_width="1400px",
        eager=True,
    ):
        with rv.Card(class_="evaluation-table-dialog"):
            with rv.CardTitle():
                solara.Text(
                    t("widgets.evaluation_results.dialog_title", truth_tag=record.truth_tag) if record else t("widgets.evaluation_results.dialog_title_fallback"))
            with rv.CardText():
                if record is not None and record.indices:
                    with rv.Tabs(v_model=active_tab, on_v_model=set_active_tab, grow=False):
                        rv.Tab(children=[t("widgets.evaluation_results.tab_table")])
                        rv.Tab(children=[t("widgets.evaluation_results.tab_charts")])
                        rv.Tab(children=[t("widgets.evaluation_results.tab_figures")])
                    with rv.TabsItems(v_model=active_tab):
                        with rv.TabItem():
                            rows = rows_for_record(record)
                            solara.DataFrame(pd.DataFrame(rows), items_per_page=max(len(rows), 1))
                        with rv.TabItem():
                            _ChartsTab(record=record, eval_key=eval_key,
                                       active_tab=active_tab)
                        with rv.TabItem():
                            _FiguresTab(record=record, eval_key=eval_key,
                                        active_tab=active_tab)
                elif record is not None:
                    solara.Info(t("widgets.evaluation_results.no_indices_info"))
            with rv.CardActions(style_="justify-content: flex-end;"):
                solara.Button(t("common.close"), on_click=on_close, text=True, small=True)

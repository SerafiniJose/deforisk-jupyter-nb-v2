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
    figure_entries, metric_bar_option, record_csizes, record_metrics)
from gui.scripts.product_rows import evaluation_tab_rows
from gui.tile.evaluation_helpers import rows_for_record
from gui.widget.echarts import EChartsChart
from gui.widget.product_table import ProductTable

# One chart per metric, two per row — the height Plotly gave each subplot row.
_CHART_HEIGHT = "260px"


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


@solara.component
def _FiguresTab(record):
    """Predicted-vs-observed PNGs (one per map) at a user-chosen cell size."""
    from pathlib import Path

    indices = getattr(record, "indices", None) or []
    # The PNGs live beside the run's indices_all.csv (evaluation/<truth_tag>/).
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
    entries = figure_entries(indices, csize, fig_dir=fig_dir)
    with solara.Row(style="flex-wrap: wrap; gap: 16px; align-items: flex-start;"):
        for label, path in entries:
            with solara.Column(style="gap: 4px; width: 420px;"):
                solara.Text(label, style="font-size: 0.85rem; font-weight: 600;")
                if path.exists():
                    solara.Image(path, width="420px")
                else:
                    solara.Info(t("widgets.evaluation_results.missing_figure",
                                  path=str(path)))
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
                            _FiguresTab(record=record)
                elif record is not None:
                    solara.Info(t("widgets.evaluation_results.no_indices_info"))
            with rv.CardActions(style_="justify-content: flex-end;"):
                solara.Button(t("common.close"), on_click=on_close, text=True, small=True)

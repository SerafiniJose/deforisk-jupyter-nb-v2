"""Render-only, read-only summary tables for the Project Summary popup.

Each renderer takes the plain Project ``p``, shapes it via a Solara-free helper,
and draws a one-line stats banner above a CSS-grid table. No actions — these are
display-only and intentionally carry no edit/delete/map callbacks.
"""

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.summary_helpers import (
    raw_variable_rows,
    processed_variable_rows,
    dataset_rows,
    sample_rows,
    model_rows,
    prediction_rows,
    evaluation_rows,
)

# Shared styling — mirrors the grid/chip conventions in gui/widget/variable_list.py.
_GRID_BASE = "display:grid;align-items:center;width:100%;column-gap:16px;"
_HEADER_EXTRA = (
    "padding:4px 8px 6px;border-bottom:2px solid rgba(0,0,0,0.15);"
    "font-size:0.72rem;font-weight:600;color:grey;"
    "text-transform:uppercase;letter-spacing:0.05em;"
)
_ROW_EXTRA = "padding:5px 8px;border-bottom:1px solid rgba(0,0,0,0.08);"
_CELL = "display:flex;align-items:center;gap:4px;min-width:0;"
_NAME_CELL = "display:flex;align-items:center;gap:6px;min-width:0;overflow:hidden;"
_NAME_TEXT = "min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"


@solara.component
def _Banner(text: str):
    solara.Text(text, style="font-size:0.78rem;color:grey;padding:2px 8px 8px;")


@solara.component
def _Empty(text: str):
    solara.Text(text, style="color:grey;padding:8px;")


def _header(grid: str, labels):
    with rv.Html(tag="div", style_=grid + _HEADER_EXTRA):
        for lbl in labels:
            rv.Html(tag="span", children=[lbl])


@solara.component
def RawVariablesSummary(p):
    stats, rows = raw_variable_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.raw_vars_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) 90px 110px 70px;"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.vars_banner", total=stats['total'], vector=stats['vector'], raster=stats['raster']))
        _header(grid, [t("widgets.summary_lists.col_name"), t("widgets.summary_lists.col_type"), t("widgets.summary_lists.col_raster_type"), t("widgets.summary_lists.col_year")])
        for r in rows:
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["name"]), style=_NAME_TEXT)
                    if r["is_base"]:
                        rv.Chip(children=[t("widgets.summary_lists.chip_base")], x_small=True, color="info")
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["data_type"])], x_small=True, outlined=True, color="primary")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["raster_type"]), style="color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["year"]), style="color:grey;")


@solara.component
def ProcessedVariablesSummary(p):
    stats, rows = processed_variable_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.processed_vars_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) 120px 110px 70px;"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.vars_banner", total=stats['total'], vector=stats['vector'], raster=stats['raster']))
        _header(grid, [t("widgets.summary_lists.col_name"), t("widgets.summary_lists.col_source"), t("widgets.summary_lists.col_raster_type"), t("widgets.summary_lists.col_year")])
        for r in rows:
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["name"]), style=_NAME_TEXT)
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["source"])], x_small=True, outlined=True)
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["raster_type"]), style="color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["year"]), style="color:grey;")


@solara.component
def DatasetsSummary(p):
    stats, rows = dataset_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.datasets_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) minmax(0,1fr) 70px 70px;"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.datasets_banner", total=stats['total']))
        _header(grid, [t("widgets.summary_lists.col_name"), t("widgets.summary_lists.col_target"), t("widgets.summary_lists.col_feats"), t("widgets.summary_lists.col_year")])
        for r in rows:
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["name"]), style=_NAME_TEXT)
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["target_name"])], x_small=True, outlined=True, color="error")
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["feature_count"])], x_small=True, outlined=True)
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["year"]), style="color:grey;")


@solara.component
def SamplesSummary(p):
    stats, rows = sample_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.samples_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) minmax(0,1fr) 150px 60px;"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.samples_banner", total=stats['total'], points=stats['points']))
        _header(grid, [t("widgets.summary_lists.col_name"), t("widgets.summary_lists.col_strategy"), t("widgets.summary_lists.col_points_classes"), t("widgets.summary_lists.col_seed")])
        for r in rows:
            alloc = f" / {r['allocation']}" if r["allocation"] and r["allocation"] != "—" else ""
            strat = f"{r['strategy']}{alloc}"
            counts = ", ".join(f"{k}:{v}" for k, v in sorted(r["class_counts"].items()))
            points_str = f"{r['n_total']} ({counts})" if counts else str(r["n_total"])
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["name"]), style=_NAME_TEXT)
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(strat, style="font-size:0.8rem;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(points_str, style="font-size:0.8rem;color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["seed"]), style="color:grey;")


@solara.component
def ModelsSummary(p):
    stats, rows = model_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.models_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) 70px 60px 80px 90px minmax(0,1.2fr);"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.models_banner", total=stats['total'], trained=stats['trained']))
        _header(grid, [t("widgets.summary_lists.col_name"), t("widgets.summary_lists.col_type"), t("widgets.summary_lists.col_year"), t("widgets.summary_lists.col_samples"), t("widgets.summary_lists.col_deviance"), t("widgets.summary_lists.col_params")])
        for r in rows:
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["name"]), style=_NAME_TEXT)
                    if r["trained"]:
                        rv.Chip(children=[t("widgets.summary_lists.chip_trained")], x_small=True, color="success")
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["model_type"])], x_small=True, outlined=True, color="primary")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["year"]), style="color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["n_samples"]), style="color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["deviance"]), style="color:grey;")
                with rv.Html(tag="div", style_="min-width:0;overflow:hidden;"):
                    solara.Text(str(r["params"]), style=_NAME_TEXT + "font-size:0.78rem;color:grey;")


@solara.component
def PredictionsSummary(p):
    stats, rows = prediction_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.predictions_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) minmax(0,1fr) 70px 70px 60px;"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.predictions_banner", total=stats['total'], active=stats['active']))
        _header(grid, [t("widgets.summary_lists.col_model"), t("widgets.summary_lists.col_dataset"), t("widgets.summary_lists.col_year"), t("widgets.summary_lists.col_window"), t("widgets.summary_lists.col_active")])
        for r in rows:
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["model_key"]), style=_NAME_TEXT)
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["dataset_name"])], x_small=True, outlined=True)
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["year"]), style="color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["window"]), style="color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    rv.Icon(children=["mdi-check" if r["active"] else "mdi-minus"], small=True)


@solara.component
def EvaluationsSummary(p):
    stats, rows = evaluation_rows(p)
    if not rows:
        _Empty(t("widgets.summary_lists.evaluations_empty"))
        return
    grid = _GRID_BASE + "grid-template-columns:minmax(0,1fr) 60px minmax(0,1fr) minmax(0,1fr) minmax(0,1fr);"
    with solara.Column(style="gap:0;width:100%;"):
        _Banner(t("widgets.summary_lists.evaluations_banner", total=stats['total']))
        _header(grid, [t("widgets.summary_lists.col_name"), t("widgets.summary_lists.col_pred_count"), t("widgets.summary_lists.col_cell_sizes"), t("widgets.summary_lists.col_metrics"), t("widgets.summary_lists.col_created")])
        for r in rows:
            with rv.Html(tag="div", style_=grid + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_NAME_CELL):
                    solara.Text(str(r["name"]), style=_NAME_TEXT)
                with rv.Html(tag="div", style_=_CELL):
                    rv.Chip(children=[str(r["n_predictions"])], x_small=True, outlined=True)
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["csizes"]), style="font-size:0.8rem;color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["metrics"]), style="font-size:0.8rem;color:grey;")
                with rv.Html(tag="div", style_=_CELL):
                    solara.Text(str(r["created_at"]), style="font-size:0.78rem;color:grey;")

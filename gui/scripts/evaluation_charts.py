"""Plotly figure builders for the evaluation results dialog (solara-free).

Turns an EvaluationRecord's stored index rows (one per map x cell size) into
the comparison charts shown in the dialog's Charts tab, plus small helpers for
the predicted-vs-observed figures tab. Kept free of solara/ipyvuetify so the
figure layout is unit-testable without a render harness.
"""

from pathlib import Path

# Metric -> (axis title, direction hint). R2 is unitless; the errors are in ha.
_METRIC_TITLES = {
    "MedAE": "MedAE (ha) ↓",
    "R2": "R² ↑",
    "RMSE": "RMSE (ha) ↓",
    "wRMSE": "wRMSE (ha) ↓",
}
_METRIC_ORDER = ["MedAE", "R2", "RMSE", "wRMSE"]


def map_label(row):
    """Display label for a row's map: 'MODEL — period' (matches map_items)."""
    model = row.get("model") or row.get("prediction") or "?"
    period = row.get("period")
    return f"{model} — {period}" if period else str(model)


def record_csizes(rows):
    """Sorted unique coarse-grid cell sizes present in the rows."""
    return sorted({r["csize_coarse_grid"] for r in rows
                   if r.get("csize_coarse_grid") is not None})


def record_metrics(rows, selected):
    """Metric keys to chart: the run's selection, else every known metric."""
    keys = [m for m in (selected or _METRIC_ORDER) if m in _METRIC_ORDER]
    return [m for m in keys if any(r.get(m) is not None for r in rows)]


def figure_entries(rows, csize, fig_dir=None):
    """[(label, Path)] of predicted-vs-observed PNGs for one cell size.

    Stored index rows carry NO fig_path column (evaluate_against_truth's
    explicit column list drops it), so the path is derived from ``fig_dir``
    (the run's evaluation folder) + the deterministic file name written by
    _evaluate_one_against_truth: ``pred_obs_{model}_{period}_{csize}.png``.
    A row's own fig_path (in-memory rows, future schema) takes precedence.
    """
    entries = []
    for r in rows:
        if r.get("csize_coarse_grid") != csize:
            continue
        if r.get("fig_path"):
            path = Path(r["fig_path"])
        elif fig_dir is not None and r.get("model") and r.get("period"):
            path = Path(fig_dir) / f"pred_obs_{r['model']}_{r['period']}_{csize}.png"
        else:
            continue
        entries.append((map_label(r), path))
    return sorted(entries, key=lambda e: e[0])


def _csize_colors(n):
    """n blues, light -> dark, so shading encodes the ordered cell sizes."""
    from plotly.colors import sample_colorscale

    if n == 1:
        return ["#2a78d6"]
    return sample_colorscale("Blues", [0.35 + 0.55 * i / (n - 1) for i in range(n)])


def metric_bars_figure(rows, metrics, dark=False):
    """Grouped-bar comparison: one subplot per metric, x = map, bars = csize.

    Args:
        rows: EvaluationRecord.indices (list of dicts).
        metrics: metric keys to show (record.metrics; empty = all).
        dark: style for the app's dark theme.

    Returns a plotly Figure, or None when the rows hold nothing chartable.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    metrics = record_metrics(rows, metrics)
    csizes = record_csizes(rows)
    labels = sorted({map_label(r) for r in rows})
    if not metrics or not csizes or not labels:
        return None

    ncols = 2 if len(metrics) > 1 else 1
    nrows = (len(metrics) + ncols - 1) // ncols
    fig = make_subplots(
        rows=nrows, cols=ncols,
        subplot_titles=[_METRIC_TITLES[m] for m in metrics],
        vertical_spacing=0.28 / nrows,
    )

    colors = _csize_colors(len(csizes))
    by_key = {(map_label(r), r.get("csize_coarse_grid")): r for r in rows}
    for mi, metric in enumerate(metrics):
        row_i, col_i = mi // ncols + 1, mi % ncols + 1
        for ci, csize in enumerate(csizes):
            values = [
                (by_key.get((lab, csize)) or {}).get(metric) for lab in labels
            ]
            fig.add_trace(
                go.Bar(
                    x=labels, y=values,
                    name=f"csize {csize} px",
                    legendgroup=f"csize-{csize}",
                    showlegend=mi == 0,
                    marker={"color": colors[ci],
                            "line": {"width": 0}},
                    hovertemplate=(
                        "%{x}<br>" + metric + " = %{y}<br>csize "
                        + str(csize) + " px<extra></extra>"),
                ),
                row=row_i, col=col_i,
            )

    ink = "#c3c2b7" if dark else "#52514e"
    grid = "#33322f" if dark else "#e3e2dd"
    fig.update_layout(
        barmode="group",
        bargroupgap=0.08,
        height=max(280, 260 * nrows),
        margin={"l": 40, "r": 20, "t": 40, "b": 30},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": ink, "size": 12},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.06,
                "xanchor": "right", "x": 1},
        showlegend=len(csizes) > 1,
    )
    fig.update_xaxes(showgrid=False, linecolor=grid)
    fig.update_yaxes(gridcolor=grid, zerolinecolor=grid, rangemode="tozero")
    for ann in fig.layout.annotations:  # subplot titles
        ann.font = {"color": ink, "size": 13}
    return fig

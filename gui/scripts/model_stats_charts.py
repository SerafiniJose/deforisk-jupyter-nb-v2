"""ECharts option builders for the model Statistics tab (solara-free).

Same conventions as evaluation_charts.py: plain JSON-serializable dicts, no
JS callables (they cannot cross the widget wire), theme ink/grid wired from
echarts_options.theme_colors, themed_option applied later by the widget.

Both charts draw one series, so both paint it in ``accent_color`` — the app's
``primary``, handed in by the widget layer (``gui.widget.echarts.theme_accent``)
rather than hardcoded, so a palette change carries.
"""

from gui.scripts.echarts_options import DEFAULT_ACCENT, accent_color, theme_colors


def importance_bars_option(entries, dark=False, accent=DEFAULT_ACCENT):
    """Horizontal importance bars, largest at the TOP.

    ECharts draws a category y-axis bottom-up, so the (descending) entries
    are reversed before they land on the axis.
    """
    if not entries:
        return None
    ink, grid = theme_colors(dark)["ink"], theme_colors(dark)["grid"]
    bar_color = accent_color(accent, dark=dark)
    rev = list(reversed(entries))
    return {
        "textStyle": {"fontSize": 12},
        "grid": {"left": 8, "right": 24, "top": 8, "bottom": 4, "containLabel": True},
        "tooltip": {"trigger": "item", "formatter": "{b}: {c}"},
        "xAxis": {
            "type": "value",
            "axisLine": {"show": False},
            "splitLine": {"lineStyle": {"color": grid}},
            "axisLabel": {"color": ink},
        },
        "yAxis": {
            "type": "category",
            "data": [n for n, _ in rev],
            "axisLine": {"lineStyle": {"color": grid}},
            "axisTick": {"show": False},
            "axisLabel": {"color": ink, "fontSize": 11},
        },
        "series": [
            {
                "type": "bar",
                "data": [round(v, 4) for _, v in rev],
                "itemStyle": {"color": bar_color},
                "barMaxWidth": 14,
            }
        ],
    }


def dist_curve_option(
    rows, dist_thresh, perc_thresh, dark=False, accent=DEFAULT_ACCENT
):
    """Cumulative deforestation (%) vs distance to forest edge (m).

    The fitted threshold is drawn as a dashed markLine (declarative — no JS).

    Line, fill and threshold marker all take the accent explicitly. Leaving any
    of them unset would fall back to ECharts' own first palette entry — a blue
    that belongs to no theme and never moves.
    """
    if not rows:
        return None
    ink, grid = theme_colors(dark)["ink"], theme_colors(dark)["grid"]
    line_color = accent_color(accent, dark=dark)
    data = [[r["distance"], r["perc"]] for r in rows]
    return {
        "textStyle": {"fontSize": 12},
        "grid": {"left": 8, "right": 24, "top": 24, "bottom": 4, "containLabel": True},
        "tooltip": {"trigger": "axis"},
        "xAxis": {
            "type": "value",
            "axisLine": {"lineStyle": {"color": grid}},
            "splitLine": {"show": False},
            "axisLabel": {"color": ink},
        },
        "yAxis": {
            "type": "value",
            "max": 100,
            "axisLine": {"show": False},
            "splitLine": {"lineStyle": {"color": grid}},
            "axisLabel": {"color": ink, "formatter": "{value}%"},
        },
        "series": [
            {
                "type": "line",
                "data": data,
                "showSymbol": False,
                "itemStyle": {"color": line_color},
                "lineStyle": {"width": 2, "color": line_color},
                "areaStyle": {"color": line_color, "opacity": 0.12},
                "markLine": {
                    "symbol": "none",
                    "lineStyle": {"type": "dashed", "color": line_color},
                    "label": {"formatter": f"{dist_thresh:.0f} m", "color": ink},
                    "data": [{"xAxis": float(dist_thresh)}],
                }
                if dist_thresh is not None
                else {"data": []},
            }
        ],
    }

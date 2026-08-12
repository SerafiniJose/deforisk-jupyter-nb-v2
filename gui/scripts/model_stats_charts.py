"""ECharts option builders for the model Statistics tab (solara-free).

Same conventions as evaluation_charts.py: plain JSON-serializable dicts, no
JS callables (they cannot cross the widget wire), theme ink/grid wired from
echarts_options.theme_colors, themed_option applied later by the widget.
"""

from gui.scripts.echarts_options import theme_colors

_BAR_COLOR = "#2171b5"  # matches the csize ramp's dark end


def importance_bars_option(entries, dark=False):
    """Horizontal importance bars, largest at the TOP.

    ECharts draws a category y-axis bottom-up, so the (descending) entries
    are reversed before they land on the axis.
    """
    if not entries:
        return None
    ink, grid = theme_colors(dark)["ink"], theme_colors(dark)["grid"]
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
                "itemStyle": {"color": _BAR_COLOR},
                "barMaxWidth": 14,
            }
        ],
    }


def dist_curve_option(rows, dist_thresh, perc_thresh, dark=False):
    """Cumulative deforestation (%) vs distance to forest edge (m).

    The fitted threshold is drawn as a dashed markLine (declarative — no JS).
    """
    if not rows:
        return None
    ink, grid = theme_colors(dark)["ink"], theme_colors(dark)["grid"]
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
                "lineStyle": {"width": 2},
                "areaStyle": {"opacity": 0.12},
                "markLine": {
                    "symbol": "none",
                    "lineStyle": {"type": "dashed"},
                    "label": {"formatter": f"{dist_thresh:.0f} m", "color": ink},
                    "data": [{"xAxis": float(dist_thresh)}],
                }
                if dist_thresh is not None
                else {"data": []},
            }
        ],
    }

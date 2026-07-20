"""The app's single seam onto ipecharts (Apache ECharts for ipywidgets).

Every chart in the app renders through ``EChartsChart``. Callers hand over a
plain, serializable ECharts option ``dict`` and a renderer choice; nothing
outside this module needs to know that ipecharts exists, which keeps the chart
builders (``gui/scripts/*``) pure and unit-testable.

Why ``EChartsRawWidget`` and not ``EChartsWidget``: ipecharts ships both. Its
``EChartsWidget.option`` trait is ``Instance(Option)`` — a tree of 57 typed
sub-widget traits (``Title``, ``XAxis``, ``seriesitems.Bar``, …) that a caller
must assemble object by object. ``EChartsRawWidget`` overrides that trait with
a plain ``Dict``, taking the option verbatim. Both share one frontend view, so
the raw path costs nothing and lets the chart builders stay dict-only.

The pure half of the adapter — palette, theme colours, option shaping, renderer
names — lives in ``gui/scripts/echarts_options`` so that solara-free chart
builders can import it without dragging solara in. Import that module directly
rather than reaching through this one.
"""

from pathlib import Path

import reacton.ipyvuetify as rv
import solara

from gui.scripts.echarts_options import (
    RENDERER_CANVAS, RENDERER_SVG, resolve_renderer, themed_option)

__all__ = [
    "DEFAULT_HEIGHT",
    "EChartsChart",
    "RENDERER_CANVAS",
    "RENDERER_SVG",
    "build_chart_widget",
    "frontend_asset_dir",
]

# ECharts measures its canvas from the container, and a container with no
# height renders an empty chart — so the height is always a concrete pixel
# value. The width is left to stretch (see build_chart_widget).
DEFAULT_HEIGHT = "360px"


def build_chart_widget(option, *, dark=False, renderer=RENDERER_SVG,
                       height=DEFAULT_HEIGHT):
    """Build an ipecharts widget from a plain option ``dict``.

    Applies the app's chart theme (transparent background, theme ink) to a copy
    of ``option``, so the caller's dict is left untouched and can be reused
    across a light/dark re-render.

    ``renderer`` is explicit rather than inferred: pass ``RENDERER_SVG`` for the
    small metric bar charts and ``RENDERER_CANVAS`` for dense scatters. Only the
    caller knows how many points it is about to draw.

    Sizing: ``width="auto"`` makes ipecharts add its ``echarts-widget-auto-width``
    class (100% of the container), so the chart tracks the width of whatever
    card or dialog holds it. ``height`` stays a fixed pixel value.
    """
    from ipecharts import EChartsRawWidget

    return EChartsRawWidget(
        option=themed_option(option, dark=dark),
        renderer=resolve_renderer(renderer),
        width="auto",
        height=height,
    )


@solara.component
def EChartsChart(option, identity, *, dark=False, renderer=RENDERER_SVG,
                 height=DEFAULT_HEIGHT):
    """Render an ECharts option dict as a chart.

    Args:
        option: plain serializable ECharts option dict.
        identity: string identifying *what* is being charted (e.g. the
            evaluation key plus the metric selection). The widget is rebuilt
            whenever this changes.
        dark: True for the app's dark theme.
        renderer: ``RENDERER_SVG`` (default, small bar charts) or
            ``RENDERER_CANVAS`` (high-point-count scatter).
        height: CSS pixel height of the chart container.

    The widget is built inside a ``use_memo`` keyed on the identity and the
    presentation inputs, so a re-render that changes neither reuses the same
    widget (no chart teardown, no flicker), while a change to either builds a
    fresh one. Recreating rather than mutating keeps the option dict and the
    live chart from drifting apart, and leaves no trait observers behind: the
    widget owns all its own state and is simply replaced.

    Note for charts inside tabs: ipecharts sizes its chart when the widget is
    attached to the DOM and on window resize; it does not watch the container
    for later size changes. A chart mounted while its tab is hidden therefore
    depends on the tab body attaching only once the tab is first activated.
    Including the active tab in ``identity`` forces a rebuild on tab entry and
    makes correct sizing independent of that behaviour.
    """
    widget = solara.use_memo(
        lambda: build_chart_widget(
            option, dark=dark, renderer=renderer, height=height),
        [identity, dark, renderer, height],
    )
    # Handing the widget to a container's `children` is how this app already
    # mounts non-solara widgets (see the SepalMap in solara_app.Page). It puts
    # the exact widget instance in the tree; `solara.display` instead falls
    # back to IPython display when there is no notebook context, so the chart
    # never reaches the DOM under a plain render. The wrapper also supplies the
    # full-width block the chart's own auto-width class stretches to.
    rv.Html(tag="div", children=[widget], style_="width: 100%;")


def frontend_asset_dir():
    """Directory holding ipecharts' prebuilt JS, or None if it is missing.

    The widget's frontend (echarts itself included) must be served from the
    installed package: SEPAL deployments cannot rely on reaching a JS CDN.
    Returns the prebuilt labextension directory — either the copy inside the
    package (pip wheels) or the one under ``share/jupyter/labextensions``
    (conda packages, and pip after the data files are installed).
    """
    import sys

    import ipecharts

    candidates = [
        Path(ipecharts.__file__).parent / "labextension",
        Path(sys.prefix) / "share" / "jupyter" / "labextensions" / "ipecharts",
    ]
    return next((c for c in candidates if (c / "package.json").is_file()), None)

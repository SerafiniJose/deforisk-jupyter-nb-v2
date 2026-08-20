"""Theme, palette and option shaping for the app's ECharts charts (solara-free).

The application-owned half of the ECharts adapter: everything a chart builder
needs to produce a plain, serializable ECharts option ``dict``. Deliberately
free of solara, ipyvuetify *and* ipecharts so the chart builders in
``gui/scripts/`` can import it — the widget half lives in
``gui/widget/echarts.py`` and is the only place that knows the ipecharts API.

``csize_colors`` replaces ``plotly.colors.sample_colorscale("Blues", ...)``.
The Plotly call resampled a library-owned colourscale on every run, so the bars
could silently change colour on a plotly upgrade. Here the ColorBrewer *Blues*
stops are frozen as module constants and interpolated in pure Python, which
reproduces the ramp the app drew before this migration.

Checked against plotly 6.6.0 for 2..32 series: identical for every count up to
16 — far past the handful of coarse-grid cell sizes a real run compares — and
for most beyond. n = 17, 21 and 29 differ in one channel by one 8-bit step,
because there the interpolation lands exactly halfway between two stops and
plotly's own answer is decided by floating-point noise (it rounds such ties up
in one place and down in another). That instability is the reason these values
are frozen here; the difference is invisible.
"""

# ECharts' two renderers. Policy: SVG for the small metric bar charts (crisp
# text, tiny DOM, scales with the browser zoom); canvas for the predicted-vs-
# observed scatters, where one SVG node per point would swamp the DOM. The
# choice is always the caller's — see gui/widget/echarts.py.
RENDERER_SVG = "svg"
RENDERER_CANVAS = "canvas"
RENDERERS = (RENDERER_SVG, RENDERER_CANVAS)

# The chart draws straight onto the dialog's surface in both themes.
TRANSPARENT = "transparent"

# Ink (text) and grid/axis-line colours, carried over verbatim from the Plotly
# charts these replace so the migration is invisible to the eye.
_INK = {True: "#c3c2b7", False: "#52514e"}
_GRID = {True: "#33322f", False: "#e3e2dd"}

# ColorBrewer "Blues", the 9 stops behind plotly's colourscale of that name.
_BLUES_STOPS = (
    "#f7fbff", "#deebf7", "#c6dbef", "#9ecae1", "#6baed6",
    "#4292c6", "#2171b5", "#08519c", "#08306b",
)
# The sub-range of the ramp the app samples: skips the near-white low end
# (invisible on the light theme) and the near-black high end.
_RAMP_START = 0.35
_RAMP_END = 0.90

# One cell size means shading would encode nothing, so a single flat blue.
SINGLE_SERIES_COLOR = "#2a78d6"


def _rgb(hex_color):
    """'#rrggbb' -> (r, g, b)."""
    return tuple(int(hex_color[i:i + 2], 16) for i in (1, 3, 5))


def _hex(rgb):
    """(r, g, b) -> '#rrggbb', rounding halves up.

    Half-up (not Python's round-half-to-even) so the sampled ramp matches the
    Plotly output it replaces channel-for-channel: an interpolated channel
    lands on an exact .5 for some series counts, where banker's rounding would
    drift one step darker.
    """
    return "#" + "".join(f"{int(c + 0.5):02x}" for c in rgb)


def _sample_blues(position):
    """Piecewise-linear sample of the frozen Blues stops at 0.0 <= p <= 1.0."""
    x = position * (len(_BLUES_STOPS) - 1)
    lower = min(int(x), len(_BLUES_STOPS) - 2)
    frac = x - lower
    start, end = _rgb(_BLUES_STOPS[lower]), _rgb(_BLUES_STOPS[lower + 1])
    return _hex(tuple(s + frac * (e - s) for s, e in zip(start, end)))


def csize_colors(n):
    """``n`` blues, light -> dark, so shading encodes the ordered cell sizes.

    A pure function of the series count: same ``n`` always yields the same
    hex strings. ``n == 1`` returns the single flat blue.
    """
    if n < 1:
        raise ValueError(f"need at least one series, got {n}")
    if n == 1:
        return [SINGLE_SERIES_COLOR]
    step = (_RAMP_END - _RAMP_START) / (n - 1)
    return [_sample_blues(_RAMP_START + step * i) for i in range(n)]


def theme_colors(dark=False):
    """``{"ink", "grid"}`` for the app's light/dark themes.

    ``ink`` is text (titles, labels, legend); ``grid`` is grid lines, axis
    lines and zero lines. Chart builders read these rather than hardcoding.
    """
    return {"ink": _INK[bool(dark)], "grid": _GRID[bool(dark)]}


def themed_option(option, *, dark=False):
    """Return a copy of ``option`` with the app's chart theme applied.

    Centralises the two settings every chart in the app shares — a transparent
    background and the theme's ink colour for text — so no chart builder has to
    repeat them. Everything else the caller set is passed through untouched;
    within ``textStyle`` only ``color`` is overridden, so a caller's font size
    survives.

    Only ``theme_colors()["ink"]`` is applied here, to ``backgroundColor`` and
    ``textStyle.color``. ``theme_colors()["grid"]`` is NOT applied by this
    function: grid/axis-line colour is set per-axis (``axisLine``,
    ``splitLine``, ...) and this adapter has no way to know a caller's axis
    structure. Chart builders that draw axes must read
    ``theme_colors(dark)["grid"]`` themselves and wire it into their own axis
    options.

    The caller's top-level dict is never mutated: callers reuse one option
    across light and dark renders. This is a shallow copy — only the
    top-level keys this function touches (``backgroundColor``, ``textStyle``)
    are copied; nested values such as ``option["series"]`` are shared by
    reference with the input, so builders should construct fresh nested
    values themselves rather than relying on this function to isolate them.
    """
    themed = dict(option)
    themed["backgroundColor"] = TRANSPARENT
    themed["textStyle"] = {
        **option.get("textStyle", {}),
        "color": theme_colors(dark)["ink"],
    }
    return themed


def resolve_renderer(renderer):
    """Validate a renderer name, returning it unchanged.

    ECharts silently falls back to canvas for an unknown renderer, which turns
    a typo into a performance mystery rather than an error — so reject it here.
    """
    if renderer not in RENDERERS:
        raise ValueError(
            f"renderer must be one of {RENDERERS}, got {renderer!r}")
    return renderer

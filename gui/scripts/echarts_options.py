"""Theme, palette and option shaping for the app's ECharts charts (solara-free).

The application-owned half of the ECharts adapter: everything a chart builder
needs to produce a plain, serializable ECharts option ``dict``. Deliberately
free of solara, ipyvuetify *and* ipecharts so the chart builders in
``gui/scripts/`` can import it — the widget half lives in
``gui/widget/echarts.py`` and is the only place that knows the ipecharts API.

**Series colour comes from the app accent.** ``accent_ramp`` derives every
chart's series colours from one input: the Vuetify ``primary`` colour (green on
the light theme, gold on the dark one) that already paints every
``color="primary"`` control. It replaced a frozen ColorBrewer *Blues* ramp,
which made the charts the only surface in the app painted in a colour the theme
does not own — they matched neither the rest of the UI nor each other after a
palette change. Deriving the ramp makes that drift impossible: move
``themes.light.primary`` and the bars, lines and point clouds move with it.

This module stays solara-free, so it never *reads* the live theme — the accent
arrives as an argument, and ``DEFAULT_ACCENT`` is only the fallback for a caller
that passes none. The solara-aware reader is ``gui.widget.echarts.theme_accent``.
"""

import colorsys

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

# Fallback accent for a caller that supplies none — pysepal's light-theme
# ``primary``. A duplicated literal on purpose: importing pysepal here would
# drag solara in through it and break this module's layering. It is a floor, not
# the source of truth; the live value always comes from the theme.
DEFAULT_ACCENT = "#5BB624"

# Lightness ends of the derived ramp (HSL). Both stop well short of the
# extremes: a near-white bar disappears against the light theme's surface and a
# near-black one against the dark theme's, and one ramp has to read on both.
_RAMP_LIGHTEST = 0.68
_RAMP_DARKEST = 0.30

# ``(min, max)`` lightness a single mark may take on each surface — dark theme
# first. The dark surface sets a floor (nothing may sink into it), the light one
# a ceiling. See ``accent_color``.
_SURFACE_LIGHTNESS = {True: (0.52, 0.86), False: (0.16, 0.62)}


def _parse_hex(color):
    """``'#rgb'`` / ``'#rrggbb'`` -> ``(r, g, b)`` in 0..1, or ``None``.

    Returns ``None`` rather than raising for anything else. The accent comes off
    a live theme slot, which may hold a Vuetify colour name, an empty string or
    nothing at all — none of which may take a render down; callers fall back to
    ``DEFAULT_ACCENT`` instead (see ``resolve_accent``).
    """
    if not isinstance(color, str):
        return None
    h = color.strip().lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    if len(h) != 6:
        return None
    try:
        return tuple(int(h[i : i + 2], 16) / 255 for i in (0, 2, 4))
    except ValueError:
        return None


def _to_hex(rgb):
    """``(r, g, b)`` in 0..1 -> ``'#rrggbb'``, rounding halves up.

    Half-up rather than Python's round-half-to-even so a channel landing on an
    exact .5 does not drift one step darker for some series counts and not
    others — the ramp has to be reproducible from its inputs alone.
    """
    return "#" + "".join(f"{int(c * 255 + 0.5):02x}" for c in rgb)


def _resolve_accent(accent):
    """``accent`` as ``(r, g, b)`` floats, falling back when it is unusable."""
    return _parse_hex(accent) or _parse_hex(DEFAULT_ACCENT)


def _surface_rgb(accent, dark):
    """``accent`` as ``(r, g, b)`` floats, clamped to read on the surface.

    Hue and saturation are never touched — only lightness, and only into
    ``_SURFACE_LIGHTNESS[dark]``. See ``accent_color`` for why.
    """
    hue, lightness, sat = colorsys.rgb_to_hls(*_resolve_accent(accent))
    low, high = _SURFACE_LIGHTNESS[bool(dark)]
    return colorsys.hls_to_rgb(hue, min(max(lightness, low), high), sat)


def accent_color(accent=DEFAULT_ACCENT, dark=False):
    """``accent`` normalized to ``'#rrggbb'``, kept legible on the surface.

    What a chart with one thing to draw paints it: the app's accent itself, so a
    lone importance bar or distance curve is the same green/gold as the button
    that produced it.

    Lightness is clamped into ``_SURFACE_LIGHTNESS[dark]`` — hue and saturation
    are never touched, so the result is always the same colour, only lifted or
    dropped far enough to be seen. The dark theme's accent is a deep gold
    (``#76591e``, lightness 0.29) and the surface behind it is near-black, so an
    unclamped mark reads as a smudge; a translucent one disappears outright.
    Material's own dark palettes lighten the accent for exactly this reason —
    pysepal's ``primary_contrast`` slot is that same adjustment made by hand.
    Deriving it instead keeps the charts on one input (``primary``), which is
    what makes them follow a palette change.
    """
    return _to_hex(_surface_rgb(accent, dark))


def accent_ramp(n, accent=DEFAULT_ACCENT, dark=False):
    """``n`` shades of ``accent``, light -> dark.

    One colour per ordered category (the evaluation charts' coarse-grid cell
    sizes), so the shading itself encodes the order. Only lightness moves — hue
    and saturation stay the accent's — which is what keeps every shade
    recognisably the app's own colour instead of an unrelated ramp.

    The ramp's own ends already sit inside readable bounds for both surfaces, so
    ``dark`` only reaches the ``n == 1`` case, where there is no ramp to speak
    of and the mark is a plain ``accent_color``.

    A pure function of its inputs: the same ``(n, accent, dark)`` always yields
    the same hex strings.
    """
    if n < 1:
        raise ValueError(f"need at least one series, got {n}")
    if n == 1:
        return [accent_color(accent, dark=dark)]
    hue, _lightness, sat = colorsys.rgb_to_hls(*_resolve_accent(accent))
    step = (_RAMP_LIGHTEST - _RAMP_DARKEST) / (n - 1)
    return [
        _to_hex(colorsys.hls_to_rgb(hue, _RAMP_LIGHTEST - step * i, sat))
        for i in range(n)
    ]


def accent_fill(accent, alpha, dark=False):
    """``'rgba(r, g, b, alpha)'`` — the accent at partial opacity.

    For marks that must not hide what is behind them: in the predicted-vs-
    observed scatter, overlapping translucent points are how density reads.
    Built on ``accent_color``, so the surface clamp applies here too — it
    matters most here, because alpha pulls a mark toward the surface behind it
    and a dark accent on the dark theme would wash out entirely.
    """
    r, g, b = (int(c * 255 + 0.5) for c in _surface_rgb(accent, dark))
    return f"rgba({r}, {g}, {b}, {alpha})"


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
        raise ValueError(f"renderer must be one of {RENDERERS}, got {renderer!r}")
    return renderer

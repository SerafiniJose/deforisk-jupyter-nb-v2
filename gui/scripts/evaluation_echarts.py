"""Interactive predicted-vs-observed scatter: point loading + option (solara-free).

The archived PNG (``spatialrisk.evaluation.save_pred_obs_png``) shows one map's
per-cell predicted vs observed deforestation as a static image. This module
builds the *interactive* twin of that figure for the Evaluation dialog: it finds
the run's saved point CSV, loads it, and returns a plain ECharts option dict.
The widget layer renders it; nothing here imports solara, ipyvuetify or
ipecharts, so the whole chart is unit-testable without a render harness.

**One frozen table, two renderings.** The chart plots the exact float values
sitting in ``pred_obs_{model}_{period}_{csize}.csv`` — no rounding, no
resampling, and no downsampling at any point count. The PNG and this chart must
never disagree about what a model predicted for a cell, so the numbers travel
from the CSV to the option untouched. That exactness depends on HOW the file is
read: pandas' default CSV float parser is fast but not correctly rounded (it
returns ``0.92`` for the text ``0.9199999999999999``), so the loader reads with
``float_precision="round_trip"`` — see ``_load_cached``. ``PredObsPlotData``
(shared with the PNG writer) supplies the axis bounds and the finite-row filter,
which is what keeps the two figures' domains identical too.

**Non-finite values.** ``PredObsPlotData.points`` here holds the persisted rows
for the four plotted columns (the loader reads no more than the chart draws) and
may include NaN/inf; those are not valid JSON and would break the option on the
wire. Only ``finite_points`` is ever plotted — matching ``save_pred_obs_png``,
which drops the same rows. The annotation's ``n`` stays the full ``ncell``, also
matching the PNG.

**Performance.** Loading is memoized on the artifact's *modification identity*
(path + size + mtime), not just its path: two runs against the same truth
produce identically named files, and keying on the name alone would serve one
run's points under another run's record. ``pred_obs_chart_identity`` wraps that
same key — plus the theme and the caller's label/title text — into one short
string, so the widget can memoize its option build and skip the adapter's
per-render option hash (see ``EChartsChart(option_digest=)``). Because it stands
in for that hash, it has to cover every option input, not just the file's.
"""

import hashlib
import logging
from functools import lru_cache
from pathlib import Path

from gui.scripts.echarts_options import (
    RENDERER_CANVAS, RENDERER_SVG, theme_colors)
from gui.scripts.evaluation_charts import pred_obs_artifact_name

logger = logging.getLogger("spatial_risk")

# Point count at which the scatter switches from SVG to canvas + ECharts'
# `large`/`progressive` pipeline. 2000 is ECharts' OWN default for
# `largeThreshold` and (with `progressiveThreshold` at 3000) the point where its
# maintainers judged per-item rendering to stop paying off; it is adopted here
# rather than invented so the number has a source. It is also roughly where one
# SVG <circle> per point starts to make the DOM the bottleneck.
#
# Task 8 measures this on real runs and documents the result — this constant is
# the single place to change if the measurement says otherwise.
PRED_OBS_LARGE_POINT_COUNT = 2000

# Points per progressive-render chunk once large mode is on. ECharts' default
# `progressive` is 400, which is too small here: the whole scatter arrives in one
# option, so larger chunks mean fewer frames to first complete paint.
PRED_OBS_PROGRESSIVE_CHUNK = 5000

# Two caches, two very different per-entry costs — so two constants. Measured on
# a 200k-point run (the realistic upper end), pandas 2.x / CPython 3.11:
#
#   _load_cached entry   (4-column DataFrame)      6.4 MB   -> 8 x  6.4 =  51 MB
#   _scatter_rows entry  (boxed [[float x5], ...]) 48.0 MB  -> 2 x 48.0 =  96 MB
#
# The row cache costs 7.5x the frame it is built from: every value becomes a
# boxed Python float/int inside a per-point list object, which is the price of
# a JSON-serializable option. Sizing both at 8 would have reserved ~435 MB on a
# SEPAL deployment for a dialog that shows one chart at a time.
#
# The loader stays at 8: cheap, and it is what makes flipping between maps and
# cell sizes feel instant. The row cache is kept at 2 — enough for a light/dark
# toggle and an A/B flip between two maps to stay hot, which is the pattern that
# actually recurs; a third map costs one rebuild, not a wrong chart.
POINTS_CACHE_SIZE = 8
SCATTER_ROWS_CACHE_SIZE = 2

# Reference line: red dashed, exactly as the PNG's `plt.plot(p, p, "r--")`.
REFERENCE_LINE_COLOR = "#ff0000"

# Colours of the point cloud. Matplotlib drew unfilled markers with a black
# edge; on a dark surface a black edge disappears, so the edge follows the ink.
_POINT_FILL = {True: "rgba(66,146,198,0.65)", False: "rgba(42,120,214,0.55)"}

# Symmetric horizontal insets keep the plot box centred, so the chart reads as
# square whenever its container is (see PRED_OBS_SQUARE_HEIGHT). ECharts has no
# aspect lock for a cartesian grid — squareness is a container property, and the
# option's job is only to not skew it.
_GRID_INSET = 16
PRED_OBS_SQUARE_HEIGHT = "560px"

# English defaults. Every one is overridable via `labels=`: the widget layer
# owns translation (`t(...)`), and this module must not import gui.i18n's
# machinery into a pure builder. The numeric FORMATS are not overridable — they
# are frozen to the PNG's ("MedAE = {:.2f} ha", "R2 = {:.2f}", "n = {:d}") so
# the two figures always quote the same rounded values.
DEFAULT_LABELS = {
    "x_axis": "Observed deforestation (ha)",
    "y_axis": "Predicted deforestation (ha)",
    "series": "Grid cells",
    "cell": "Cell",
    "observed": "Observed",
    "predicted": "Predicted",
    "forest": "Forest",
    "residual": "Residual",
    "medae": "MedAE",
    "r2": "R2",
    "n": "n",
    "ha": "ha",
}

_POINT_CSV_COLUMNS = ["cell", "nfor_obs_ha", "ndefor_obs_ha", "ndefor_pred_ha"]


# ---------------------------------------------------------------------------
# Artifact resolution — typed record first, legacy derivation second
# ---------------------------------------------------------------------------

def _record_artifacts(record):
    return list(getattr(record, "artifacts", None) or [])


def _artifact_points_csv(record, model, period, csize, prediction_key=None):
    """Path this run RECORDED for one map + cell size, or None.

    The typed half of ``resolve_points_csv``: it looks only at
    ``record.artifacts`` and never derives a path. Matching is on
    ``(model, period, csize)`` — the file-level identity, since the file name is
    built from exactly those three — narrowed by ``prediction_key`` when the
    caller has one and the artifact records it.
    """
    if csize is None or not model or not period:
        return None
    csize = int(csize)
    for art in _record_artifacts(record):
        if (getattr(art, "model", None) != model
                or getattr(art, "period", None) != period
                or int(getattr(art, "csize_px", -1)) != csize):
            continue
        if (prediction_key is not None
                and getattr(art, "prediction_key", None) != prediction_key):
            continue
        points_csv = getattr(art, "points_csv", None)
        if points_csv:
            return Path(points_csv)
    return None


def points_csv_is_expected(record, model, period, csize, *,
                           prediction_key=None):
    """True when THIS map + cell size has a recorded point-CSV artifact.

    A *per-artifact* question, deliberately not a per-record one. A run-scoped
    record can carry a partially populated artifact list — one map recorded, its
    sibling not — and ``resolve_points_csv`` falls back to the derived path for
    the omitted ones on purpose. Asking ``bool(record.artifacts)`` instead would
    treat "this run never recorded a table for this map" as "this run's table
    went missing" and warn about maps that were never promised.
    """
    return _artifact_points_csv(
        record, model, period, csize, prediction_key) is not None


def resolve_points_csv(record, model, period, csize, *,
                       prediction_key=None, fig_dir=None):
    """Path of the point CSV for one map + cell size, or None.

    Two eras of records, one resolution order:

    * **Typed** (run-scoped, Task 4): ``record.artifacts`` names the file this
      run actually wrote under ``evaluation/<truth_tag>/<run_id>/``. Preferred
      always — it is the only path that stays correct after a later run against
      the same truth overwrites the shared folder.
    * **Derived** (the fallback): the path is built the same way
      ``figure_entries`` builds the PNG's — the run's folder (``fig_dir``,
      defaulting to ``Path(record.csv_path).parent``) plus the deterministic
      name from ``pred_obs_artifact_name``.

    The fallback fires on ANY miss, not only on a record with no artifacts at
    all: an empty list, a list that names other maps, and a matching artifact
    whose ``points_csv`` is blank all land there. That is deliberate — a typed
    record's ``csv_path`` already points inside its own run folder, so the
    derived path stays run-scoped for exactly the records run-scoping protects,
    and a partially populated artifact list still resolves the maps it omitted
    instead of blanking them. Narrowing it to ``artifacts == []`` would buy no
    extra scoping safety and would regress those records to "no chart".

    Matching is on ``(model, period, csize)``, which is the file-level identity:
    the file name is built from exactly those three. ``prediction_key`` narrows
    it further when the caller has one and the artifact records it — two
    predictions can share a model+period label while being different maps.
    """
    if csize is None or not model or not period:
        return None
    csize = int(csize)
    recorded = _artifact_points_csv(record, model, period, csize,
                                    prediction_key)
    if recorded is not None:
        return recorded

    if fig_dir is None:
        csv_path = getattr(record, "csv_path", None)
        if not csv_path:
            return None
        fig_dir = Path(csv_path).parent
    return Path(fig_dir) / pred_obs_artifact_name(model, period, csize, "csv")


def _index_row_for(record, model, period, csize):
    """The record's stored index row for one map + cell size ({} if absent)."""
    for row in getattr(record, "indices", None) or []:
        if (row.get("model") == model and row.get("period") == period
                and row.get("csize_coarse_grid") == int(csize)):
            return row
    return {}


def _modification_identity(path):
    """``(size, mtime_ns)`` of ``path``, or None when it is not readable.

    The cache key's whole reason for existing: two runs against one truth write
    identically named files, so a path alone does not identify a table. Size and
    mtime together change whenever the bytes do, at no read cost.
    """
    try:
        stat = path.stat()
    except OSError:
        return None
    return (stat.st_size, stat.st_mtime_ns)


def _record_key(record):
    """Short stable id for the record itself (run-scoped when possible)."""
    storage_key = getattr(record, "storage_key", None)
    if callable(storage_key):
        try:
            return str(storage_key())
        except Exception:  # noqa: BLE001 - identity must never break a render
            pass
    return str(getattr(record, "run_id", None) or getattr(record, "csv_path", ""))


# ---------------------------------------------------------------------------
# Loading — memoized on the artifact's modification identity
# ---------------------------------------------------------------------------

@lru_cache(maxsize=POINTS_CACHE_SIZE)
def _load_cached(path_str, size, mtime_ns, model, period, csize, csize_ha,
                 medae, r2):
    """Read one point CSV into a PredObsPlotData. Keyed by file identity.

    ``size``/``mtime_ns`` are in the key but unused in the body — that is the
    point: they make a rewritten (or different-run) file a cache miss. All args
    are hashable scalars so ``lru_cache`` can key on them directly.

    Only the four columns the chart draws are read; the other two exist for the
    archived table, not for the plot.

    ``float_precision="round_trip"`` is load-bearing, not a tuning knob.
    pandas' default C float parser is not correctly rounded and can land on a
    neighbouring double: the text ``0.9199999999999999`` parses to ``0.92``, and
    ``123456789.12345679`` to ``123456789.1234568``. ``to_csv`` writes the full
    round-trippable repr, so every bit of loss would be on this side of the
    trip — and since ``save_pred_obs_png`` draws from the in-memory frame, a
    default parse makes the PNG and this chart differ by up to 1 ulp. The
    round-trip parser is exactly ``float()`` on the text and costs a few ms per
    100k rows, once per file (this is memoized).
    """
    import pandas as pd

    from spatialrisk.evaluation import pred_obs_axis_bounds, PredObsPlotData

    points = pd.read_csv(path_str, usecols=_POINT_CSV_COLUMNS,
                         float_precision="round_trip")[_POINT_CSV_COLUMNS]
    axis_min, axis_max = pred_obs_axis_bounds(points)
    return PredObsPlotData(
        model=model, period=period, csize_px=int(csize), csize_ha=csize_ha,
        points=points, axis_min=axis_min, axis_max=axis_max,
        medae=medae, r2=r2, ncell=len(points),
    )


def load_pred_obs_plot_data(record, model, period, csize, *,
                            prediction_key=None, fig_dir=None):
    """Load one map's saved point table as a ``PredObsPlotData``, or None.

    Returns None when the artifact cannot be resolved, does not exist, or cannot
    be parsed — a saved run whose files were moved or deleted must degrade to a
    message in the dialog, never to an exception out of a render.

    ``ncell`` and the axis bounds come from the loaded frame, never from the
    record, so the ``ncell == len(points)`` invariant can never fail on a stale
    index row. ``MedAE``/``R2`` do come from the record's index row: they are the
    already-rounded values the PNG quoted, and recomputing them here would risk
    a different answer than the archived figure.

    Repeated calls with an unchanged file return the SAME object (see
    ``_load_cached``), which is what lets the option builder memoize on it.
    """
    path = resolve_points_csv(record, model, period, csize,
                              prediction_key=prediction_key, fig_dir=fig_dir)
    if path is None:
        return None
    identity = _modification_identity(path)
    if identity is None:
        logger.warning("Evaluation point table is missing: %s", path)
        return None
    row = _index_row_for(record, model, period, csize)
    try:
        return _load_cached(str(path), identity[0], identity[1],
                            model, period, int(csize),
                            row.get("csize_coarse_grid_ha"),
                            row.get("MedAE"), row.get("R2"))
    except Exception as exc:  # noqa: BLE001 - a bad CSV must not kill the dialog
        logger.warning("Evaluation point table is unreadable (%s): %s", path, exc)
        return None


def _text_identity(labels, title):
    """Short digest of the option's TEXT inputs (``labels`` + ``title``).

    Hashed rather than concatenated only to keep the identity short; these are a
    dozen tiny strings, so the cost is microseconds.
    """
    if not labels and title is None:
        return "-"
    payload = repr((sorted((labels or {}).items()), title))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:8]


def pred_obs_chart_identity(record, model, period, csize, *, dark=False,
                            labels=None, title=None,
                            prediction_key=None, fig_dir=None):
    """Cheap, stable identity of the chart this call would draw.

    Folds in EVERY input of ``pred_obs_scatter_option``: the record, the
    resolved artifact path, that file's modification identity, the cell size,
    the theme, and the ``labels``/``title`` text. Everything but the text comes
    from a single ``stat()`` — no read, no parse, no option hash — and the file
    half is the SAME key the loader memoizes on, so the two can never disagree
    about what is current.

    Two uses, both in the widget layer: memoize the option build, and hand the
    result to ``EChartsChart(option_digest=...)`` so the adapter skips hashing an
    option that can carry hundreds of thousands of points.

    **Pass the same ``labels`` and ``title`` you pass to
    ``pred_obs_scatter_option``.** Used as ``option_digest`` this REPLACES the
    adapter's content hash, so an input the identity misses is an input nothing
    checks: switching the app's language would leave the previous language's
    axis titles on screen with no error anywhere. The two signatures share their
    names and defaults precisely so one call site can pass the same values to
    both.
    """
    path = resolve_points_csv(record, model, period, csize,
                              prediction_key=prediction_key, fig_dir=fig_dir)
    if path is None:
        return None
    size, mtime_ns = _modification_identity(path) or (-1, -1)
    return "|".join(str(part) for part in (
        _record_key(record), path, size, mtime_ns, int(csize),
        "dark" if dark else "light", _text_identity(labels, title)))


# ---------------------------------------------------------------------------
# Option building
# ---------------------------------------------------------------------------

def pred_obs_renderer(plot_data):
    """SVG for a small scatter, canvas once it gets dense.

    SVG keeps text and markers crisp and scales with the browser zoom, but costs
    one DOM node per point; past ``PRED_OBS_LARGE_POINT_COUNT`` that is the
    bottleneck and canvas (with ``large``) wins outright.
    """
    return (RENDERER_CANVAS if _plotted_count(plot_data) >= PRED_OBS_LARGE_POINT_COUNT
            else RENDERER_SVG)


def _plotted_count(plot_data):
    return 0 if plot_data is None else len(plot_data.finite_points)


@lru_cache(maxsize=SCATTER_ROWS_CACHE_SIZE)
def _scatter_rows(plot_data):
    """``[[obs, pred, cell, forest_ha, residual], ...]`` for one plot data.

    Cached because building it is the only per-point Python work in this module
    and the option is rebuilt on every render. ``PredObsPlotData`` is a frozen
    dataclass with ``eq=False``, so it hashes by identity — and the loader hands
    back the same object for an unchanged file, which makes this a hit.

    Kept to ``SCATTER_ROWS_CACHE_SIZE`` (not the loader's size): an entry here
    costs 7.5x the DataFrame it comes from — 48 MB at 200k points against the
    frame's 6.4 MB — because every value is a boxed Python scalar in its own
    list. See the constants block for the measurements.

    Values are converted through ``.tolist()`` so they are native Python floats
    and ints, never numpy scalars: the option is serialized to the frontend as
    JSON, and a numpy scalar is not JSON-serializable.

    The residual (predicted - observed) is precomputed into the value array
    because ECharts tooltips here are template STRINGS, not callbacks — the
    option crosses the wire as a dict, so nothing can be computed at hover time.
    """
    points = plot_data.finite_points
    obs = points["ndefor_obs_ha"].to_numpy(dtype="float64")
    pred = points["ndefor_pred_ha"].to_numpy(dtype="float64")
    cells = points["cell"].to_numpy(dtype="int64").tolist()
    forest = points["nfor_obs_ha"].to_numpy(dtype="float64").tolist()
    residual = (pred - obs).tolist()
    return [list(row) for row in zip(obs.tolist(), pred.tolist(),
                                     cells, forest, residual)]


def pred_obs_annotation(plot_data, labels=None):
    """``MedAE = x ha / R2 = y / n = k``, with the PNG's exact number formats.

    A metric the record never stored is left out rather than printed as ``nan``.
    Only the words are translatable; the formats are frozen to the PNG's.
    """
    text = {**DEFAULT_LABELS, **(labels or {})}
    parts = []
    if plot_data.medae is not None:
        parts.append(f"{text['medae']} = {plot_data.medae:.2f} {text['ha']}")
    if plot_data.r2 is not None:
        parts.append(f"{text['r2']} = {plot_data.r2:.2f}")
    parts.append(f"{text['n']} = {plot_data.ncell:d}")
    return "\n".join(parts)


def pred_obs_scatter_option(plot_data, *, dark=False, labels=None, title=None):
    """ECharts option for one map's predicted-vs-observed scatter.

    Args:
        plot_data: a ``PredObsPlotData`` (from ``load_pred_obs_plot_data``).
        dark: style for the app's dark theme.
        labels: overrides for ``DEFAULT_LABELS`` — the widget layer passes
            translated strings here.
        title: chart title. ``PredObsPlotData.title`` is deliberately NOT used:
            it is the PNG's English text, and the dialog shows a translated one.

    Returns a plain, JSON-serializable option dict, or None when there is
    nothing to draw.

    **Whatever you pass as ``labels``/``title`` here must also be passed to
    ``pred_obs_chart_identity``** if you use its result as the adapter's
    ``option_digest``: that digest replaces the content hash, so text it does
    not cover cannot trigger a re-render. Both functions take these two by the
    same names and defaults so a single call site can forward them unchanged.

    Structure: one ``scatter`` series carrying every point, plus one ``line``
    series drawing the 1:1 reference. Both axes are pinned to the SAME
    ``axis_min``/``axis_max`` — an independently scaled y-axis would put points
    above or below the 1:1 line for reasons that have nothing to do with the
    model, which is the single most misleading thing this chart could do.

    ``themed_option`` (applied later by the widget) sets only the background and
    the top-level ink, so grid and axis colours are wired in here.
    """
    if plot_data is None:
        return None
    data = _scatter_rows(plot_data)
    if not data:
        return None

    text = {**DEFAULT_LABELS, **(labels or {})}
    colors = theme_colors(dark)
    ink, grid = colors["ink"], colors["grid"]
    axis_min, axis_max = float(plot_data.axis_min), float(plot_data.axis_max)
    large = len(data) >= PRED_OBS_LARGE_POINT_COUNT

    scatter = {
        "type": "scatter",
        "name": text["series"],
        "data": data,
        # cell/forest/residual ride along in the value array purely to reach the
        # tooltip template; only the first two dimensions are coordinates.
        "dimensions": [text["observed"], text["predicted"], text["cell"],
                       text["forest"], text["residual"]],
        "symbolSize": 7,
        "itemStyle": {
            "color": _POINT_FILL[bool(dark)],
            "borderColor": ink,
            "borderWidth": 0.5,
        },
        "emphasis": {"focus": "series"},
        "tooltip": {
            # Template string, not a callback: EChartsRawWidget serializes the
            # option, so no JS function can cross the wire. {c0}..{c4} index the
            # value array laid out by _scatter_rows.
            "formatter": (
                f"{text['cell']} {{c2}}<br/>"
                f"{text['observed']} = {{c0}} {text['ha']}<br/>"
                f"{text['predicted']} = {{c1}} {text['ha']}<br/>"
                f"{text['forest']} = {{c3}} {text['ha']}<br/>"
                f"{text['residual']} = {{c4}} {text['ha']}"
            ),
        },
    }
    if large:
        # Canvas-only fast path: one batched draw call instead of one shape per
        # point, rendered in chunks so the first frame does not block.
        scatter.update({
            "large": True,
            "largeThreshold": PRED_OBS_LARGE_POINT_COUNT,
            "progressive": PRED_OBS_PROGRESSIVE_CHUNK,
            "progressiveThreshold": PRED_OBS_LARGE_POINT_COUNT,
        })

    reference = {
        # The PNG's `plt.plot(p, p, "r--")`. Deliberately nameless, silent and
        # tooltip-free: it is an annotation, not data. A name would put it in
        # the legend and let a user toggle away the chart's frame of reference.
        "type": "line",
        "data": [[axis_min, axis_min], [axis_max, axis_max]],
        "showSymbol": False,
        "silent": True,
        "legendHoverLink": False,
        "tooltip": {"show": False},
        "lineStyle": {"color": REFERENCE_LINE_COLOR, "type": "dashed",
                      "width": 1.2},
        "z": 1,
    }

    axis = {
        "type": "value",
        "min": axis_min,
        "max": axis_max,
        "axisLine": {"lineStyle": {"color": grid}},
        "axisTick": {"show": False},
        "splitLine": {"lineStyle": {"color": grid}},
        "axisLabel": {"color": ink},
        "nameTextStyle": {"color": ink},
        "nameLocation": "middle",
    }

    option = {
        "textStyle": {"fontSize": 12},
        "animation": not large,  # animating 200k points helps nobody
        "grid": {"left": _GRID_INSET, "right": _GRID_INSET, "top": 64,
                 "bottom": 8, "containLabel": True},
        "tooltip": {"trigger": "item", "confine": True},
        # Nothing toggleable: one data series, and the reference line must stay.
        "legend": {"show": False},
        "toolbox": {
            "show": True,
            "right": 8,
            "top": 0,
            "feature": {
                # Box zoom over BOTH axes (not ECharts' x-only default): the
                # shared domain is this chart's whole premise, and an x-only
                # zoom would silently break it.
                "dataZoom": {},
                "restore": {},
                "saveAsImage": {"pixelRatio": 2},
            },
            "iconStyle": {"borderColor": ink},
        },
        # Wheel/pinch zoom over the plot itself, both axes together so the 1:1
        # line stays at 45 degrees through any zoom.
        "dataZoom": [{"type": "inside", "xAxisIndex": 0, "yAxisIndex": 0}],
        "xAxis": {**axis, "name": text["x_axis"], "nameGap": 28},
        "yAxis": {**axis, "name": text["y_axis"], "nameGap": 48},
        "series": [scatter, reference],
        "graphic": [{
            # The PNG's top-left MedAE/R2/n block, in the same corner.
            "type": "text",
            "left": 56,
            "top": 68,
            "silent": True,
            "style": {"text": pred_obs_annotation(plot_data, labels),
                      "fill": ink, "fontSize": 12, "lineHeight": 16},
        }],
    }
    if title is not None:
        option["title"] = {
            "text": title, "left": "center", "top": 0,
            "textStyle": {"color": ink, "fontSize": 13,
                          "fontWeight": "normal"},
        }
    return option

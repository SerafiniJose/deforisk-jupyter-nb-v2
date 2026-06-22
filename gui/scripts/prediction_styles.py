"""QGIS-faithful colour ramps for model prediction rasters.

The colour stops and value ranges below are translated verbatim from the QGIS
style files in the repo-root ``qgis_layer_style/`` directory:

* ``prob.qml``     — FAR ML models (glm/rf/icar); ``far.misc.rescale`` maps
  probability [0,1] -> [1, 65535].
* ``prob_bm.qml``  — jnr benchmark; values 1001..30999.
* ``prob_mw.qml``  — moving-window; values 1..65535 (low-skewed nodes).

The qml dir is not part of the installed package, so the stops are baked here as
constants rather than parsed at runtime. ``resolve_prediction_style`` builds a
256-entry rio-tiler colormap (``{0..255: (r, g, b, a)}``) by sampling a
matplotlib ``LinearSegmentedColormap`` whose nodes sit at
``(value - vmin) / (vmax - vmin)`` — so pinning ``vmin/vmax`` on the tile layer
reproduces QGIS's value->colour mapping exactly. The colormap is returned as a
matplotlib ``Colormap`` object (not a sampled dict): localtileserver's
``get_leaflet_tile_layer`` accepts it natively while rejecting a plain dict.
"""

from matplotlib.colors import LinearSegmentedColormap, to_rgb

# family group -> qml-derived range + (value, hex) colour nodes (ascending value)
PREDICTION_PALETTES = {
    "far": {  # prob.qml — glm / rf / icar
        "vmin": 1,
        "vmax": 65535,
        "nodata": 0,
        "nodes": [
            (1, "#228b22"),
            (39322, "#ffa500"),
            (52429, "#e31a1c"),
            (65535, "#000000"),
        ],
    },
    "jnr": {  # prob_bm.qml — benchmark
        "vmin": 1001,
        "vmax": 30999,
        "nodata": 0,
        "nodes": [
            (1001, "#196e19"),
            (2000, "#228b22"),
            (10000, "#ffa500"),
            (20000, "#e31a1c"),
            (30999, "#000000"),
        ],
    },
    "mw": {  # prob_mw.qml — moving window
        "vmin": 1,
        "vmax": 65535,
        "nodata": 0,
        "nodes": [
            (1, "#196e19"),
            (2, "#228b22"),
            (200, "#ffa500"),
            (2000, "#e31a1c"),
            (65535, "#000000"),
        ],
    },
}

# model_key family token -> palette group
_FAMILY_GROUP = {
    "glm": "far",
    "rf": "far",
    "icar": "far",
    "jnr": "jnr",
    "mw": "mw",
}

_FALLBACK_GROUP = "far"


def _build_colormap(palette):
    """Build a matplotlib colormap whose nodes sit at the qml value positions.

    Returned as a Colormap OBJECT (not a sampled dict): localtileserver's
    get_leaflet_tile_layer accepts a matplotlib Colormap natively — it samples it
    to 256 entries and registers it server-side (``custom:<hash>``), keeping the
    tile URL short — while ``vmin``/``vmax`` pin the value range. A dict colormap
    is rejected by the installed localtileserver, so we must pass the object.
    """
    vmin, vmax = palette["vmin"], palette["vmax"]
    span = vmax - vmin
    stops = [((value - vmin) / span, to_rgb(hex_)) for value, hex_ in palette["nodes"]]
    return LinearSegmentedColormap.from_list("prediction", stops, N=256)


def resolve_prediction_style(model_key: str) -> dict:
    """Return tile-layer styling for a prediction, keyed by model family.

    ``family = model_key.split("_")[0]``. Unknown families fall back to the FAR
    (prob.qml) palette so display never raises.

    Returns a dict with ``colormap`` (matplotlib Colormap object), ``vmin``,
    ``vmax`` and ``nodata``.
    """
    family = (model_key or "").split("_")[0]
    group = _FAMILY_GROUP.get(family, _FALLBACK_GROUP)
    palette = PREDICTION_PALETTES[group]
    return {
        "colormap": _build_colormap(palette),
        "vmin": palette["vmin"],
        "vmax": palette["vmax"],
        "nodata": palette["nodata"],
    }

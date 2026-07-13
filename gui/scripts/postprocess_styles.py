"""Default map styling for post-process outputs (edge / dist / loss / gain).

Variable styling elsewhere is a name -> ``PREDEFINED_CATALOGUE`` lookup
(``variable_styles.resolve_variable_style``). Processed variables survive that
lookup because alignment preserves the variable's ``name``. Post-process outputs
do not: they are given a *new* name (``forest_gfc_edge``, ``loss_forest_2015_2020``)
because they measure a *new quantity*, so they miss the catalogue and used to fall
through to grayscale (distances) or black/white (change masks).

This module supplies their default look instead, mirroring ``prediction_styles``:
QGIS-derived colour stops baked in as constants, plus a resolver that returns a
matplotlib ``Colormap`` + pinned ``vmin``/``vmax`` for
``localtileserver.get_leaflet_tile_layer`` (which honours a Colormap object and a
pinned value range, unlike ``SepalMap.add_raster``).

Kept free of Solara / ipyvuetify / localtileserver so the mapping stays
unit-testable. Pure and total: unrecognised variables resolve to ``None`` and the
caller keeps its own default.
"""

from typing import Optional

from matplotlib.colors import Colormap, LinearSegmentedColormap, to_rgb

# Colour stops translated verbatim from the repo-root QGIS styles / the change-layer
# value convention. The qml dir is not part of the installed package, so the stops are
# baked here rather than parsed at runtime (same choice as ``prediction_styles``).
POSTPROCESS_PALETTES = {
    # dist_edge.qml — INTERPOLATED, classificationMin=30, classificationMax=1000.
    # Values are METRES: the GDAL proximity call passes DISTUNITS=GEO
    # (spatialrisk/processing.py). Pinned rather than auto-stretched so the same
    # colour means the same distance in every project. >1000 m clamps to green,
    # exactly as QGIS clamps.
    "distance": {
        "vmin": 30,
        "vmax": 1000,
        "nodes": [
            (30, "#e31a1c"),  # at the edge
            (100, "#ffa500"),
            (500, "#ffffb2"),
            (1000, "#228b22"),  # deep interior
        ],
    },
    # generate_change_var writes 1 = event, 0 = stable, 255 = nodata. The event gets
    # the semantic colour; stable stays an opaque neutral so the layer's extent reads.
    # NB this deliberately departs from fcc.qml (0=red, 1=green) — that style is for a
    # forest-cover mask where 1 means *forest*, and applied to a loss mask it would
    # paint deforestation green.
    "loss": {
        "vmin": 0,
        "vmax": 1,
        "nodes": [(0, "#d9d9d9"), (1, "#e31a1c")],
    },
    "gain": {
        "vmin": 0,
        "vmax": 1,
        "nodes": [(0, "#d9d9d9"), (1, "#228b22")],
    },
}

# The steps in PostProcessing (spatialrisk/variables/models.py). Both produce a
# distance raster in metres, so they share one ramp.
_DISTANCE_STEPS = ("edge", "dist")
_CHANGE_OPS = ("loss", "gain")


def classify_postprocess(var) -> Optional[str]:
    """Which post-process output ``var`` is: 'distance', 'loss', 'gain', or None.

    The predicates mirror ``process_actions.postprocess_output_keys`` — the function
    that decides which variables the Post-process tile *lists* — so what is listed and
    what is styled cannot drift apart. Change layers are checked first, matching the
    order there.

    Tags / processing history are the authority; the name suffix and prefix are the
    fallback for legacy variables saved before those fields existed.

    KNOWN LIMITATION (accepted). A variable *can* be both: ``_create_post_var``
    inherits the parent's tags, and the tile's edge/dist picker offers every processed
    variable — so running ``dist`` on a loss layer yields ``loss_..._dist`` carrying
    both ``tags=["loss", "change", ...]`` and ``processing_history=["dist"]``. Because
    change is checked first, that raster classifies as ``loss`` and is drawn with the
    binary 0/1 ramp even though its values are metres. Flipping the order would fix it
    (a change layer never carries edge/dist history or suffix, so the reverse
    false positive is impossible), but the ordering is a deliberate project decision —
    do not change it without raising the question again.
    """
    name = getattr(var, "name", "") or ""
    tags = getattr(var, "tags", None) or []
    history = getattr(var, "processing_history", None) or []

    for op in _CHANGE_OPS:
        if op in tags or name.startswith(f"{op}_"):
            return op

    for step in _DISTANCE_STEPS:
        if step in history or name.endswith(f"_{step}"):
            return "distance"

    return None


def resolve_postprocess_style(var) -> Optional[dict]:
    """Tile-layer styling for a post-process raster, or None if it isn't one.

    Returns ``{"colormap": Colormap, "vmin": float, "vmax": float}``. Both ramps pin
    their range, so no caller needs to auto-stretch. ``None`` means "not a post-process
    output" — the caller keeps whatever default it would otherwise have used.
    """
    kind = classify_postprocess(var)
    if kind is None:
        return None

    palette = POSTPROCESS_PALETTES[kind]
    return {
        "colormap": _ramp(palette["nodes"], palette["vmin"], palette["vmax"], kind),
        "vmin": palette["vmin"],
        "vmax": palette["vmax"],
    }


def _ramp(nodes, vmin: float, vmax: float, name: str) -> Colormap:
    """Build a colormap whose stops land on their values once vmin/vmax are pinned.

    Each node sits at ``(value - vmin) / (vmax - vmin)``, so a tile layer pinned to the
    same ``vmin``/``vmax`` reproduces the QGIS value -> colour mapping exactly. Same
    technique as ``prediction_styles``.
    """
    span = float(vmax - vmin)
    stops = [((value - vmin) / span, to_rgb(hex_)) for value, hex_ in nodes]
    return LinearSegmentedColormap.from_list(f"postprocess_{name}", stops, N=256)

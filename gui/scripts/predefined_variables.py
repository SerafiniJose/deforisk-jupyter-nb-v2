"""Predefined GEE variable catalogue.

Extracted from notebooks/1.variables_factory.ipynb.

Each entry provides a get_image(aoi, year=None) function that returns an ee.Image,
plus metadata (raster_type, temporal flag) for populating the Add Variable modal.
"""

import ee


def get_aoi_ee_feature(gdf):
    """Convert a GeoDataFrame AOI to an ee.Feature for GEE operations."""
    import json

    geojson = json.loads(gdf.dissolve().to_json())
    geometry = ee.Geometry(geojson["features"][0]["geometry"])
    return ee.Feature(geometry)


def resolve_aoi_ee(aoi_result):
    """Resolve an AoiResult to an Earth Engine object for variable extraction.

    AOI selections expose their geometry one of two ways:
      * DRAW / local selections -> a GeoDataFrame in ``aoi_result.gdf``.
      * Admin-boundary / GEE-asset selections -> an ee object in
        ``aoi_result.feature_collection`` (``gdf`` is None — fetched lazily on GEE).

    Prefer the GEE object when present (it covers admin/asset selections, which
    have no local gdf); otherwise convert the local gdf. The returned object
    supports ``.geometry()``/``.clip()``/``.filterBounds()`` either way.
    """
    fc = getattr(aoi_result, "feature_collection", None)
    if fc is not None:
        return fc
    if getattr(aoi_result, "gdf", None) is not None:
        return get_aoi_ee_feature(aoi_result.gdf)
    raise ValueError(
        "Selected AOI has no usable geometry. Re-select the area "
        "(admin boundaries need GEE enabled to fetch their geometry)."
    )


# ---------------------------------------------------------------------------
# Individual get_image functions
# ---------------------------------------------------------------------------


def _get_altitude(aoi, year=None):
    """USGS SRTM 30m elevation."""
    return ee.Image("USGS/SRTMGL1_003").select("elevation").clip(aoi)


def _get_slope(aoi, year=None):
    """Terrain slope derived from SRTM elevation."""
    elevation = ee.Image("USGS/SRTMGL1_003").select("elevation")
    return ee.Terrain.slope(elevation).clip(aoi)


def _get_protected_area(aoi, year=None):
    """WDPA protected areas — binary mask (1 = inside a designated protected area).

    Rasterized by painting the status-filtered polygons onto a 0 background, so it
    depends on no feature attribute. The previous implementation reduced on a
    ``WDPAID`` property that the current ``WCMC/WDPA/current/polygons`` schema no
    longer exposes (it was renamed to ``SITE_ID`` / ``SITE_PID``), which silently
    produced an all-zero raster. ``aoi`` may be a Geometry, Feature, or
    FeatureCollection; ``filterBounds`` requires a Geometry.
    """
    geom = aoi if isinstance(aoi, ee.Geometry) else aoi.geometry()
    wdpa = (
        ee.FeatureCollection("WCMC/WDPA/current/polygons")
        .filterBounds(geom)
        .filter(
            ee.Filter.inList(
                "STATUS", ["Designated", "Inscribed", "Established", "Proposed"]
            )
        )
    )
    return ee.Image(0).paint(wdpa, 1).clip(geom).toByte()


def _get_rivers(aoi, year=None):
    """OSM water layer — binary mask (rivers/streams)."""
    return (
        ee.ImageCollection("projects/sat-io/open-datasets/OSM_waterLayer")
        .filterBounds(aoi)
        .mosaic()
        .clip(aoi)
        .gte(2)
        .unmask()
        .clip(aoi)
        .toByte()
    )


def _get_roads(aoi, year=None):
    """OSM roads — binary mask."""
    return (
        ee.Image(
            "projects/ee-andyarnellgee/assets/crosscutting/infrastructure"
            "/roads_osm/roadsAllImageOSM"
        )
        .unmask()
        .clip(aoi)
        .toByte()
    )


def _get_forest_gfc(aoi, year, tree_cover_threshold=30):
    """Hansen Global Forest Change — forest cover at a given year.

    ``tree_cover_threshold`` is the minimum canopy cover percentage in 2000 that
    counts as forest. It is user-selectable from the Add Variable modal; the
    default here must match the catalogue's declared default (see ``params``
    on the ``forest_gfc`` entry), which is the single source of truth.
    """
    gfc = ee.Image("UMD/hansen/global_forest_change_2024_v1_12").clip(aoi)
    forest2000 = gfc.select("treecover2000")
    forest2000_thr = (
        ee.Image(0).where(forest2000.gte(tree_cover_threshold), 1).clip(aoi)
    )
    loss = gfc.select("lossyear")
    return forest2000_thr.where(loss.lt(year - 2000), 0).rename("B1")


def _get_forest_tmf(aoi, year):
    """JRC Tropical Moist Forest — forest cover at a given year.

    Reads the AnnualChanges ``Dec{year-1}`` band and reduces it to a binary
    forest mask (1 = forest, 0 = non-forest), matching the temporal forest_gfc
    structure. The TMF AnnualChange classes are 1 = undisturbed, 2 = degraded,
    3 = deforested, 4 = regrowth, 5 = water, 6 = other. This expression keeps
    class 1 as forest: ``where(eq(2), 1)`` is overwritten by the subsequent
    ``where(neq(1), 0)`` (which references the original band), so degraded
    pixels resolve to 0 — preserving the behaviour of notebooks/
    1.variables_factory.ipynb verbatim.
    """
    tmf = (
        ee.ImageCollection("projects/JRC/TMF/v1_2024/AnnualChanges")
        .filterBounds(aoi)
        .mosaic()
    )
    band = tmf.select("Dec" + str(year - 1))
    return band.where(band.eq(2), 1).where(band.neq(1), 0).clip(aoi).rename("B1")


def _get_towns(aoi, year):
    """JRC GHSL population + built surface — urban area binary mask."""
    epochs = list(range(1975, 2021, 5))
    epoch = min(epochs, key=lambda x: abs(x - year))
    pop = ee.Image(f"JRC/GHSL/P2023A/GHS_POP/{epoch}")
    built = ee.Image(f"JRC/GHSL/P2023A/GHS_BUILT_S/{epoch}").select("built_surface")
    return ee.Image(0).where(pop.gte(15).And(built.gte(90)), 1).clip(aoi)


def _get_subj(aoi, year=None):
    """FAO GAUL level-2 subjurisdiction — categorical raster."""
    from spatialrisk.gee.ee_fao_gaul import get_fao_gaul_subj
    from spatialrisk.gee.ee_rasterize_unique_values import gee_rasterize_unique_values

    filtered_subj, _ = get_fao_gaul_subj(2, aoi)
    return (
        ee.Image(gee_rasterize_unique_values(filtered_subj, "gaul2_name"))
        .clip(aoi)
        .toByte()
    )


# ---------------------------------------------------------------------------
# Catalogue
# ---------------------------------------------------------------------------

# Map visualization for predefined layers. Consumed by ``_styled_layer`` in
# gui/tile/variables_tile.py:
#   * ``vis_params`` — a GEE visualization dict. A palette with no ``min``/``max``
#     is stretched dynamically to the image's min/max over the AOI. Binary masks
#     render filled solid: ``0`` -> first colour (white), ``1`` -> feature colour.
#   * ``random_visualizer`` — render via ``image.randomVisualizer()`` (one random
#     RGB colour per distinct value); used for multi-class categorical rasters
#     whose value range is arbitrary. Takes precedence over ``vis_params``.
PREDEFINED_CATALOGUE = {
    "altitude": {
        "label_key": "vars.predefined.altitude",
        "description_key": "vars.predefined_info.altitude",
        "var_type": "GEEVar",
        "raster_type": "continuous",
        "temporal": False,
        "get_image": _get_altitude,
        # Terrain ramp (green lowlands -> brown -> white peaks), stretched to AOI.
        "vis_params": {
            "palette": ["006633", "E5FFCC", "662A00", "D8D8D8", "F5F5F5"],
        },
    },
    "slope": {
        "label_key": "vars.predefined.slope",
        "description_key": "vars.predefined_info.slope",
        "var_type": "GEEVar",
        "raster_type": "continuous",
        "temporal": False,
        "get_image": _get_slope,
        # Degrees: green (flat) -> yellow -> red (steep).
        "vis_params": {
            "palette": ["1a9850", "ffffbf", "d73027"],
            "min": 0,
            "max": 60,
        },
    },
    "protected_area": {
        "label_key": "vars.predefined.protected_area",
        "description_key": "vars.predefined_info.protected_area",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_protected_area,
        "vis_params": {"palette": ["ffffff", "4caf50"], "min": 0, "max": 1},
    },
    "rivers": {
        "label_key": "vars.predefined.rivers",
        "description_key": "vars.predefined_info.rivers",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_rivers,
        "vis_params": {"palette": ["ffffff", "2196f3"], "min": 0, "max": 1},
    },
    "roads": {
        "label_key": "vars.predefined.roads",
        "description_key": "vars.predefined_info.roads",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_roads,
        "vis_params": {"palette": ["ffffff", "ff9800"], "min": 0, "max": 1},
    },
    "forest_gfc": {
        "label_key": "vars.predefined.forest_gfc",
        "description_key": "vars.predefined_info.forest_gfc",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(2001, 2025)),
        # User-selectable knobs, rendered generically by the Add Variable modal
        # (same shape as MODEL_REGISTRY["params"] in gui/tile/train_tile.py).
        # ``suffix_prefix`` makes the value part of the variable name
        # (forest_gfc_tc30) so two forest definitions coexist as separate
        # variables — see ``build_predefined_name`` / ``resolve_predefined``.
        "params": [
            {
                "key": "tree_cover_threshold",
                "label_key": "vars.modal.param_tree_cover_threshold",
                "hint_key": "vars.modal.param_tree_cover_threshold_hint",
                "type": "int",
                "default": 30,
                "min": 1,
                "max": 100,
                "suffix_prefix": "tc",
            }
        ],
        "get_image": _get_forest_gfc,
        "vis_params": {"palette": ["ffffff", "2e7d32"], "min": 0, "max": 1},
    },
    "forest_tmf": {
        "label_key": "vars.predefined.forest_tmf",
        "description_key": "vars.predefined_info.forest_tmf",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(2001, 2025)),
        "get_image": _get_forest_tmf,
        "vis_params": {"palette": ["ffffff", "2e7d32"], "min": 0, "max": 1},
    },
    "towns": {
        "label_key": "vars.predefined.towns",
        "description_key": "vars.predefined_info.towns",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(1975, 2021, 5)),
        "get_image": _get_towns,
        "vis_params": {"palette": ["ffffff", "e91e63"], "min": 0, "max": 1},
    },
    "subj": {
        "label_key": "vars.predefined.subj",
        "description_key": "vars.predefined_info.subj",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_subj,
        # Many subjurisdictions with arbitrary rasterized values -> random RGB.
        "random_visualizer": True,
    },
}

PREDEFINED_NAMES = list(PREDEFINED_CATALOGUE.keys())

# ---------------------------------------------------------------------------
# Parameterised layers: name <-> params
# ---------------------------------------------------------------------------
#
# A layer may declare ``params`` (see forest_gfc). The chosen values are baked
# into the variable *name* — "forest_gfc" + {"tree_cover_threshold": 30} ->
# "forest_gfc_tc30" — so the storage key, the downloaded GeoTIFF, the processed
# variable and every derived name carry the parameterisation with no extra
# plumbing, and two settings coexist as separate variables.
#
# Nothing persists the values separately (raw GEEVars are session-only, and the
# Process step drops the ee.Image but keeps the name), so ``resolve_predefined``
# parses them back out of the name. Keep it an exact inverse of
# ``build_predefined_name``.


def param_specs(catalogue_key):
    """Parameter specs declared by a catalogue entry ([] when it has none)."""
    cat = PREDEFINED_CATALOGUE.get(catalogue_key or "")
    return list(cat.get("params", [])) if cat else []


def default_param_values(catalogue_key):
    """``{param key: default}`` for a catalogue entry ({} when unparameterised)."""
    return {spec["key"]: spec["default"] for spec in param_specs(catalogue_key)}


def coerce_param_values(catalogue_key, raw):
    """Validate raw form values for a catalogue entry.

    ``raw`` maps param key -> whatever the form holds (text fields hand back
    strings). Returns ``(values, None)`` with every value coerced to ``int``, or
    ``({}, spec)`` naming the first param that is blank, not a whole number, or
    outside its ``[min, max]`` range — the caller turns that spec into a
    localized message. Only ``int`` params exist today.
    """
    values = {}
    for spec in param_specs(catalogue_key):
        text = str(raw.get(spec["key"], "")).strip()
        try:
            value = int(text)
        except (TypeError, ValueError):
            return {}, spec
        if not (spec["min"] <= value <= spec["max"]):
            return {}, spec
        values[spec["key"]] = value
    return values, None


def build_predefined_name(catalogue_key, values):
    """Variable name for a catalogue key plus its parameter values.

    ``("forest_gfc", {"tree_cover_threshold": 30})`` -> ``"forest_gfc_tc30"``.
    An unparameterised layer keeps its bare key. The year is NOT part of the
    name — ``entry_key`` appends it when forming the storage key.
    """
    parts = [catalogue_key]
    for spec in param_specs(catalogue_key):
        parts.append(f"{spec['suffix_prefix']}{values[spec['key']]}")
    return "_".join(parts)


def resolve_predefined(name):
    """Split a variable name into its catalogue key and parameter values.

    The inverse of ``build_predefined_name``::

        "altitude"                       -> ("altitude", {})
        "forest_gfc_tc30"                -> ("forest_gfc", {"tree_cover_threshold": 30})
        "loss_forest_gfc_tc30_2015_2020" -> (None, {})

    Returns ``(None, {})`` for anything that is not a catalogue variable —
    custom variables and post-process outputs — so they keep falling through to
    their own defaults. Use this instead of ``PREDEFINED_CATALOGUE.get(name)``
    anywhere a *variable* name (rather than a catalogue key) is looked up.
    """
    import re

    if not name:
        return None, {}
    if name in PREDEFINED_CATALOGUE:
        return name, {}

    # Longest key first: a shorter key must never shadow a longer one that also
    # prefixes the name.
    for key in sorted(PREDEFINED_CATALOGUE, key=len, reverse=True):
        specs = param_specs(key)
        if not specs or not name.startswith(f"{key}_"):
            continue
        segments = name[len(key) + 1 :].split("_")
        if len(segments) != len(specs):
            continue
        values = {}
        for spec, segment in zip(specs, segments):
            match = re.fullmatch(rf"{re.escape(spec['suffix_prefix'])}(\d+)", segment)
            if match is None:
                values = None
                break
            values[spec["key"]] = int(match.group(1))
        if values is not None:
            return key, values
    return None, {}

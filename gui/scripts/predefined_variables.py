"""Predefined GEE variable catalogue — extracted from notebooks/1.variables_factory.ipynb.

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


def _get_forest_gfc(aoi, year, tree_cover_threshold=10):
    """Hansen Global Forest Change — forest cover at a given year."""
    gfc = ee.Image("UMD/hansen/global_forest_change_2024_v1_12").clip(aoi)
    forest2000 = gfc.select("treecover2000")
    forest2000_thr = (
        ee.Image(0).where(forest2000.gte(tree_cover_threshold), 1).clip(aoi)
    )
    loss = gfc.select("lossyear")
    return forest2000_thr.where(loss.lt(year - 2000), 0).rename("B1")


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
        "label": "Altitude (SRTM)",
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
        "label": "Slope (SRTM)",
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
        "label": "Protected areas (WDPA)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_protected_area,
        "vis_params": {"palette": ["ffffff", "4caf50"], "min": 0, "max": 1},
    },
    "rivers": {
        "label": "Rivers (OSM)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_rivers,
        "vis_params": {"palette": ["ffffff", "2196f3"], "min": 0, "max": 1},
    },
    "roads": {
        "label": "Roads (OSM)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_roads,
        "vis_params": {"palette": ["ffffff", "ff9800"], "min": 0, "max": 1},
    },
    "forest_gfc": {
        "label": "Forest cover (Hansen GFC)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(2001, 2025)),
        "get_image": _get_forest_gfc,
        "vis_params": {"palette": ["ffffff", "2e7d32"], "min": 0, "max": 1},
    },
    "towns": {
        "label": "Towns / urban areas (JRC GHSL)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(1975, 2021, 5)),
        "get_image": _get_towns,
        "vis_params": {"palette": ["ffffff", "e91e63"], "min": 0, "max": 1},
    },
    "subj": {
        "label": "Subjurisdiction (FAO GAUL)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_subj,
        # Many subjurisdictions with arbitrary rasterized values -> random RGB.
        "random_visualizer": True,
    },
}

PREDEFINED_NAMES = list(PREDEFINED_CATALOGUE.keys())

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
    """WDPA protected areas — binary mask."""
    wdpa = (
        ee.FeatureCollection("WCMC/WDPA/current/polygons")
        .filterBounds(aoi)
        .filter(
            ee.Filter.inList(
                "STATUS", ["Designated", "Inscribed", "Established", "Proposed"]
            )
        )
    )
    return (
        wdpa.reduceToImage(["WDPAID"], ee.Reducer.first())
        .gt(0)
        .unmask()
        .clip(aoi)
        .toByte()
    )


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

PREDEFINED_CATALOGUE = {
    "altitude": {
        "label": "Altitude (SRTM)",
        "var_type": "GEEVar",
        "raster_type": "continuous",
        "temporal": False,
        "get_image": _get_altitude,
    },
    "slope": {
        "label": "Slope (SRTM)",
        "var_type": "GEEVar",
        "raster_type": "continuous",
        "temporal": False,
        "get_image": _get_slope,
    },
    "protected_area": {
        "label": "Protected areas (WDPA)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_protected_area,
    },
    "rivers": {
        "label": "Rivers (OSM)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_rivers,
    },
    "roads": {
        "label": "Roads (OSM)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_roads,
    },
    "forest_gfc": {
        "label": "Forest cover (Hansen GFC)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(2001, 2025)),
        "get_image": _get_forest_gfc,
    },
    "towns": {
        "label": "Towns / urban areas (JRC GHSL)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": True,
        "years": list(range(1975, 2021, 5)),
        "get_image": _get_towns,
    },
    "subj": {
        "label": "Subjurisdiction (FAO GAUL)",
        "var_type": "GEEVar",
        "raster_type": "categorical",
        "temporal": False,
        "get_image": _get_subj,
    },
}

PREDEFINED_NAMES = list(PREDEFINED_CATALOGUE.keys())

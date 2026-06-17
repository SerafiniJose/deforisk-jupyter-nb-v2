"""Shared GEE recipe catalogue.

A registry mapping ``catalogue_key -> resolver(aoi_ee, **params) -> ee.Image |
ee.FeatureCollection``. Promoted out of ``gui/scripts/predefined_variables.py``
and ``notebooks/1.variables_factory.ipynb`` so both the GUI and notebooks resolve
against one source of truth. ``GEEAdapter`` is the only runtime caller.
"""

from typing import Any, Callable, Dict

import ee  # noqa: F401  (module-level so tests can patch spatialrisk.gee.catalogue.ee)
from spatialrisk.gee.ee_fao_gaul import get_fao_gaul_subj
from spatialrisk.gee.ee_rasterize_unique_values import gee_rasterize_unique_values

Resolver = Callable[..., Any]

CATALOGUE: Dict[str, Resolver] = {}


def register(key: str) -> Callable[[Resolver], Resolver]:
    """Decorator: register ``fn`` under ``key`` in the shared catalogue."""

    def _decorator(fn: Resolver) -> Resolver:
        if key in CATALOGUE:
            raise ValueError(f"catalogue key already registered: {key!r}")
        CATALOGUE[key] = fn
        return fn

    return _decorator


def get_resolver(key: str) -> Resolver:
    """Return the resolver registered under ``key``."""
    if key not in CATALOGUE:
        raise KeyError(f"unknown catalogue key: {key!r}")
    return CATALOGUE[key]


# ---------------------------------------------------------------------------
# Terrain + binary-mask resolvers
# ---------------------------------------------------------------------------


@register("altitude")
def _altitude(aoi_ee):
    """USGS SRTM 30m elevation."""
    return ee.Image("USGS/SRTMGL1_003").select("elevation").clip(aoi_ee)


@register("slope")
def _slope(aoi_ee):
    """Terrain slope derived from SRTM elevation (self-contained)."""
    elevation = ee.Image("USGS/SRTMGL1_003").select("elevation")
    return ee.Terrain.slope(elevation).clip(aoi_ee)


@register("protected_area")
def _protected_area(aoi_ee):
    """WDPA protected areas -- binary mask."""
    wdpa = (
        ee.FeatureCollection("WCMC/WDPA/current/polygons")
        .filterBounds(aoi_ee)
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
        .clip(aoi_ee)
        .toByte()
    )


@register("rivers")
def _rivers(aoi_ee):
    """OSM water layer -- binary mask (rivers/streams)."""
    return (
        ee.ImageCollection("projects/sat-io/open-datasets/OSM_waterLayer")
        .filterBounds(aoi_ee)
        .mosaic()
        .clip(aoi_ee)
        .gte(2)
        .unmask()
        .clip(aoi_ee)
        .toByte()
    )


@register("roads")
def _roads(aoi_ee):
    """OSM roads -- binary mask."""
    return (
        ee.Image(
            "projects/ee-andyarnellgee/assets/crosscutting/infrastructure"
            "/roads_osm/roadsAllImageOSM"
        )
        .unmask()
        .clip(aoi_ee)
        .toByte()
    )


# ---------------------------------------------------------------------------
# Forest resolvers (temporal)
# ---------------------------------------------------------------------------


@register("forest_gfc")
def _forest_gfc(aoi_ee, year, tree_cover_threshold=10):
    """Hansen Global Forest Change -- forest cover at a given year."""
    gfc = ee.Image("UMD/hansen/global_forest_change_2024_v1_12").clip(aoi_ee)
    forest2000 = gfc.select("treecover2000")
    forest2000_thr = (
        ee.Image(0).where(forest2000.gte(tree_cover_threshold), 1).clip(aoi_ee)
    )
    loss = gfc.select("lossyear")
    return forest2000_thr.where(loss.lt(year - 2000), 0).rename("B1")


@register("forest_tmf")
def _forest_tmf(aoi_ee, year):
    """JRC TMF AnnualChanges -- forest mask from the Dec{year-1} band."""
    tmf = (
        ee.ImageCollection("projects/JRC/TMF/v1_2024/AnnualChanges")
        .filterBounds(aoi_ee)
        .mosaic()
    )
    band = tmf.select("Dec" + str(year - 1))
    return band.where(band.eq(2), 1).where(band.neq(1), 0).rename("B1")


# ---------------------------------------------------------------------------
# towns (GHSL) + subj (FAO GAUL)
# ---------------------------------------------------------------------------


@register("towns")
def _towns(aoi_ee, year):
    """JRC GHSL population + built surface -- urban-area binary mask."""
    epochs = list(range(1975, 2021, 5))
    epoch = min(epochs, key=lambda x: abs(x - year))
    pop = ee.Image(f"JRC/GHSL/P2023A/GHS_POP/{epoch}")
    built = ee.Image(f"JRC/GHSL/P2023A/GHS_BUILT_S/{epoch}").select("built_surface")
    return ee.Image(0).where(pop.gte(15).And(built.gte(90)), 1).clip(aoi_ee)


@register("subj")
def _subj(aoi_ee):
    """FAO GAUL level-2 subjurisdiction -- categorical raster."""
    filtered_subj, _ = get_fao_gaul_subj(2, aoi_ee)
    return (
        ee.Image(gee_rasterize_unique_values(filtered_subj, "gaul2_name"))
        .clip(aoi_ee)
        .toByte()
    )

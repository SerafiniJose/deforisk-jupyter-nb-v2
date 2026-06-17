"""Shared GEE recipe catalogue.

A registry mapping ``catalogue_key -> resolver(aoi_ee, **params) -> ee.Image |
ee.FeatureCollection``. Promoted out of ``gui/scripts/predefined_variables.py``
and ``notebooks/1.variables_factory.ipynb`` so both the GUI and notebooks resolve
against one source of truth. ``GEEAdapter`` is the only runtime caller.
"""

from typing import Any, Callable, Dict

import ee  # noqa: F401  (module-level so tests can patch spatialrisk.gee.catalogue.ee)

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

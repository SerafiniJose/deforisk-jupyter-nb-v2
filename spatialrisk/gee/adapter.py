# spatialrisk/gee/adapter.py
"""GEEAdapter -- the ONLY runtime ``import ee`` outside the catalogue.

Resolves a frozen ``GEERecipe`` into a live ee object (``build_image``) and
materializes it to a local file (``materialize``). Raster export reuses
``download_ee_image`` (geedim); vector export is geemap-free.
"""

from typing import Optional, Union

import ee  # noqa: F401  (module-level so tests can patch spatialrisk.gee.adapter.ee)

from spatialrisk.document import GeoJSONGeometry


class GEEAdapter:
    """Stateless adapter: GeoJSON/recipe in, ee object / file path out."""

    def aoi_to_ee(
        self,
        aoi: Optional[GeoJSONGeometry],
        as_feature: bool = False,
    ) -> Optional[Union["ee.Geometry", "ee.Feature"]]:
        """Rebuild an ``ee.Geometry`` (or ``ee.Feature``) from a GeoJSON dict."""
        if aoi is None:
            return None
        geometry = ee.Geometry(aoi)
        if as_feature:
            return ee.Feature(geometry)
        return geometry

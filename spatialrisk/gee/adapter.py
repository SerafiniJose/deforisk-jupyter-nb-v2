# spatialrisk/gee/adapter.py
"""GEEAdapter -- the ONLY runtime ``import ee`` outside the catalogue.

Resolves a frozen ``GEERecipe`` into a live ee object (``build_image``) and
materializes it to a local file (``materialize``). Raster export reuses
``download_ee_image`` (geedim); vector export is geemap-free.
"""

from typing import Optional, Union

import ee  # noqa: F401  (module-level so tests can patch spatialrisk.gee.adapter.ee)

from spatialrisk.document import AssetRecipe, CatalogueRecipe, GEERecipe, GeoJSONGeometry
from spatialrisk.gee.catalogue import get_resolver


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

    def build_image(self, recipe: GEERecipe) -> Union["ee.Image", "ee.FeatureCollection"]:
        """Resolve a frozen recipe into a live ee object."""
        if isinstance(recipe, CatalogueRecipe):
            # aoi_fao_gaul *produces* the AOI -> no geometry needed; others take one.
            as_feature = recipe.catalogue_key in {"protected_area"}
            aoi_ee = self.aoi_to_ee(recipe.aoi, as_feature=as_feature)
            resolver = get_resolver(recipe.catalogue_key)
            return resolver(aoi_ee, **recipe.params)

        if isinstance(recipe, AssetRecipe):
            if recipe.export_kind == "vector":
                return ee.FeatureCollection(recipe.asset_id)
            image = ee.Image(recipe.asset_id)
            if recipe.band is not None:
                image = image.select(recipe.band)
            return image

        raise TypeError(f"unsupported recipe type: {type(recipe).__name__}")

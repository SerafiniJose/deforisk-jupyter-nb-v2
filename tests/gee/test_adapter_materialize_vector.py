# tests/gee/test_adapter_materialize_vector.py
"""GEEAdapter vector export is geemap-free (getInfo -> GeoDataFrame.to_file)."""

import sys
from unittest.mock import MagicMock

import pytest

from spatialrisk.document import AssetRecipe

GEOM = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.adapter.ee", m)
    return m


def test_vector_export_uses_getinfo_and_geopandas_no_geemap(
    fake_ee, monkeypatch
):
    from spatialrisk.gee import adapter as adapter_mod
    from spatialrisk.gee.adapter import GEEAdapter

    # Build a FeatureCollection whose getInfo returns a GeoJSON FeatureCollection.
    fc = MagicMock(name="fc")
    fc.getInfo.return_value = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": GEOM,
                "properties": {"gaul0_name": "X", "iso3_code": "XXX"},
            }
        ],
    }
    monkeypatch.setattr(adapter_mod.GEEAdapter, "build_image", lambda self, r: fc)

    # Patch geopandas so .to_file is observable and no real I/O happens.
    fake_gpd = MagicMock(name="geopandas")
    gdf = MagicMock(name="gdf")
    fake_gpd.GeoDataFrame.from_features.return_value = gdf
    monkeypatch.setitem(sys.modules, "geopandas", fake_gpd)

    # geemap must NOT be imported by the export path.
    monkeypatch.setitem(sys.modules, "geemap", None)

    recipe = AssetRecipe(
        source="asset",
        asset_id="users/me/boundaries",
        aoi=GEOM,
        export_kind="vector",
        vector_selectors=("gaul0_name", "iso3_code"),
    )
    out = GEEAdapter().materialize(recipe, "/tmp/boundaries.shp")

    assert out == "/tmp/boundaries.shp"
    # selectors propagate to getInfo via fc.select(...).getInfo()
    fc.select.assert_called_once_with(["gaul0_name", "iso3_code"])
    fc.select.return_value.getInfo.assert_called_once_with()
    fake_gpd.GeoDataFrame.from_features.assert_called_once()
    gdf.to_file.assert_called_once_with("/tmp/boundaries.shp")


def test_vector_export_without_selectors_calls_getinfo_directly(
    fake_ee, monkeypatch
):
    from spatialrisk.gee import adapter as adapter_mod
    from spatialrisk.gee.adapter import GEEAdapter

    fc = MagicMock(name="fc")
    fc.getInfo.return_value = {"type": "FeatureCollection", "features": []}
    monkeypatch.setattr(adapter_mod.GEEAdapter, "build_image", lambda self, r: fc)

    fake_gpd = MagicMock(name="geopandas")
    fake_gpd.GeoDataFrame.from_features.return_value = MagicMock()
    monkeypatch.setitem(sys.modules, "geopandas", fake_gpd)

    recipe = AssetRecipe(
        source="asset",
        asset_id="users/me/boundaries",
        aoi=GEOM,
        export_kind="vector",
    )
    GEEAdapter().materialize(recipe, "/tmp/b.shp")

    fc.getInfo.assert_called_once_with()
    fc.select.assert_not_called()

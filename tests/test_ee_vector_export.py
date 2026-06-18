import sys
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point


def test_download_ee_vector_uses_getinfo_not_geemap(tmp_path, monkeypatch):
    """The helper writes a vector file from fc.getInfo() with no geemap import."""
    # Hard-fail if anything tries to import geemap.
    monkeypatch.setitem(sys.modules, "geemap", None)

    from spatialrisk.gee import ee_raster_export

    class _FakeFC:
        def select(self, selectors):
            assert selectors == ["gaul0_name", "iso3_code"]
            return self

        def getInfo(self):
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                        "properties": {"gaul0_name": "Foo", "iso3_code": "FOO"},
                    }
                ],
            }

    out = tmp_path / "aoi.shp"
    result = ee_raster_export.download_ee_vector(
        _FakeFC(),
        out,
        selectors=["gaul0_name", "iso3_code"],
    )

    assert Path(result).exists()
    gdf = gpd.read_file(result)
    assert len(gdf) == 1
    assert gdf.iloc[0]["gaul0_name"] == "Foo"
    assert gdf.iloc[0].geometry.equals(Point(1.0, 2.0))


def test_download_ee_vector_no_selectors_skips_select(tmp_path):
    from spatialrisk.gee import ee_raster_export

    class _FakeFC:
        def getInfo(self):
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                        "properties": {"a": 1},
                    }
                ],
            }

    out = tmp_path / "all.shp"
    result = ee_raster_export.download_ee_vector(_FakeFC(), out, selectors=None)
    gdf = gpd.read_file(result)
    assert len(gdf) == 1

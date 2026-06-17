# tests/gee/test_adapter_to_gee_var.py
"""to_gee_var relocated onto GEEAdapter; ee leaves the variable modules."""

import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.adapter.ee", m)
    return m


def test_to_gee_var_vector_reads_file_and_builds_fc(fake_ee, monkeypatch, tmp_path):
    from spatialrisk.gee.adapter import GEEAdapter

    shp = tmp_path / "v.shp"
    shp.write_text("stub")

    fake_gpd = MagicMock(name="geopandas")
    gdf = MagicMock(name="gdf")
    gdf.to_json.return_value = '{"type":"FeatureCollection","features":[]}'
    fake_gpd.read_file.return_value = gdf
    monkeypatch.setitem(sys.modules, "geopandas", fake_gpd)

    out = GEEAdapter().to_gee_var_vector(str(shp))

    fake_gpd.read_file.assert_called_once_with(str(shp))
    fake_ee.FeatureCollection.assert_called_once()
    assert out is fake_ee.FeatureCollection.return_value


def test_to_gee_var_vector_missing_file_raises(fake_ee):
    from spatialrisk.gee.adapter import GEEAdapter

    with pytest.raises(FileNotFoundError):
        GEEAdapter().to_gee_var_vector("/nonexistent/v.shp")


def test_to_gee_var_raster_not_implemented(fake_ee, tmp_path):
    from spatialrisk.gee.adapter import GEEAdapter

    tif = tmp_path / "r.tif"
    tif.write_text("stub")
    with pytest.raises(NotImplementedError):
        GEEAdapter().to_gee_var_raster(str(tif))

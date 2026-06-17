# tests/gee/test_adapter_materialize_raster.py
"""GEEAdapter.materialize raster path calls download_ee_image, never geedim/net."""

from unittest.mock import MagicMock

import pytest

from spatialrisk.document import CatalogueRecipe

GEOM = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.adapter.ee", m)
    return m


def test_materialize_raster_calls_download_with_contract(fake_ee, monkeypatch):
    from spatialrisk.gee import adapter as adapter_mod
    from spatialrisk.gee.adapter import GEEAdapter

    image = MagicMock(name="ee_image")
    monkeypatch.setattr(adapter_mod.GEEAdapter, "build_image", lambda self, r: image)
    download = MagicMock(name="download_ee_image")
    monkeypatch.setattr(adapter_mod, "download_ee_image", download)

    recipe = CatalogueRecipe(
        source="catalogue",
        catalogue_key="altitude",
        aoi=GEOM,
        scale=30.0,
        crs="EPSG:4326",
        export_kind="raster",
    )
    out = GEEAdapter().materialize(recipe, "/tmp/altitude.tif")

    assert out == "/tmp/altitude.tif"
    download.assert_called_once()
    _, kwargs = download.call_args
    assert kwargs["scale"] == 30.0
    assert kwargs["crs"] == "EPSG:4326"
    assert kwargs["unmask_value"] == 255
    assert kwargs["nodata_value"] == 255
    # region is the ee.Geometry rebuilt from recipe.aoi
    assert kwargs["region"] is fake_ee.Geometry.return_value
    # the image positionally first, filename second
    assert download.call_args.args[0] is image
    assert download.call_args.args[1] == "/tmp/altitude.tif"


def test_materialize_raster_defaults_scale_and_crs(fake_ee, monkeypatch):
    from spatialrisk.gee import adapter as adapter_mod
    from spatialrisk.gee.adapter import GEEAdapter

    monkeypatch.setattr(
        adapter_mod.GEEAdapter, "build_image", lambda self, r: MagicMock()
    )
    download = MagicMock()
    monkeypatch.setattr(adapter_mod, "download_ee_image", download)

    recipe = CatalogueRecipe(
        source="catalogue",
        catalogue_key="altitude",
        aoi=GEOM,
        export_kind="raster",
    )
    GEEAdapter().materialize(recipe, "/tmp/x.tif")

    _, kwargs = download.call_args
    assert kwargs["scale"] == 30
    assert kwargs["crs"] == "EPSG:4326"

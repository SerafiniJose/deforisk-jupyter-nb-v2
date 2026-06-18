"""GEEAdapter.build_image dispatches catalogue vs asset recipes."""

from unittest.mock import MagicMock

import pytest

from spatialrisk.document import AssetRecipe, CatalogueRecipe

GEOM = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.adapter.ee", m)
    return m


def test_build_image_catalogue_calls_resolver_with_geometry(fake_ee, monkeypatch):
    from spatialrisk.gee import adapter as adapter_mod
    from spatialrisk.gee.adapter import GEEAdapter

    resolver = MagicMock(return_value=MagicMock(name="ee_image"))
    monkeypatch.setattr(
        adapter_mod, "get_resolver", MagicMock(return_value=resolver)
    )

    recipe = CatalogueRecipe(
        source="catalogue",
        catalogue_key="forest_gfc",
        params={"year": 2020, "tree_cover_threshold": 10},
        aoi=GEOM,
        export_kind="raster",
    )
    out = GEEAdapter().build_image(recipe)

    # resolver is invoked with the ee.Geometry rebuilt from recipe.aoi + params
    resolver.assert_called_once_with(
        fake_ee.Geometry.return_value, year=2020, tree_cover_threshold=10
    )
    assert out is resolver.return_value


def test_build_image_asset_raster_selects_band(fake_ee):
    from spatialrisk.gee.adapter import GEEAdapter

    recipe = AssetRecipe(
        source="asset",
        asset_id="users/me/my_layer",
        band="B4",
        aoi=GEOM,
        export_kind="raster",
    )
    out = GEEAdapter().build_image(recipe)

    fake_ee.Image.assert_called_once_with("users/me/my_layer")
    fake_ee.Image.return_value.select.assert_called_once_with("B4")
    assert out is fake_ee.Image.return_value.select.return_value


def test_build_image_asset_vector_uses_feature_collection(fake_ee):
    from spatialrisk.gee.adapter import GEEAdapter

    recipe = AssetRecipe(
        source="asset",
        asset_id="users/me/boundaries",
        aoi=GEOM,
        export_kind="vector",
    )
    out = GEEAdapter().build_image(recipe)

    fake_ee.FeatureCollection.assert_called_once_with("users/me/boundaries")
    assert out is fake_ee.FeatureCollection.return_value

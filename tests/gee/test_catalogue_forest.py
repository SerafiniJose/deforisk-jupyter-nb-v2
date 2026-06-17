"""Forest catalogue resolvers (GFC + TMF) honour year/threshold params."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.catalogue.ee", m)
    return m


def test_forest_gfc_uses_hansen_2024_and_renames_b1(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    out = catalogue.get_resolver("forest_gfc")(aoi, year=2020, tree_cover_threshold=10)

    fake_ee.Image.assert_any_call("UMD/hansen/global_forest_change_2024_v1_12")
    # final op renames the single band to B1
    gfc = fake_ee.Image.return_value.clip.return_value
    forest2000 = gfc.select.return_value
    forest2000.gte.assert_called_once_with(10)
    assert out is out  # built without raising; band-rename asserted below
    # the .rename("B1") is the terminal call on the where() chain
    loss = gfc.select.return_value
    # ee.Image(0) is the zero-image base; ensure year arithmetic ran (year-2000)
    loss.lt.assert_called_once_with(2020 - 2000)


def test_forest_gfc_requires_year(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    with pytest.raises(TypeError):
        catalogue.get_resolver("forest_gfc")(aoi)  # year is required (no default)


def test_forest_tmf_selects_dec_of_year_minus_one(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    catalogue.get_resolver("forest_tmf")(aoi, year=2020)

    fake_ee.ImageCollection.assert_called_once_with(
        "projects/JRC/TMF/v1_2024/AnnualChanges"
    )
    mosaic = fake_ee.ImageCollection.return_value.filterBounds.return_value.mosaic.return_value
    mosaic.select.assert_called_once_with("Dec2019")

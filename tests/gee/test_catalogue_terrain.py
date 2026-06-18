"""Terrain + binary-mask catalogue resolvers build the right ee assets."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_ee(monkeypatch):
    """Patch the module-level ee with a MagicMock; chained calls auto-return mocks."""
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.catalogue.ee", m)
    return m


def test_altitude_selects_srtm_elevation_and_clips(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    out = catalogue.get_resolver("altitude")(aoi)

    fake_ee.Image.assert_called_once_with("USGS/SRTMGL1_003")
    fake_ee.Image.return_value.select.assert_called_once_with("elevation")
    elevation = fake_ee.Image.return_value.select.return_value
    elevation.clip.assert_called_once_with(aoi)
    assert out is elevation.clip.return_value


def test_slope_is_self_contained_recomputes_elevation(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    out = catalogue.get_resolver("slope")(aoi)

    fake_ee.Image.assert_called_once_with("USGS/SRTMGL1_003")
    elevation = fake_ee.Image.return_value.select.return_value
    fake_ee.Terrain.slope.assert_called_once_with(elevation)
    slope = fake_ee.Terrain.slope.return_value
    slope.clip.assert_called_once_with(aoi)
    assert out is slope.clip.return_value


def test_protected_area_filters_wdpa_status(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    catalogue.get_resolver("protected_area")(aoi)

    fake_ee.FeatureCollection.assert_called_once_with("WCMC/WDPA/current/polygons")
    fake_ee.Filter.inList.assert_called_once_with(
        "STATUS", ["Designated", "Inscribed", "Established", "Proposed"]
    )


def test_rivers_uses_osm_waterlayer_collection(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    catalogue.get_resolver("rivers")(aoi)

    fake_ee.ImageCollection.assert_called_once_with(
        "projects/sat-io/open-datasets/OSM_waterLayer"
    )


def test_roads_uses_andyarnell_asset(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    catalogue.get_resolver("roads")(aoi)

    fake_ee.Image.assert_called_once_with(
        "projects/ee-andyarnellgee/assets/crosscutting/infrastructure"
        "/roads_osm/roadsAllImageOSM"
    )

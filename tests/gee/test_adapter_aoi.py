# tests/gee/test_adapter_aoi.py
"""GEEAdapter rebuilds ee.Geometry / ee.Feature from GeoJSON dicts."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.adapter.ee", m)
    return m


def test_aoi_to_ee_geometry_builds_ee_geometry(fake_ee):
    from spatialrisk.gee.adapter import GEEAdapter

    geom = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
    out = GEEAdapter().aoi_to_ee(geom, as_feature=False)

    fake_ee.Geometry.assert_called_once_with(geom)
    assert out is fake_ee.Geometry.return_value


def test_aoi_to_ee_feature_wraps_geometry(fake_ee):
    from spatialrisk.gee.adapter import GEEAdapter

    geom = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
    out = GEEAdapter().aoi_to_ee(geom, as_feature=True)

    fake_ee.Feature.assert_called_once_with(fake_ee.Geometry.return_value)
    assert out is fake_ee.Feature.return_value


def test_aoi_to_ee_none_returns_none(fake_ee):
    from spatialrisk.gee.adapter import GEEAdapter

    assert GEEAdapter().aoi_to_ee(None) is None
    fake_ee.Geometry.assert_not_called()

"""aoi_fao_gaul resolver returns the FAO GAUL FeatureCollection for level/iso."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.catalogue.ee", m)
    return m


def test_aoi_fao_gaul_passes_level_and_iso(fake_ee, monkeypatch):
    from spatialrisk.gee import catalogue

    fc = MagicMock(name="aoi_fc")
    get_features = MagicMock(return_value=fc)
    monkeypatch.setattr(catalogue, "get_fao_gaul_features", get_features)

    # aoi_ee is ignored for this source recipe (it *produces* the AOI)
    out = catalogue.get_resolver("aoi_fao_gaul")(None, level=0, iso="MTQ")

    get_features.assert_called_once_with(level=0, code="MTQ")
    assert out is fc


def test_aoi_fao_gaul_defaults_to_level_0(fake_ee, monkeypatch):
    from spatialrisk.gee import catalogue

    get_features = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(catalogue, "get_fao_gaul_features", get_features)

    catalogue.get_resolver("aoi_fao_gaul")(None, iso="BRA")

    get_features.assert_called_once_with(level=0, code="BRA")

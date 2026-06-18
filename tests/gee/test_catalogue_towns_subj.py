"""towns (GHSL closest epoch) + subj (FAO GAUL L2 rasterized) resolvers."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_ee(monkeypatch):
    m = MagicMock(name="ee")
    monkeypatch.setattr("spatialrisk.gee.catalogue.ee", m)
    return m


def test_towns_picks_closest_epoch_pop_and_built(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    # 2018 -> closest epoch is 2020
    catalogue.get_resolver("towns")(aoi, year=2018)

    fake_ee.Image.assert_any_call("JRC/GHSL/P2023A/GHS_POP/2020")
    fake_ee.Image.assert_any_call("JRC/GHSL/P2023A/GHS_BUILT_S/2020")


def test_towns_epoch_rounds_down_on_tie_to_earlier(fake_ee):
    from spatialrisk.gee import catalogue

    aoi = MagicMock(name="aoi")
    # 2017 -> 2015 and 2020 are equidistant; min(..., key=abs) picks 2015 (earlier)
    catalogue.get_resolver("towns")(aoi, year=2017)

    fake_ee.Image.assert_any_call("JRC/GHSL/P2023A/GHS_POP/2015")


def test_subj_calls_fao_gaul_and_rasterizer(fake_ee, monkeypatch):
    from spatialrisk.gee import catalogue

    filtered = MagicMock(name="filtered_subj")
    get_subj = MagicMock(return_value=(filtered, "gaul2_name"))
    rasterize = MagicMock(return_value=MagicMock(name="rasterized"))
    monkeypatch.setattr(catalogue, "get_fao_gaul_subj", get_subj)
    monkeypatch.setattr(catalogue, "gee_rasterize_unique_values", rasterize)

    aoi = MagicMock(name="aoi")
    catalogue.get_resolver("subj")(aoi)

    get_subj.assert_called_once_with(2, aoi)
    rasterize.assert_called_once_with(filtered, "gaul2_name")
    fake_ee.Image.assert_called_once_with(rasterize.return_value)

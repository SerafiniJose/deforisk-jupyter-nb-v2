"""All ~10 inventory keys are registered and exported from spatialrisk.gee."""

import sys
from unittest.mock import MagicMock


def test_all_inventory_keys_registered(monkeypatch):
    # importing the catalogue must not require a live ee session
    monkeypatch.setitem(sys.modules, "ee", MagicMock())
    from spatialrisk.gee import catalogue

    expected = {
        "altitude",
        "slope",
        "protected_area",
        "rivers",
        "roads",
        "forest_gfc",
        "forest_tmf",
        "towns",
        "subj",
        "aoi_fao_gaul",
    }
    assert expected <= set(catalogue.CATALOGUE)


def test_package_exports_catalogue_and_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "ee", MagicMock())
    from spatialrisk.gee import CATALOGUE, GEEAdapter, get_resolver  # noqa: F401

    assert callable(get_resolver)

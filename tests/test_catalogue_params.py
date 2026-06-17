import inspect

from spatialrisk.gee.catalogue import get_resolver


def _params(key):
    sig = inspect.signature(get_resolver(key))
    return [p.name for p in sig.parameters.values()]


def test_subj_resolver_takes_only_aoi():
    assert _params("subj") == ["aoi_ee"]


def test_forest_gfc_params():
    assert _params("forest_gfc") == ["aoi_ee", "year", "tree_cover_threshold"]


def test_forest_tmf_params():
    assert _params("forest_tmf") == ["aoi_ee", "year"]


def test_aoi_fao_gaul_params():
    assert _params("aoi_fao_gaul") == ["aoi_ee", "iso", "level"]

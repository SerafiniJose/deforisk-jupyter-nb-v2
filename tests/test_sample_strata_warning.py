"""is_continuous_strata(): warn when a continuous raster is used as strata."""
from types import SimpleNamespace

import pytest

from spatialrisk.variables.models import RasterType
from gui.widget.sample_form_dialog import is_continuous_strata


def _project(**vars_):
    return SimpleNamespace(processed_variables=vars_)


def _var(raster_type=None):
    return SimpleNamespace(data_type="raster", raster_type=raster_type)


@pytest.mark.parametrize("rt", [RasterType.continuous, "continuous"])
def test_continuous_strata_warns(rt):
    p = _project(altitude=_var(rt))
    assert is_continuous_strata(p, "stratified", "altitude") is True


@pytest.mark.parametrize("rt", [RasterType.categorical, "categorical"])
def test_categorical_strata_does_not_warn(rt):
    p = _project(landcover=_var(rt))
    assert is_continuous_strata(p, "stratified", "landcover") is False


@pytest.mark.parametrize("strategy", ["random", "systematic"])
def test_non_stratified_never_warns(strategy):
    """Other strategies ignore pixel values entirely — nothing to warn about."""
    p = _project(altitude=_var(RasterType.continuous))
    assert is_continuous_strata(p, strategy, "altitude") is False


def test_unset_raster_type_does_not_warn():
    """Metadata is optional; an unset type must not cry wolf on every variable."""
    p = _project(mystery=_var(None))
    assert is_continuous_strata(p, "stratified", "mystery") is False


def test_variable_without_the_attribute_does_not_warn():
    p = SimpleNamespace(processed_variables={"v": SimpleNamespace(data_type="raster")})
    assert is_continuous_strata(p, "stratified", "v") is False


def test_unknown_or_empty_variable_does_not_warn():
    p = _project(altitude=_var(RasterType.continuous))
    assert is_continuous_strata(p, "stratified", "nope") is False
    assert is_continuous_strata(p, "stratified", "") is False


def test_missing_project_does_not_warn():
    assert is_continuous_strata(None, "stratified", "altitude") is False

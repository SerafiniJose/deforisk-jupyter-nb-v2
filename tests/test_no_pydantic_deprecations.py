import warnings
import pytest


def test_variable_construction_has_no_pydantic_deprecation():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        import importlib, spatialrisk.variables.variable as v
        importlib.reload(v)

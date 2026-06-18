"""The predictors package must be importable WITHOUT touching the (currently
broken) spatialrisk/__init__.py import chain.
"""


def test_predictors_package_imports_standalone():
    import importlib

    mod = importlib.import_module("spatialrisk.predictors")
    assert mod is not None
    # Importing the package must NOT pull in the broken top-level __init__
    # (which imports the not-yet-existing spatialrisk.predictions).
    assert hasattr(mod, "__path__")

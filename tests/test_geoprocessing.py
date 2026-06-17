"""Phase 6: stateless variable-geoprocessing functions.

Each function takes an EXPLICIT out_path + input spec + base/geobox and RETURNS
a new VariableSpec. No function references a live Project. Numeric/geospatial
behavior is the verbatim current path (geo_utils / processing primitives).
"""
import importlib


def test_geoprocessing_module_imports():
    mod = importlib.import_module("spatialrisk.geoprocessing")
    # The four stateless seams this phase delivers.
    assert callable(mod.reproject_and_match)
    assert callable(mod.rasterize_vector)
    assert callable(mod.apply_post_processing)


def test_geoprocessing_does_not_import_project_or_ee():
    """Leaf module: no live-Project reach-through, no runtime ee import."""
    import sys

    sys.modules.pop("spatialrisk.geoprocessing", None)
    importlib.import_module("spatialrisk.geoprocessing")
    mod = sys.modules["spatialrisk.geoprocessing"]
    src = mod.__file__
    text = open(src).read()
    assert "self.project" not in text
    assert ".project.folders" not in text
    assert "import ee" not in text

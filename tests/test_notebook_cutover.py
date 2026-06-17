"""Phase 8 cutover guards for notebooks/1.variables_factory.ipynb.

These tests parse the notebook source (no execution) and assert that every
legacy Project/GEEVar call site has been replaced by the Session API, and that
the new cell sources are syntactically valid Python.
"""

import ast
from pathlib import Path

import nbformat

REPO_ROOT = Path(__file__).resolve().parent.parent
NB_PATH = REPO_ROOT / "notebooks" / "1.variables_factory.ipynb"


def _code_cells():
    nb = nbformat.read(NB_PATH, as_version=4)
    return [c.source for c in nb.cells if c.cell_type == "code"]


def _all_code():
    return "\n".join(_code_cells())


def test_notebook_code_cells_parse_as_python():
    """Every code cell must be syntactically valid Python (minus magics)."""
    for src in _code_cells():
        # Strip Jupyter line magics / shell escapes so ast.parse accepts the cell.
        cleaned = "\n".join(
            line for line in src.splitlines()
            if not line.lstrip().startswith(("%", "!"))
        )
        ast.parse(cleaned)


def test_notebook_uses_session_create_not_project():
    code = _all_code()
    assert "ProjectSession.create(" in code
    assert "Project(project_name=" not in code
    assert "from spatialrisk.project import Project" not in code


def test_notebook_has_no_live_ee_or_geevar_calls():
    code = _all_code()
    # No live Earth Engine objects constructed in the notebook anymore.
    assert "GEEVar(" not in code
    assert "gee_images=" not in code
    assert "ee.Image(" not in code
    assert "ee.ImageCollection(" not in code
    assert "ee.FeatureCollection(" not in code
    assert ".to_local_raster(" not in code
    assert ".to_local_vector(" not in code
    assert ".add_as_raw(" not in code


def test_notebook_uses_catalogue_recipes_for_all_layers():
    code = _all_code()
    assert "session.add_gee_variable(" in code
    assert "CatalogueRecipe(" in code
    # Every catalogue key the old notebook covered must appear by name.
    for key in (
        "aoi_fao_gaul", "subj", "protected_area", "altitude", "slope",
        "forest_gfc", "rivers", "roads", "towns",
    ):
        assert f'catalogue_key="{key}"' in code, f"missing recipe: {key}"


def test_notebook_imports_ee_for_auth():
    code = _all_code()
    assert "import ee" in code
    # auth cells remain (live-EE; documented credential limitation)
    assert "ee.Authenticate()" in code
    assert "ee.Initialize(" in code


def test_notebook_no_set_aoi_from_variable():
    code = _all_code()
    assert "set_aoi_from_variable" not in code
    assert "session.set_aoi(" in code


def test_notebook_recipe_params_match_resolvers():
    import inspect
    from spatialrisk.gee.catalogue import get_resolver

    code = _all_code()
    tree = ast.parse("\n".join(
        l for l in code.splitlines() if not l.lstrip().startswith(("%", "!"))))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "CatalogueRecipe":
            kws = {k.arg: k.value for k in node.keywords}
            if "catalogue_key" not in kws or not isinstance(kws["catalogue_key"], ast.Constant):
                continue
            key = kws["catalogue_key"].value
            params_node = kws.get("params")
            param_names = set()
            if isinstance(params_node, ast.Dict):
                param_names = {k.value for k in params_node.keys if isinstance(k, ast.Constant)}
            accepted = {p.name for p in inspect.signature(get_resolver(key)).parameters.values()
                        if p.name != "aoi_ee"}
            assert param_names <= accepted, f"{key}: {param_names - accepted} rejected by resolver"


def test_notebook_wires_store_and_base_raster():
    code = _all_code()
    assert "ProjectSession.create(" in code and "store=" in code
    assert "set_base_raster(" in code
    # commented examples are API-correct: spec objects, not name=/path= kwargs
    assert "add_local_vector(name=" not in code
    assert "add_local_raster(name=" not in code

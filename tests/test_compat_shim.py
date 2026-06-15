"""Tests for the ``component.script`` -> ``spatialrisk`` backward-compat shim.

The package was renamed (``git mv component/script -> spatialrisk``; every
submodule path preserved 1:1) but the notebooks and published docs still import
from the old dotted path. The shim must transparently redirect every
``component.script.<sub>`` import to ``spatialrisk.<sub>`` and re-export the
top-level public API, so existing notebooks import unchanged.
"""

import ast
import importlib
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"


def test_component_script_package_is_importable():
    import component.script  # noqa: F401


def test_submodule_redirects_to_the_same_spatialrisk_module():
    for sub in [
        "dataset",
        "project",
        "sampling",
        "processing",
        "far_helpers",
        "variables",
        "variables.models",
        "gee.ee_fao_gaul",
        "utilities.file_helpers",
    ]:
        old = importlib.import_module(f"component.script.{sub}")
        new = importlib.import_module(f"spatialrisk.{sub}")
        assert old is new, f"component.script.{sub} should BE spatialrisk.{sub}"


def test_top_level_public_api_reexported():
    import spatialrisk
    from component.script import (  # noqa: F401
        Dataset,
        GLMModel,
        ICARModel,
        JNRBenchmarkModel,
        MWModel,
        Project,
        RFModel,
        Sampling,
        SamplingStrategy,
        rmj,
    )

    assert Project is spatialrisk.Project
    assert GLMModel is spatialrisk.GLMModel
    assert rmj is spatialrisk.rmj


def _component_import_statements(nb_path):
    """Return canonical source for every ``component`` import in a notebook."""
    nb = json.loads(nb_path.read_text())
    statements = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        # Blank out IPython magics / shell escapes so ``ast.parse`` succeeds.
        cleaned = "\n".join(
            "" if ln.lstrip().startswith(("%", "!", "?")) else ln
            for ln in src.splitlines()
        )
        try:
            tree = ast.parse(cleaned)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "component"
            ):
                statements.append(ast.unparse(node))
            elif isinstance(node, ast.Import) and any(
                a.name.startswith("component") for a in node.names
            ):
                statements.append(ast.unparse(node))
    return statements


@pytest.mark.parametrize(
    "nb_path",
    sorted(NOTEBOOKS_DIR.glob("*.ipynb")),
    ids=lambda p: p.name,
)
def test_notebook_component_imports_resolve(nb_path):
    statements = _component_import_statements(nb_path)
    if not statements:
        pytest.skip("no component.script imports")

    failures = []
    for stmt in statements:
        try:
            exec(compile(stmt, nb_path.name, "exec"), {})
        except Exception as exc:  # noqa: BLE001 - report any import failure
            failures.append(f"  {stmt!r}\n    -> {type(exc).__name__}: {exc}")

    assert not failures, f"{nb_path.name}: unresolved component.script imports:\n" + "\n".join(
        failures
    )

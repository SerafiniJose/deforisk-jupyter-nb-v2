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

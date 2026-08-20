"""ui.ipynb is the SEPAL entry point (run via voila).

Guard against it drifting from the shell entry point in gui/solara_app.py.
"""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _notebook_code() -> str:
    """Extract and concatenate all code cells from ui.ipynb."""
    nb = json.loads((REPO_ROOT / "ui.ipynb").read_text())
    return "\n".join(
        "".join(cell["source"]) for cell in nb["cells"] if cell["cell_type"] == "code"
    )


def test_ui_notebook_displays_page():
    """Verify notebook imports Page and calls display(element)."""
    code = _notebook_code()
    assert "from gui.solara_app import Page" in code
    assert "element = Page()" in code
    assert "display(element)" in code


def test_ui_notebook_fixes_sys_path():
    """Verify notebook fixes sys.path to make gui module importable."""
    # voila starts the kernel with cwd = notebook dir; the notebook must make
    # `import gui` resolvable itself (gui/ is import-path-only by design).
    code = _notebook_code()
    assert "sys.path" in code

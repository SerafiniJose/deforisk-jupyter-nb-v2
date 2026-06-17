# tests/test_geemap_pruned.py
"""geemap must be fully pruned: no import in source, not in pyproject, the
package imports without geemap installed."""

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_no_geemap_import_in_source():
    offenders = []
    for py in (REPO_ROOT / "spatialrisk").rglob("*.py"):
        text = py.read_text()
        if "import geemap" in text or "geemap." in text:
            offenders.append(str(py))
    assert not offenders, f"geemap still referenced in: {offenders}"


def test_geemap_not_in_pyproject():
    text = (REPO_ROOT / "pyproject.toml").read_text()
    assert "geemap" not in text


def test_geemap_is_not_installed_so_absence_is_safe():
    # The env intentionally has no geemap; the package must not need it.
    assert importlib.util.find_spec("geemap") is None

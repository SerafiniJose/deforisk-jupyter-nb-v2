"""Shared pytest configuration for the spatial-risk test suite.

Importing ``spatialrisk`` triggers an import-time ``mkdir`` of
``Path.cwd().parent / "data"`` (see ``spatialrisk/project.py``) -- a known wart
slated for removal in a later refactor step. We chdir into ``<repo>/notebooks``
before any test imports so that side effect lands on the existing ``<repo>/data``
directory (exactly as it does when the notebooks run from ``notebooks/``)
instead of polluting a parent of the developer's checkout.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Make the repo root importable so ``import spatialrisk`` and the
# ``component.script`` compat shim both resolve regardless of where pytest runs.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Contain spatialrisk.project's import-time mkdir side effect (see docstring).
_notebooks = REPO_ROOT / "notebooks"
if _notebooks.is_dir():
    os.chdir(_notebooks)

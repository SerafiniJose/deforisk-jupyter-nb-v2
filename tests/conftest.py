"""Shared pytest configuration for the spatial-risk test suite."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Make the repo root importable so ``import spatialrisk`` resolves regardless of
# where pytest runs (the package is run from the repo root, not pip-installed).
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

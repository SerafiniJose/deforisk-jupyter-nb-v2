# tests/test_import_after_prune.py
"""Post-prune invariants: spatialrisk imports cleanly, and the distributed
dependency stays because dask-ml still needs it (so it must NOT be pruned)."""

import importlib
import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_spatialrisk_imports_after_prune():
    mod = importlib.import_module("spatialrisk")
    assert mod is not None


def test_distributed_still_required_by_dask_ml():
    # dask-ml is a declared dependency and pulls in distributed transitively;
    # confirm distributed is importable so we keep it in pyproject.
    assert importlib.util.find_spec("distributed") is not None


def test_dask_ml_still_declared():
    text = (REPO_ROOT / "pyproject.toml").read_text()
    assert "dask-ml" in text  # justifies keeping the transitive distributed dep

# tests/test_import_after_prune.py
"""Post-prune invariants: spatialrisk imports cleanly after dependency pruning."""

import importlib


def test_spatialrisk_imports_after_prune():
    mod = importlib.import_module("spatialrisk")
    assert mod is not None

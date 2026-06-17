"""Phase 2 — v1 persistence (ports + LocalFS adapters + v0->v1 migrator)."""

import json
import pickle
from pathlib import Path

import pytest


def test_ports_are_runtime_checkable_protocols():
    from spatialrisk.persistence import EstimatorStorePort, ProjectStorePort

    # Both ports are runtime-checkable Protocols with the contract methods.
    class DummyProjectStore:
        def save(self, doc):
            return "ok"

        def load(self, name):
            return None

        def list(self):
            return []

        def exists(self, name):
            return False

    class DummyEstimatorStore:
        def save(self, payload, dest):
            return dest

        def load(self, ref):
            return {}

    assert isinstance(DummyProjectStore(), ProjectStorePort)
    assert isinstance(DummyEstimatorStore(), EstimatorStorePort)

    # A class missing a method is NOT an instance.
    class Incomplete:
        def save(self, doc):
            return "x"

    assert not isinstance(Incomplete(), ProjectStorePort)


def test_estimator_store_round_trip(tmp_path):
    from spatialrisk.persistence import EstimatorStorePort, LocalFSEstimatorStore

    store = LocalFSEstimatorStore()
    assert isinstance(store, EstimatorStorePort)

    payload = {
        "ml_model": {"coef": [1, 2, 3]},
        "design_sample": {"x": [0.1, 0.2]},
        "formula": "y ~ x",
        "samples_path": "/data/samples.csv",
    }
    dest = tmp_path / "nested" / "glm_cal.pickle"

    ref = store.save(payload, str(dest))
    assert ref == str(dest)
    assert dest.exists()  # parent dir created

    reloaded = store.load(ref)
    assert reloaded == payload


def test_estimator_store_load_missing_raises(tmp_path):
    from spatialrisk.persistence import LocalFSEstimatorStore

    store = LocalFSEstimatorStore()
    with pytest.raises(FileNotFoundError):
        store.load(str(tmp_path / "does_not_exist.pickle"))

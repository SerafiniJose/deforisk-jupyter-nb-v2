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

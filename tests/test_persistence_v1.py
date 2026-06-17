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


def _minimal_doc(name="ut_v1"):
    from spatialrisk.document import LocalRasterSpec, LocalVectorSpec, ProjectDocument
    from spatialrisk.variables.models import RasterizationMethod, RasterType

    raster = LocalRasterSpec(
        kind="local_raster", name="forest", path="/nope/forest.tif",
        raster_type=RasterType.continuous,
    )
    vector = LocalVectorSpec(
        kind="local_vector", name="roads", year=None, active=True,
        path="/nope/roads.shp", rasterization_method=RasterizationMethod.binary,
    )
    return ProjectDocument(
        project_name=name,
        raw_variables={"forest": raster},
        processed_variables={"roads": vector},
    )


def test_project_store_save_writes_json_and_returns_path(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    store = LocalFSProjectStore(data_root=tmp_path)
    doc = _minimal_doc("ut_v1")

    ref = store.save(doc)
    save_path = tmp_path / "ut_v1" / "ut_v1_project.json"
    assert ref == str(save_path)
    assert save_path.exists()

    payload = json.loads(save_path.read_text())
    assert payload["project_name"] == "ut_v1"
    assert payload["schema_version"] == 1
    assert payload["raw_variables"]["forest"]["kind"] == "local_raster"
    assert payload["processed_variables"]["roads"]["kind"] == "local_vector"


def test_project_store_exists_and_list(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    store = LocalFSProjectStore(data_root=tmp_path)
    assert store.list() == []
    assert store.exists("alpha") is False

    store.save(_minimal_doc("beta"))
    store.save(_minimal_doc("alpha"))
    assert store.exists("alpha") is True
    assert store.list() == ["alpha", "beta"]


def test_project_store_round_trip_preserves_registries(tmp_path):
    from spatialrisk.document import FrozenDict, ProjectDocument
    from spatialrisk.persistence import LocalFSProjectStore

    store = LocalFSProjectStore(data_root=tmp_path)
    doc = _minimal_doc("rt")
    store.save(doc)

    loaded = store.load("rt")
    assert isinstance(loaded, ProjectDocument)
    assert loaded == doc  # frozen models compare by value, totally lossless

    # Registries survive as immutable FrozenDicts.
    assert isinstance(loaded.raw_variables, FrozenDict)
    assert set(loaded.raw_variables) == {"forest"}
    assert set(loaded.processed_variables) == {"roads"}
    assert loaded.raw_variables["forest"].kind == "local_raster"
    assert loaded.raw_variables["forest"].path == "/nope/forest.tif"
    with pytest.raises(TypeError):
        loaded.raw_variables["x"] = doc.raw_variables["forest"]


def test_project_store_load_missing_raises(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    store = LocalFSProjectStore(data_root=tmp_path)
    with pytest.raises(FileNotFoundError):
        store.load("nope")

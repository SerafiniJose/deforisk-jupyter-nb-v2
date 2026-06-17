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


DELEGATED_V0 = {
    "project_name": "delegated",
    "raw_variables": {
        "forest": {
            "name": "forest", "data_type": "raster", "year": None, "active": True,
            "tags": [], "path": "/nope/forest.tif", "raster_type": "continuous",
            "post_processing": [], "processing_history": [],
            "default_crs": None, "default_resolution": None,
        }
    },
    "processed_variables": {
        "roads": {
            "name": "roads", "data_type": "vector", "year": None, "active": True,
            "tags": [], "path": "/nope/roads.shp", "rasterization_method": None,
            "default_crs": None,
        }
    },
}


def test_migrate_v0_variables_inject_kind(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    pdir = tmp_path / "delegated"
    pdir.mkdir()
    (pdir / "delegated_project.json").write_text(json.dumps(DELEGATED_V0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("delegated")

    assert doc.schema_version == 1
    assert doc.project_name == "delegated"
    forest = doc.raw_variables["forest"]
    assert forest.kind == "local_raster"
    assert forest.path == "/nope/forest.tif"
    assert forest.raster_type.value == "continuous"
    roads = doc.processed_variables["roads"]
    assert roads.kind == "local_vector"
    # a null v0 rasterization_method becomes the binary default
    assert roads.rasterization_method.value == "binary"


def test_migrate_v0_preserves_dict_key_distinct_from_name(tmp_path):
    """nuevo3 keys vars 'forest_gfc_2015' but the spec.name is 'forest_gfc'."""
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = {
        "project_name": "nuevo3",
        "raw_variables": {
            "forest_gfc_2015": {
                "name": "forest_gfc", "data_type": "raster", "year": 2015,
                "active": True, "tags": ["forest"],
                "path": "/d/forest_gfc_2015.tif", "raster_type": "categorical",
                "post_processing": [], "processing_history": [],
                "default_crs": None, "default_resolution": None,
            }
        },
        "processed_variables": {},
    }
    pdir = tmp_path / "nuevo3"
    pdir.mkdir()
    (pdir / "nuevo3_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("nuevo3")
    assert set(doc.raw_variables) == {"forest_gfc_2015"}
    spec = doc.raw_variables["forest_gfc_2015"]
    assert spec.name == "forest_gfc"
    assert spec.year == 2015
    assert spec.kind == "local_raster"


def test_migrate_v0_drops_unknown_multi_year_field(tmp_path):
    """v0 raster carries a 'multi_year' field absent from v1 LocalRasterSpec."""
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = {
        "project_name": "my", "processed_variables": {},
        "raw_variables": {
            "alt": {
                "name": "alt", "data_type": "raster", "year": None,
                "multi_year": None, "active": True, "tags": [],
                "path": "/d/alt.tif", "raster_type": "continuous",
                "post_processing": [], "default_crs": None,
                "default_resolution": None,
            }
        },
    }
    pdir = tmp_path / "my"
    pdir.mkdir()
    (pdir / "my_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("my")
    assert doc.raw_variables["alt"].kind == "local_raster"
    assert not hasattr(doc.raw_variables["alt"], "multi_year")

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


def test_migrate_v0_base_raster_resolves_to_processed_ref(tmp_path):
    """The embedded base_raster dict -> a VariableId, source matched by name/year."""
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = {
        "project_name": "nuevo2",
        "years": [2015, 2020, 2024],
        "raw_variables": {},
        "processed_variables": {
            "subj_reprojected": {
                "name": "subj_reprojected", "data_type": "raster", "year": None,
                "multi_year": None, "active": True, "tags": [],
                "path": "/d/subj_reprojected.tif", "raster_type": "categorical",
                "post_processing": [], "default_crs": "EPSG:32618",
                "default_resolution": 30.0,
            }
        },
        "base_raster": {
            "name": "subj_reprojected", "data_type": "raster", "year": None,
            "multi_year": None, "active": True, "tags": [],
            "path": "/d/subj_reprojected.tif", "raster_type": "categorical",
            "post_processing": [], "default_crs": "EPSG:32618",
            "default_resolution": 30.0,
        },
    }
    pdir = tmp_path / "nuevo2"
    pdir.mkdir()
    (pdir / "nuevo2_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("nuevo2")
    assert doc.base_raster_ref is not None
    assert doc.base_raster_ref.source == "processed"
    assert doc.base_raster_ref.name == "subj_reprojected"
    assert doc.base_raster_ref.year is None


def test_migrate_v0_base_raster_falls_back_to_raw_source(tmp_path):
    """If base_raster matches only a raw var (by name+year), source is 'raw'."""
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = {
        "project_name": "p",
        "raw_variables": {
            "fl_2020": {
                "name": "fl", "data_type": "raster", "year": 2020, "active": True,
                "tags": [], "path": "/d/fl.tif", "raster_type": "categorical",
                "post_processing": [], "default_crs": None, "default_resolution": None,
            }
        },
        "processed_variables": {},
        "base_raster": {
            "name": "fl", "data_type": "raster", "year": 2020, "active": True,
            "tags": [], "path": "/d/fl.tif", "raster_type": "categorical",
            "post_processing": [], "default_crs": None, "default_resolution": None,
        },
    }
    pdir = tmp_path / "p"
    pdir.mkdir()
    (pdir / "p_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("p")
    assert doc.base_raster_ref.source == "raw"
    assert doc.base_raster_ref.name == "fl"
    assert doc.base_raster_ref.year == 2020


def test_migrate_v0_no_base_raster_leaves_ref_none(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    pdir = tmp_path / "delegated"
    pdir.mkdir()
    (pdir / "delegated_project.json").write_text(json.dumps(DELEGATED_V0))
    doc = LocalFSProjectStore(data_root=tmp_path).load("delegated")
    assert doc.base_raster_ref is None


def _v0_with_models(models: dict) -> dict:
    return {
        "project_name": "m",
        "raw_variables": {},
        "processed_variables": {},
        "models": models,
    }


def test_migrate_v0_glm_model_path_to_estimator_pickle(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = _v0_with_models({
        "glm_cal": {
            "name": "cal", "model_type": "glm", "project_name": "m",
            "dataset_name": "cal_2020", "target_name": "fl",
            "feature_names": ["altitude", "slope"], "year": 2020,
            "formula": "y ~ altitude + slope", "parameters": {"solver": "lbfgs"},
            "sampling": None, "model_path": "/d/glm_cal.pickle",
            "samples_path": "/d/samples.csv", "trained": True,
            "trained_at": "2025-01-01T00:00:00", "n_samples": 10000,
            "deviance": 123.4,
        }
    })
    pdir = tmp_path / "m"
    pdir.mkdir()
    (pdir / "m_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("m")
    glm = doc.models["glm_cal"]
    assert glm.model_type == "glm"
    assert glm.estimator_pickle == "/d/glm_cal.pickle"
    assert glm.feature_names == ("altitude", "slope")
    assert glm.target_name == "fl"
    assert glm.trained is True
    assert not hasattr(glm, "model_path")


def test_migrate_v0_icar_carries_rho_path(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = _v0_with_models({
        "icar_cal": {
            "name": "cal", "model_type": "icar", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": ["slope"],
            "year": 2020, "formula": "y ~ slope", "parameters": {},
            "sampling": None, "model_path": "/d/icar.pickle",
            "rho_path": "/d/rho.tif", "samples_path": None, "trained": True,
            "trained_at": None, "n_samples": None, "deviance": None,
        }
    })
    pdir = tmp_path / "m"
    pdir.mkdir()
    (pdir / "m_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("m")
    icar = doc.models["icar_cal"]
    assert icar.model_type == "icar"
    assert icar.estimator_pickle == "/d/icar.pickle"
    assert icar.rho_path == "/d/rho.tif"


def test_migrate_v0_jnr_and_mw_have_no_pickle(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = _v0_with_models({
        "jnr_b": {
            "name": "b", "model_type": "jnr", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": None, "parameters": {}, "sampling": None,
            "samples_path": None, "trained": True, "trained_at": None,
            "n_samples": None, "deviance": None,
            "dist_thresh": 1000.0, "dist_bins": [0.0, 100.0, 200.0],
            "defrate_files": {"calibration": "/d/defrate_cal.csv"},
        },
        "mw_w": {
            "name": "w", "model_type": "mw", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": None, "parameters": {}, "sampling": None,
            "samples_path": None, "trained": True, "trained_at": None,
            "n_samples": None, "deviance": None,
            "dist_thresh": 500.0, "win_size_list": [5, 11, 21],
            "ldefrate_files": {"calibration": "/d/ldefrate_cal.tif"},
        },
    })
    pdir = tmp_path / "m"
    pdir.mkdir()
    (pdir / "m_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("m")
    jnr = doc.models["jnr_b"]
    assert jnr.model_type == "jnr"
    assert jnr.dist_thresh == 1000.0
    assert jnr.dist_bins == (0.0, 100.0, 200.0)
    assert jnr.defrate_files == {"calibration": "/d/defrate_cal.csv"}
    assert not hasattr(jnr, "estimator_pickle")
    mw = doc.models["mw_w"]
    assert mw.model_type == "mw"
    assert mw.win_size_list == (5, 11, 21)
    assert mw.ldefrate_files == {"calibration": "/d/ldefrate_cal.tif"}


def test_migrate_v0_unknown_model_type_warns_and_skips(tmp_path, capsys):
    from spatialrisk.persistence import LocalFSProjectStore

    v0 = _v0_with_models({
        "weird": {
            "name": "x", "model_type": "bogus", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": None, "formula": None, "parameters": {}, "sampling": None,
            "samples_path": None, "trained": False, "trained_at": None,
            "n_samples": None, "deviance": None,
        }
    })
    pdir = tmp_path / "m"
    pdir.mkdir()
    (pdir / "m_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("m")
    assert "weird" not in doc.models
    assert "bogus" in capsys.readouterr().out


def _v0_with_datasets(raw, processed, datasets) -> dict:
    return {
        "project_name": "d",
        "raw_variables": raw,
        "processed_variables": processed,
        "datasets": datasets,
    }


def _raster_var(name, year):
    return {
        "name": name, "data_type": "raster", "year": year, "active": True,
        "tags": [], "path": f"/d/{name}.tif", "raster_type": "categorical",
        "post_processing": [], "default_crs": None, "default_resolution": None,
    }


def test_migrate_v0_dataset_temporal_target_keeps_year(tmp_path):
    """Temporal target: target_ref carries the dataset year."""
    from spatialrisk.persistence import LocalFSProjectStore

    processed = {
        "fl_2020": _raster_var("fl", 2020),
        "fl_2024": _raster_var("fl", 2024),
        "slope": _raster_var("slope", None),
    }
    v0 = _v0_with_datasets({}, processed, {
        "cal": {
            "name": "cal", "year": 2020, "target_name": "fl",
            "target_year": 2020, "feature_names": ["slope"],
        }
    })
    pdir = tmp_path / "d"
    pdir.mkdir()
    (pdir / "d_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("d")
    ds = doc.datasets["cal"]
    assert ds.name == "cal"
    assert ds.year == 2020
    assert ds.target_ref.name == "fl"
    assert ds.target_ref.year == 2020          # temporal -> year retained
    assert ds.target_ref.source == "processed"
    assert tuple(f.name for f in ds.feature_refs) == ("slope",)
    assert ds.feature_refs[0].year is None      # static feature -> no year


def test_migrate_v0_dataset_static_target_no_year(tmp_path):
    """Static target: target_ref.year is None even when the dataset has a year."""
    from spatialrisk.persistence import LocalFSProjectStore

    processed = {
        "subj": _raster_var("subj", None),
        "slope": _raster_var("slope", None),
    }
    v0 = _v0_with_datasets({}, processed, {
        "cal": {
            "name": "cal", "year": 2020, "target_name": "subj",
            "target_year": None, "feature_names": ["slope"],
        }
    })
    pdir = tmp_path / "d"
    pdir.mkdir()
    (pdir / "d_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("d")
    ds = doc.datasets["cal"]
    assert ds.target_ref.name == "subj"
    assert ds.target_ref.year is None           # static -> year dropped


def test_migrate_v0_dataset_missing_feature_warns_and_skips(tmp_path, capsys):
    """Feature not found in processed vars -> warn + skip (behaviour preserved)."""
    from spatialrisk.persistence import LocalFSProjectStore

    processed = {"slope": _raster_var("slope", None)}
    v0 = _v0_with_datasets({}, processed, {
        "cal": {
            "name": "cal", "year": None, "target_name": None,
            "target_year": None, "feature_names": ["slope", "ghost"],
        }
    })
    pdir = tmp_path / "d"
    pdir.mkdir()
    (pdir / "d_project.json").write_text(json.dumps(v0))

    doc = LocalFSProjectStore(data_root=tmp_path).load("d")
    ds = doc.datasets["cal"]
    names = tuple(f.name for f in ds.feature_refs)
    assert names == ("slope",)                  # ghost dropped
    assert ds.target_ref is None
    captured = capsys.readouterr().out
    assert "ghost" in captured

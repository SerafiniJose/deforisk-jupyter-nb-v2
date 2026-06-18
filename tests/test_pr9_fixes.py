"""Regression tests for the PR#9 review fixes.

Each test pins one defect surfaced in the target-state review:

  #3  JNR/MW variable-name mappings live on the spec (not hard-coded).
  #2  v0->v1 migration folds model hyperparameters into ``parameters``.
  #4  v0 base-raster migration resolves robustly (no dangling ref).
  #5  v0 GEE variables migrate to GEESpec, not broken LocalRasterSpec.
  #6  iCAR fit honours ``csize_interpolate`` and persists its real params.
  #1  Orchestration iterates by registry key, not ``spec.name``.
  #7  ``get_fao_gaul_subj`` accepts Geometry | Feature | FeatureCollection.
  #8  Packaging discovers the ``spatialrisk`` package.
"""

from spatialrisk.document import (
    DatasetSpec,
    JNRSpec,
    LocalRasterSpec,
    MWSpec,
    VariableId,
)
from spatialrisk.session import JNRApplySpec, JNRFitSpec, MWApplySpec, ProjectSession


def _raw_raster(sess, name, path):
    sess.add_local_raster(
        LocalRasterSpec(name=name, path=path, raster_type="continuous")
    )


# --------------------------------------------------------------------------- #
# Fix #3 — JNR/MW mapping fields drive feature resolution
# --------------------------------------------------------------------------- #
def test_jnr_fit_spec_honors_custom_forest_edge_var():
    """A JNR model whose edge feature is named ``forest_gfc_edge`` must resolve
    through ``JNRSpec.forest_edge_var`` rather than a hard-coded ``forest_edge``.
    """
    sess = ProjectSession.create("p")
    _raw_raster(sess, "defor", "/d/defor.tif")
    _raw_raster(sess, "forest_gfc_edge", "/d/edge.tif")
    sess.register_dataset(
        DatasetSpec(
            name="cal",
            target_ref=VariableId(source="raw", name="defor"),
            feature_refs=(VariableId(source="raw", name="forest_gfc_edge"),),
        ),
        key="cal",
    )
    sess.register_model(
        JNRSpec(
            model_type="jnr",
            name="b",
            dataset_name="cal",
            forest_edge_var="forest_gfc_edge",
            parameters={"defor_threshold": 99.5, "max_dist": 5000},
        ),
        key="jnr_b",
    )

    spec = sess.fit_spec("jnr_b")
    assert isinstance(spec, JNRFitSpec)
    assert spec.forest_edge_file == "/d/edge.tif"


def test_jnr_apply_spec_honors_custom_mappings():
    sess = ProjectSession.create("p")
    _raw_raster(sess, "defor", "/d/defor.tif")
    _raw_raster(sess, "forest_gfc_edge", "/d/edge.tif")
    _raw_raster(sess, "forest_gfc", "/d/forest.tif")
    _raw_raster(sess, "adm2", "/d/adm2.tif")
    sess.register_dataset(
        DatasetSpec(
            name="cal",
            target_ref=VariableId(source="raw", name="defor"),
            feature_refs=(
                VariableId(source="raw", name="forest_gfc_edge"),
                VariableId(source="raw", name="forest_gfc"),
                VariableId(source="raw", name="adm2"),
            ),
        ),
        key="cal",
    )
    sess.register_model(
        JNRSpec(
            model_type="jnr",
            name="b",
            dataset_name="cal",
            forest_edge_var="forest_gfc_edge",
            forest_var="forest_gfc",
            subj_var="adm2",
            dist_bins=(0.0, 100.0),
            parameters={"time_interval": 5, "blk_rows": 128},
        ),
        key="jnr_b",
    )

    spec = sess.apply_spec("jnr_b", out_path="/d/out.tif")
    assert isinstance(spec, JNRApplySpec)
    assert spec.forest_edge_file == "/d/edge.tif"
    assert spec.forest_file == "/d/forest.tif"
    assert spec.subj_file == "/d/adm2.tif"


def test_mw_apply_spec_honors_custom_mappings():
    sess = ProjectSession.create("p")
    _raw_raster(sess, "defor", "/d/defor.tif")
    _raw_raster(sess, "forest_gfc_edge", "/d/edge.tif")
    _raw_raster(sess, "forest_gfc", "/d/forest.tif")
    sess.register_dataset(
        DatasetSpec(
            name="cal",
            target_ref=VariableId(source="raw", name="defor"),
            feature_refs=(
                VariableId(source="raw", name="forest_gfc_edge"),
                VariableId(source="raw", name="forest_gfc"),
            ),
        ),
        key="cal",
    )
    sess.register_model(
        MWSpec(
            model_type="mw",
            name="w",
            dataset_name="cal",
            forest_edge_var="forest_gfc_edge",
            forest_var="forest_gfc",
            win_size_list=(5, 11),
            dist_thresh=500.0,
            ldefrate_files={"cal": "/d/ldefrate.tif"},
            parameters={"time_interval": 5, "blk_rows": 256},
        ),
        key="mw_w",
    )

    spec = sess.apply_spec("mw_w", out_path="/d/out.tif")
    assert isinstance(spec, MWApplySpec)
    assert spec.forest_edge_file == "/d/edge.tif"
    assert spec.forest_file == "/d/forest.tif"


def test_jnr_mw_mapping_defaults_match_legacy_session_hardcodes():
    """Defaults preserve the prior hard-coded behaviour for existing projects."""
    jnr = JNRSpec(model_type="jnr")
    assert jnr.forest_edge_var == "forest_edge"
    assert jnr.forest_var == "forest_2015"
    assert jnr.subj_var == "subj"
    mw = MWSpec(model_type="mw")
    assert mw.forest_edge_var == "forest_edge"
    assert mw.forest_var == "forest_2015"


# --------------------------------------------------------------------------- #
# Fix #2 — v0->v1 migration folds model hyperparameters into ``parameters``
# --------------------------------------------------------------------------- #
import json  # noqa: E402

from spatialrisk.persistence import LocalFSProjectStore  # noqa: E402


def _write_v0(tmp_path, models):
    v0 = {
        "project_name": "m",
        "raw_variables": {},
        "processed_variables": {},
        "models": models,
    }
    pdir = tmp_path / "m"
    pdir.mkdir()
    (pdir / "m_project.json").write_text(json.dumps(v0))
    return LocalFSProjectStore(data_root=tmp_path).load("m")


def test_migrate_v0_glm_folds_hyperparams_into_parameters(tmp_path):
    doc = _write_v0(tmp_path, {
        "glm_cal": {
            "name": "cal", "model_type": "glm", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": "y ~ x", "parameters": {}, "sampling": None,
            "model_path": "/d/glm.pickle", "samples_path": None, "trained": True,
            "trained_at": None, "n_samples": None, "deviance": None,
            # legacy top-level GLM hyperparameters (lost before the fix)
            "solver": "liblinear", "max_iter": 250, "random_seed": 42,
        }
    })
    glm = doc.models["glm_cal"]
    assert glm.parameters["solver"] == "liblinear"
    assert glm.parameters["max_iter"] == 250
    assert glm.parameters["random_seed"] == 42
    assert glm.estimator_pickle == "/d/glm.pickle"


def test_migrate_v0_rf_folds_hyperparams_into_parameters(tmp_path):
    doc = _write_v0(tmp_path, {
        "rf_cal": {
            "name": "cal", "model_type": "rf", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": "y ~ x", "parameters": {}, "sampling": None,
            "model_path": "/d/rf.pickle", "samples_path": None, "trained": True,
            "trained_at": None, "n_samples": None, "deviance": None,
            "n_trees": 500, "max_depth": 30, "min_samples_leaf": 5,
            "random_seed": 7,
        }
    })
    rf = doc.models["rf_cal"]
    assert rf.parameters["n_trees"] == 500
    assert rf.parameters["max_depth"] == 30
    assert rf.parameters["min_samples_leaf"] == 5
    assert rf.parameters["random_seed"] == 7


def test_migrate_v0_icar_folds_hyperparams_into_parameters(tmp_path):
    doc = _write_v0(tmp_path, {
        "icar_cal": {
            "name": "cal", "model_type": "icar", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": "y ~ x", "parameters": {}, "sampling": None,
            "model_path": "/d/icar.pickle", "rho_path": "/d/rho.tif",
            "samples_path": None, "trained": True, "trained_at": None,
            "n_samples": None, "deviance": None,
            "csize": 12.5, "mcmc": 3000, "burnin": 2000, "thin": 2,
            "prior_vrho": -2.0, "beta_start": -50.0, "random_seed": 11,
            "csize_interpolate": 0.25,
        }
    })
    icar = doc.models["icar_cal"]
    p = icar.parameters
    assert p["csize"] == 12.5 and p["mcmc"] == 3000 and p["burnin"] == 2000
    assert p["thin"] == 2 and p["prior_vrho"] == -2.0 and p["beta_start"] == -50.0
    assert p["random_seed"] == 11 and p["csize_interpolate"] == 0.25
    assert icar.estimator_pickle == "/d/icar.pickle"
    assert icar.rho_path == "/d/rho.tif"


def test_migrate_v0_jnr_carries_mappings_and_config(tmp_path):
    doc = _write_v0(tmp_path, {
        "jnr_b": {
            "name": "b", "model_type": "jnr", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": None, "parameters": {}, "sampling": None,
            "samples_path": None, "trained": True, "trained_at": None,
            "n_samples": None, "deviance": None,
            "dist_thresh": 1000.0, "dist_bins": [0.0, 100.0],
            "defrate_files": {"cal": "/d/defrate.csv"},
            "forest_edge_var": "forest_gfc_edge", "forest_var": "forest_gfc",
            "subj_var": "adm2",
            "blk_rows": 64, "defor_threshold": 98.0, "max_dist": 4000,
        }
    })
    jnr = doc.models["jnr_b"]
    assert jnr.forest_edge_var == "forest_gfc_edge"
    assert jnr.forest_var == "forest_gfc"
    assert jnr.subj_var == "adm2"
    assert jnr.parameters["blk_rows"] == 64
    assert jnr.parameters["defor_threshold"] == 98.0
    assert jnr.parameters["max_dist"] == 4000
    assert jnr.dist_thresh == 1000.0


def test_migrate_v0_mw_carries_mappings_and_config(tmp_path):
    doc = _write_v0(tmp_path, {
        "mw_w": {
            "name": "w", "model_type": "mw", "project_name": "m",
            "dataset_name": "cal", "target_name": "fl", "feature_names": [],
            "year": 2020, "formula": None, "parameters": {}, "sampling": None,
            "samples_path": None, "trained": True, "trained_at": None,
            "n_samples": None, "deviance": None,
            "dist_thresh": 500.0, "win_size_list": [5, 11],
            "ldefrate_files": {"cal": "/d/ldefrate.tif"},
            "forest_edge_var": "forest_gfc_edge", "forest_var": "forest_gfc",
            "blk_rows": 128, "rescale_max_val": 1000, "time_interval": 5,
        }
    })
    mw = doc.models["mw_w"]
    assert mw.forest_edge_var == "forest_gfc_edge"
    assert mw.forest_var == "forest_gfc"
    assert mw.parameters["blk_rows"] == 128
    assert mw.parameters["rescale_max_val"] == 1000
    assert mw.parameters["time_interval"] == 5


def test_executor_glm_forwards_hyperparams_to_model(tmp_path, monkeypatch):
    """_fit_supervised must pass solver/max_iter/random_seed from spec.parameters
    to the GLMModel constructor, not merely store them in ``parameters``.
    """
    import spatialrisk.mlmodels.glm_model as glm_mod
    from spatialrisk.predictors.executor import SessionExecutor

    captured = {}

    class _SpyGLM(glm_mod.GLMModel):
        def __init__(self, **kw):
            captured.update(kw)
            super().__init__(**{k: v for k, v in kw.items()})

        def fit(self, folder=None):  # no real raster I/O
            self._ml_model = {"trained": True}
            self.samples_path = f"{folder}/samples.csv"
            self.trained_at = "2025-01-01T00:00:00"
            self.n_samples = 10
            self.deviance = 1.0

    monkeypatch.setattr(glm_mod, "GLMModel", _SpyGLM)

    sess = ProjectSession.create("p", estimator_store=_MemEstimatorStore())
    _raw_raster(sess, "fl", "/d/fl.tif")
    _raw_raster(sess, "x", "/d/x.tif")
    sess.register_dataset(
        DatasetSpec(
            name="cal",
            target_ref=VariableId(source="raw", name="fl"),
            feature_refs=(VariableId(source="raw", name="x"),),
        ),
        key="cal",
    )
    from spatialrisk.document import GLMSpec
    from spatialrisk.sampling import Sampling
    sess.register_model(
        GLMSpec(
            model_type="glm", name="cal", dataset_name="cal",
            formula="fl ~ x", sampling=Sampling(strategy="random", n_samples=5),
            parameters={"solver": "liblinear", "max_iter": 250, "random_seed": 42},
        ),
        key="glm_cal",
    )

    SessionExecutor().fit(sess, "glm_cal")
    assert captured.get("solver") == "liblinear"
    assert captured.get("max_iter") == 250
    assert captured.get("random_seed") == 42


class _MemEstimatorStore:
    def save(self, payload, dest):
        return str(dest)

    def load(self, ref):
        return {}


# --------------------------------------------------------------------------- #
# Fix #4 — robust v0 base-raster migration (never a dangling ref)
# --------------------------------------------------------------------------- #
def _write_v0_full(tmp_path, name, payload):
    pdir = tmp_path / name
    pdir.mkdir()
    (pdir / f"{name}_project.json").write_text(json.dumps(payload))
    return LocalFSProjectStore(data_root=tmp_path).load(name)


def _v0_raster(name, year=None, path=None):
    return {
        "name": name, "data_type": "raster", "year": year, "active": True,
        "tags": [], "path": path or f"/d/{name}.tif", "raster_type": "continuous",
        "post_processing": [], "default_crs": None, "default_resolution": None,
    }


def test_base_raster_suffix_normalized_match_is_not_dangling(tmp_path):
    """nuevo2 case: base 'subj_reprojected' but registry has the '_matched' product."""
    doc = _write_v0_full(tmp_path, "nuevo2", {
        "project_name": "nuevo2",
        "raw_variables": {"subj": _v0_raster("subj")},
        "processed_variables": {
            "subj_reprojected_matched": _v0_raster(
                "subj_reprojected_matched", path="/d/subj_reprojected_matched.tif"
            ),
        },
        "base_raster": _v0_raster("subj_reprojected", path="/d/subj_reprojected.tif"),
    })
    ref = doc.base_raster_ref
    assert ref is not None
    # resolves to the on-disk processed product (suffix-normalized), not a ghost
    assert ref.source == "processed"
    assert ref.name == "subj_reprojected_matched"
    # and the reference actually resolves to a variable that exists
    sess = ProjectSession.from_document(doc)
    assert sess.get_variable(ref.name, year=ref.year, source=ref.source) is not None


def test_base_raster_path_match_wins_over_name(tmp_path):
    """Same on-disk file under a different registry name resolves by path."""
    doc = _write_v0_full(tmp_path, "p", {
        "project_name": "p",
        "raw_variables": {},
        "processed_variables": {
            "grid": _v0_raster("grid", path="/d/base.tif"),
        },
        "base_raster": _v0_raster("base_raster", path="/d/base.tif"),
    })
    ref = doc.base_raster_ref
    assert ref.source == "processed"
    assert ref.name == "grid"


def test_base_raster_unmatched_is_inserted_as_processed(tmp_path):
    """When nothing matches, the embedded base raster is inserted, not dropped."""
    doc = _write_v0_full(tmp_path, "p", {
        "project_name": "p",
        "raw_variables": {},
        "processed_variables": {},
        "base_raster": _v0_raster("mybase", path="/d/mybase.tif"),
    })
    ref = doc.base_raster_ref
    assert ref is not None and ref.name == "mybase"
    sess = ProjectSession.from_document(doc)
    resolved = sess.get_variable(ref.name, year=ref.year, source=ref.source)
    assert resolved is not None
    assert resolved.path == "/d/mybase.tif"
    assert resolved.kind == "local_raster"


# --------------------------------------------------------------------------- #
# Fix #5 — v0 GEE variables migrate to GEESpec (not broken LocalRasterSpec)
# --------------------------------------------------------------------------- #
import pytest  # noqa: E402


def _v0_gee(name, data_type, *, path=None, gee_images=None, scale=30.0,
            raster_type="categorical"):
    d = {
        "name": name, "data_type": data_type, "year": None, "active": True,
        "tags": [], "path": path, "gee_images": gee_images,
        "default_scale": scale, "default_crs": "EPSG:4326",
        "raster_type": raster_type if data_type == "raster" else None,
        "rasterization_method": "binary" if data_type == "vector" else None,
        "post_processing": [],
    }
    return d


def test_migrate_v0_gee_asset_path_becomes_gee_spec(tmp_path):
    doc = _write_v0_full(tmp_path, "p", {
        "project_name": "p",
        "raw_variables": {
            "subj": _v0_gee("subj", "raster", path="projects/foo/assets/subj"),
        },
        "processed_variables": {},
    })
    spec = doc.raw_variables["subj"]
    assert spec.kind == "gee"
    assert spec.recipe.source == "asset"
    assert spec.recipe.asset_id == "projects/foo/assets/subj"
    assert spec.recipe.export_kind == "raster"


def test_migrate_v0_gee_images_vector_becomes_gee_spec(tmp_path):
    doc = _write_v0_full(tmp_path, "p", {
        "project_name": "p",
        "raw_variables": {
            "roads": _v0_gee("roads", "vector", gee_images=["users/me/roads"]),
        },
        "processed_variables": {},
    })
    spec = doc.raw_variables["roads"]
    assert spec.kind == "gee"
    assert spec.recipe.asset_id == "users/me/roads"
    assert spec.recipe.export_kind == "vector"


def test_migrate_v0_gee_live_image_dump_raises_clear_error(tmp_path):
    payload = {
        "project_name": "p",
        "raw_variables": {
            "ndvi": _v0_gee("ndvi", "raster", path=None,
                            gee_images=["<ee.Image object at 0x7f00>"]),
        },
        "processed_variables": {},
    }
    pdir = tmp_path / "p"
    pdir.mkdir()
    (pdir / "p_project.json").write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=r"GEE variable 'ndvi'.*no asset id"):
        LocalFSProjectStore(data_root=tmp_path).load("p")


# --------------------------------------------------------------------------- #
# Fix #6 — iCAR fit honours csize_interpolate and persists its real params
# --------------------------------------------------------------------------- #
def _icar_session(tmp_path):
    from spatialrisk.document import ICARSpec
    from spatialrisk.sampling import Sampling

    sess = ProjectSession.create("p", estimator_store=_MemEstimatorStore())
    _raw_raster(sess, "fl", "/d/fl.tif")
    _raw_raster(sess, "x", "/d/x.tif")
    sess.register_dataset(
        DatasetSpec(
            name="cal",
            target_ref=VariableId(source="raw", name="fl"),
            feature_refs=(VariableId(source="raw", name="x"),),
        ),
        key="cal",
    )
    sess.register_model(
        ICARSpec(
            model_type="icar", name="cal", dataset_name="cal", formula="fl ~ x",
            sampling=Sampling(strategy="random", n_samples=5),
            parameters={
                "csize": 12.0, "mcmc": 100, "burnin": 50, "thin": 2,
                "prior_vrho": -2.0, "beta_start": -50.0, "random_seed": 11,
                "csize_interpolate": 0.25,
            },
        ),
        key="icar_cal",
    )
    return sess


def _spy_icar(monkeypatch):
    import spatialrisk.mlmodels.icar_model as icar_mod

    captured = {}

    class _SpyICAR(icar_mod.ICARModel):
        def __init__(self, **kw):
            captured.update(kw)
            super().__init__(**kw)

        def fit(self, folder=None):  # no MCMC
            self._ml_model = {"betas": [0.0]}
            self.samples_path = f"{folder}/samples.csv"
            self.rho_path = f"{folder}/rho.tif"
            self.trained_at = "2025-01-01T00:00:00"
            self.n_samples = 5
            self.deviance = 1.0

    monkeypatch.setattr(icar_mod, "ICARModel", _SpyICAR)
    return captured


def test_icar_fit_forwards_csize_interpolate(tmp_path, monkeypatch):
    from spatialrisk.predictors.executor import SessionExecutor

    captured = _spy_icar(monkeypatch)
    sess = _icar_session(tmp_path)
    SessionExecutor().fit(sess, "icar_cal")
    assert captured.get("csize_interpolate") == 0.25
    assert captured.get("csize") == 12.0


def test_icar_trained_spec_persists_real_parameters(tmp_path, monkeypatch):
    from spatialrisk.predictors.executor import SessionExecutor

    _spy_icar(monkeypatch)
    sess = _icar_session(tmp_path)
    trained = SessionExecutor().fit(sess, "icar_cal")
    p = trained.parameters
    assert p != {}
    assert p["csize_interpolate"] == 0.25
    assert p["csize"] == 12.0
    assert p["mcmc"] == 100
    assert p["burnin"] == 50
    assert p["thin"] == 2
    assert p["prior_vrho"] == -2.0
    assert p["beta_start"] == -50.0
    assert p["random_seed"] == 11


# --------------------------------------------------------------------------- #
# Fix #1 — orchestration iterates by registry KEY, not spec.name
# --------------------------------------------------------------------------- #
def _session_with_materialized_raster():
    from spatialrisk.document import CatalogueRecipe, GEESpec

    sess = ProjectSession.create("p")
    # GEE source (name 'altitude') already materialized into a product.
    sess.add_gee_variable(
        GEESpec(
            name="altitude", data_type="raster", raster_type="continuous",
            recipe=CatalogueRecipe(catalogue_key="terrain", export_kind="raster"),
            materialized_key="altitude__materialized",
        ),
        key="altitude",
    )
    # The on-disk product lives under a DIFFERENT key but the SAME spec.name.
    sess.add_local_raster(
        LocalRasterSpec(
            name="altitude", path="/d/alt_matched.tif",
            raster_type="continuous", derived_from="altitude",
        ),
        key="altitude__materialized",
    )
    _raw_raster(sess, "base", "/d/base.tif")
    sess.set_base_raster(VariableId(source="raw", name="base"))
    return sess


def test_reproject_all_processes_materialized_product_by_key(monkeypatch):
    import spatialrisk.session as smod

    recorded = []

    def _spy(self, *a, **k):
        recorded.append(self.key)
        return self

    monkeypatch.setattr(smod.VariableHandle, "reproject_and_match", _spy)

    sess = _session_with_materialized_raster()
    sess.reproject_and_match_all(source="raw")
    # the product (under its own key) is reprojected; the GEE recipe is skipped,
    # and we must NOT resolve to the recipe under the bare name 'altitude'.
    assert recorded == ["altitude__materialized"]


def test_rasterize_all_processes_materialized_vector_by_key(monkeypatch):
    import spatialrisk.session as smod
    from spatialrisk.document import CatalogueRecipe, GEESpec, LocalVectorSpec

    recorded = []

    def _spy_rasterize(self, *a, **k):
        recorded.append(self.key)
        return self

    monkeypatch.setattr(smod.VariableHandle, "rasterize", _spy_rasterize)

    sess = ProjectSession.create("p")
    sess.add_gee_variable(
        GEESpec(
            name="roads", data_type="vector",
            rasterization_method="binary",
            recipe=CatalogueRecipe(catalogue_key="roads", export_kind="vector"),
            materialized_key="roads__materialized",
        ),
        key="roads",
    )
    sess.add_local_vector(
        LocalVectorSpec(
            name="roads", path="/d/roads_matched.shp",
            rasterization_method="binary", derived_from="roads",
        ),
        key="roads__materialized",
    )
    _raw_raster(sess, "base", "/d/base.tif")
    sess.set_base_raster(VariableId(source="raw", name="base"))

    sess.rasterize_all(source="raw")
    assert recorded == ["roads__materialized"]


# --------------------------------------------------------------------------- #
# Fix #7 — get_fao_gaul_subj accepts Geometry | Feature | FeatureCollection
# --------------------------------------------------------------------------- #
def test_get_fao_gaul_subj_accepts_bare_geometry(monkeypatch):
    import spatialrisk.gee.ee_fao_gaul as mod

    class _FakeGeometry:  # a bare ee.Geometry has NO .geometry() method
        pass

    captured = {}

    class _FakeFC:
        def __init__(self, path):
            self.path = path

        def filterBounds(self, arg):
            captured["arg"] = arg
            return "filtered_fc"

    class _FakeEE:
        FeatureCollection = _FakeFC

    monkeypatch.setattr(mod, "ee", _FakeEE)

    geom = _FakeGeometry()
    fc, attr = mod.get_fao_gaul_subj(2, geom)  # must NOT raise AttributeError
    assert fc == "filtered_fc"
    assert captured["arg"] is geom
    assert attr == "gaul2_name"


def test_get_fao_gaul_subj_still_accepts_feature_collection(monkeypatch):
    import spatialrisk.gee.ee_fao_gaul as mod

    captured = {}

    class _FakeFC:
        def __init__(self, path):
            self.path = path

        def filterBounds(self, arg):
            captured["arg"] = arg
            return "filtered_fc"

    class _FakeEE:
        FeatureCollection = _FakeFC

    monkeypatch.setattr(mod, "ee", _FakeEE)

    aoi_fc = _FakeFC("aoi")
    fc, attr = mod.get_fao_gaul_subj(1, aoi_fc)
    assert fc == "filtered_fc"
    assert captured["arg"] is aoi_fc
    assert attr == "gaul1_name"


# --------------------------------------------------------------------------- #
# Fix #8 — packaging discovers the spatialrisk package
# --------------------------------------------------------------------------- #
def test_packaging_discovers_spatialrisk():
    import tomllib
    from pathlib import Path

    from setuptools import find_packages

    root = Path(__file__).resolve().parent.parent
    cfg = tomllib.loads((root / "pyproject.toml").read_text())
    find_cfg = cfg["tool"]["setuptools"]["packages"]["find"]
    pkgs = find_packages(
        where=str(root),
        include=find_cfg.get("include", ["*"]),
        exclude=find_cfg.get("exclude", []),
    )
    assert "spatialrisk" in pkgs
    # subpackages come along too
    assert "spatialrisk.gee" in pkgs


# --------------------------------------------------------------------------- #
# Coverage gap (review M7): RF was never exercised end-to-end through the
# executor, and the GLM apply test asserted only existence/dtype. This runs a
# real RF fit+apply and asserts predicted pixel values + registration. It also
# exercises Fix #2 (RF hyperparameters forwarded from parameters).
# --------------------------------------------------------------------------- #
def test_executor_rf_fit_and_apply_end_to_end(tmp_path):
    from pathlib import Path

    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from spatialrisk.document import RFSpec
    from spatialrisk.persistence import LocalFSEstimatorStore, LocalFSProjectStore
    from spatialrisk.predictors.executor import SessionExecutor
    from spatialrisk.sampling import Sampling
    from spatialrisk.variables.models import RasterType

    def _w(path, arr):
        h, w = arr.shape
        with rasterio.open(
            path, "w", driver="GTiff", height=h, width=w, count=1,
            dtype="float32", crs="EPSG:4326", transform=from_origin(0, h, 1, 1),
        ) as dst:
            dst.write(arr.astype("float32"), 1)

    rng = np.random.default_rng(0)
    dem_arr = rng.normal(size=(40, 40)).astype("float32")
    y_arr = (dem_arr > 0).astype("float32")
    tgt, dem = tmp_path / "defor.tif", tmp_path / "dem.tif"
    _w(tgt, y_arr)
    _w(dem, dem_arr)

    sess = ProjectSession.create(
        "rfproj",
        store=LocalFSProjectStore(data_root=tmp_path),
        estimator_store=LocalFSEstimatorStore(),
    )
    sess.add_local_raster(
        LocalRasterSpec(name="defor", path=str(tgt), raster_type=RasterType.continuous)
    )
    sess.add_local_raster(
        LocalRasterSpec(name="dem", path=str(dem), raster_type=RasterType.continuous)
    )
    sess.register_dataset(
        DatasetSpec(
            name="calib",
            target_ref=VariableId(source="raw", name="defor"),
            feature_refs=(VariableId(source="raw", name="dem"),),
            sampling=Sampling(strategy="random", n_samples=500, seed=1),
        )
    )
    sess.register_model(
        RFSpec(
            model_type="rf", name="m1", dataset_name="calib", formula="defor ~ dem",
            parameters={"n_trees": 25, "max_depth": 5, "random_seed": 0},
        ),
        key="rf_m1",
    )

    ex = SessionExecutor()
    trained = ex.fit(sess, "rf_m1")
    assert trained.trained is True
    assert trained.estimator_pickle and Path(trained.estimator_pickle).exists()
    # Fix #2: forwarded hyperparameters are preserved on the trained spec.
    assert trained.parameters["n_trees"] == 25

    out = tmp_path / "rf_pred.tif"
    ex.apply(sess, "rf_m1", str(out))
    assert out.exists()
    with rasterio.open(out) as src:
        assert src.dtypes[0] == "uint16"
        data = src.read(1)
    # real predicted probabilities (rescaled to uint16), not an all-nodata raster
    assert (data > 0).any()
    assert any(p.model_key == "rf_m1" for p in sess._doc.predictions.values())

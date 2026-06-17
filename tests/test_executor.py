from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from spatialrisk.predictors.executor import _DatasetShim, _Var


def _write_raster(path, arr, nodata=None):
    h, w = arr.shape
    profile = dict(driver="GTiff", height=h, width=w, count=1,
                   dtype="float32", crs="EPSG:4326",
                   transform=from_origin(0, h, 1, 1))
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr.astype("float32"), 1)


def test_dataset_shim_to_dataframe(tmp_path):
    from spatialrisk.sampling import Sampling
    tgt = tmp_path / "y.tif"; dem = tmp_path / "dem.tif"
    _write_raster(tgt, np.array([[0, 1], [1, 0]], dtype="float32"))
    _write_raster(dem, np.array([[10, 20], [30, 40]], dtype="float32"))
    ds = _DatasetShim(
        name="calib", year=2020,
        target=_Var("defor", str(tgt)),
        features=[_Var("dem", str(dem))],
    )
    df = ds.to_dataframe(sampling=Sampling(strategy="random", n_samples=4, seed=1))
    assert set(["defor", "dem", "cell_id", "trial"]).issubset(df.columns)
    assert ds.validate() is True


def test_executor_fit_glm_pickles_estimator_and_trains_spec(tmp_path):
    import numpy as np

    from spatialrisk.document import DatasetSpec, GLMSpec, LocalRasterSpec, VariableId
    from spatialrisk.persistence import LocalFSEstimatorStore, LocalFSProjectStore
    from spatialrisk.predictors.executor import SessionExecutor
    from spatialrisk.sampling import Sampling
    from spatialrisk.session import ProjectSession
    from spatialrisk.variables.models import RasterType

    rng = np.random.default_rng(0)
    tgt = tmp_path / "defor.tif"; dem = tmp_path / "dem.tif"
    dem_arr = rng.normal(size=(40, 40)).astype("float32")
    y_arr = (dem_arr > 0).astype("float32")
    _write_raster(tgt, y_arr); _write_raster(dem, dem_arr)

    store = LocalFSProjectStore(data_root=tmp_path)
    est_store = LocalFSEstimatorStore()
    doc_session = ProjectSession.create("fitglm", store=store,
                                        estimator_store=est_store)
    doc_session.add_local_raster(LocalRasterSpec(
        name="defor", path=str(tgt), raster_type=RasterType.continuous))
    doc_session.add_local_raster(LocalRasterSpec(
        name="dem", path=str(dem), raster_type=RasterType.continuous))
    doc_session.register_dataset(DatasetSpec(
        name="calib",
        target_ref=VariableId(source="raw", name="defor"),
        feature_refs=(VariableId(source="raw", name="dem"),),
        sampling=Sampling(strategy="random", n_samples=500, seed=1)))
    doc_session.register_model(GLMSpec(
        model_type="glm", name="m1", dataset_name="calib",
        formula="defor ~ scale(dem)",
        parameters={}, sampling=None, samples_path=None,
        trained=False, n_samples=None, deviance=None,
        estimator_pickle=None), key="glm_m1")

    ex = SessionExecutor()
    ex.fit(doc_session, "glm_m1")

    spec = doc_session._doc.models["glm_m1"]
    assert spec.trained is True
    assert spec.estimator_pickle and Path(spec.estimator_pickle).exists()
    assert spec.samples_path and Path(spec.samples_path).exists()
    assert spec.n_samples and spec.deviance is not None
    payload = est_store.load(spec.estimator_pickle)
    assert set(payload) >= {"ml_model", "formula", "samples_path"}


def _glm_session(tmp_path):
    """Build a ProjectSession with a fittable GLM (defor ~ scale(dem))."""
    import numpy as np

    from spatialrisk.document import DatasetSpec, GLMSpec, LocalRasterSpec, VariableId
    from spatialrisk.persistence import LocalFSEstimatorStore, LocalFSProjectStore
    from spatialrisk.sampling import Sampling
    from spatialrisk.session import ProjectSession
    from spatialrisk.variables.models import RasterType

    rng = np.random.default_rng(0)
    tgt = tmp_path / "defor.tif"; dem = tmp_path / "dem.tif"
    dem_arr = rng.normal(size=(40, 40)).astype("float32")
    y_arr = (dem_arr > 0).astype("float32")
    _write_raster(tgt, y_arr); _write_raster(dem, dem_arr)

    store = LocalFSProjectStore(data_root=tmp_path)
    est_store = LocalFSEstimatorStore()
    doc_session = ProjectSession.create("applyglm", store=store,
                                        estimator_store=est_store)
    doc_session.add_local_raster(LocalRasterSpec(
        name="defor", path=str(tgt), raster_type=RasterType.continuous))
    doc_session.add_local_raster(LocalRasterSpec(
        name="dem", path=str(dem), raster_type=RasterType.continuous))
    doc_session.register_dataset(DatasetSpec(
        name="calib",
        target_ref=VariableId(source="raw", name="defor"),
        feature_refs=(VariableId(source="raw", name="dem"),),
        sampling=Sampling(strategy="random", n_samples=500, seed=1)))
    doc_session.register_model(GLMSpec(
        model_type="glm", name="m1", dataset_name="calib",
        formula="defor ~ scale(dem)",
        parameters={}, sampling=None, samples_path=None,
        trained=False, n_samples=None, deviance=None,
        estimator_pickle=None), key="glm_m1")
    return doc_session


def test_executor_apply_glm_writes_raster_and_registers(tmp_path):
    from spatialrisk.predictors.executor import SessionExecutor

    doc_session = _glm_session(tmp_path)
    ex = SessionExecutor()
    ex.fit(doc_session, "glm_m1")
    out = tmp_path / "pred.tif"
    ex.apply(doc_session, "glm_m1", str(out))
    assert out.exists()
    with rasterio.open(out) as src:
        assert src.dtypes[0] == "uint16"
    preds = doc_session._doc.predictions
    assert any(p.model_key == "glm_m1" for p in preds.values())


def _benchmark_session(tmp_path, name, with_forest=False):
    """Build a ProjectSession with defor + forest_edge (+ forest) rasters."""
    from spatialrisk.document import DatasetSpec, LocalRasterSpec, VariableId
    from spatialrisk.persistence import LocalFSEstimatorStore, LocalFSProjectStore
    from spatialrisk.session import ProjectSession
    from spatialrisk.variables.models import RasterType

    defor = tmp_path / "defor.tif"
    edge = tmp_path / "forest_edge.tif"
    _write_raster(defor, np.array([[0, 1], [1, 0]], dtype="float32"))
    _write_raster(edge, np.array([[10, 20], [30, 40]], dtype="float32"))

    store = LocalFSProjectStore(data_root=tmp_path)
    est_store = LocalFSEstimatorStore()
    sess = ProjectSession.create(name, store=store, estimator_store=est_store)
    sess.add_local_raster(LocalRasterSpec(
        name="defor", path=str(defor), raster_type=RasterType.continuous))
    sess.add_local_raster(LocalRasterSpec(
        name="forest_edge", path=str(edge), raster_type=RasterType.continuous))

    feature_refs = [VariableId(source="raw", name="forest_edge")]
    if with_forest:
        forest = tmp_path / "forest_2015.tif"
        _write_raster(forest, np.array([[1, 1], [1, 1]], dtype="float32"))
        sess.add_local_raster(LocalRasterSpec(
            name="forest_2015", path=str(forest), raster_type=RasterType.continuous))
        feature_refs.append(VariableId(source="raw", name="forest_2015"))

    sess.register_dataset(DatasetSpec(
        name="calib",
        target_ref=VariableId(source="raw", name="defor"),
        feature_refs=tuple(feature_refs)))
    return sess


def test_executor_fit_jnr_captures_thresh_and_bins(tmp_path, monkeypatch):
    import spatialrisk.rmj as rmj
    from spatialrisk.document import JNRSpec
    from spatialrisk.predictors.executor import SessionExecutor

    sess = _benchmark_session(tmp_path, "fitjnr")
    sess.register_model(JNRSpec(
        model_type="jnr", name="m1", dataset_name="calib",
        parameters={"defor_threshold": 99.5, "max_dist": 5000},
        trained=False), key="jnr_m1")

    # Fakes for the heavy rmj ops (real ones need representative rasters).
    monkeypatch.setattr(
        rmj.deforrate, "dist_edge_threshold",
        lambda **kw: {"dist_thresh": 123.0})
    monkeypatch.setattr(
        rmj, "compute_dist_bins",
        lambda **kw: [0.0, 50.0, 123.0])

    ex = SessionExecutor()
    ex.fit(sess, "jnr_m1")

    spec = sess._doc.models["jnr_m1"]
    assert isinstance(spec, JNRSpec)
    assert spec.trained is True
    assert spec.dist_thresh == 123.0
    assert len(spec.dist_bins) > 0


def test_executor_fit_mw_captures_thresh_and_ldefrate(tmp_path, monkeypatch):
    import spatialrisk.rmj as rmj
    from spatialrisk.document import MWSpec
    from spatialrisk.predictors.executor import SessionExecutor

    sess = _benchmark_session(tmp_path, "fitmw", with_forest=True)
    sess.register_model(MWSpec(
        model_type="mw", name="m1", dataset_name="calib",
        parameters={"win_size_list": [5, 11], "time_interval": 5,
                    "defor_threshold": 99.5},
        trained=False), key="mw_m1")

    monkeypatch.setattr(
        rmj.deforrate, "dist_edge_threshold",
        lambda **kw: {"dist_thresh": 222.0})

    def _fake_local_defor_rate(**kw):
        # Create the ldefrate raster the model records so it exists on disk.
        _write_raster(kw["ldefrate_file"], np.zeros((2, 2), dtype="float32"))

    monkeypatch.setattr(
        rmj.deforrate, "local_defor_rate", _fake_local_defor_rate)

    ex = SessionExecutor()
    ex.fit(sess, "mw_m1")

    spec = sess._doc.models["mw_m1"]
    assert isinstance(spec, MWSpec)
    assert spec.trained is True
    assert spec.dist_thresh == 222.0
    assert spec.win_size_list == (5, 11)
    assert len(spec.ldefrate_files) > 0


def _write_raster_m(path, arr, res=1000):
    """Write a metric-CRS raster (EPSG:3857, ``res`` m pixels).

    forestatrisk's ``cellneigh`` derives the spatial-cell grid from the
    raster's geotransform extent in CRS units, so the iCAR fit needs a
    projected (metres) raster rather than the degree-based EPSG:4326 grid
    used by the supervised tests.
    """
    h, w = arr.shape
    profile = dict(driver="GTiff", height=h, width=w, count=1,
                   dtype="float32", crs="EPSG:3857",
                   transform=from_origin(0, h * res, res, res))
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr.astype("float32"), 1)


def test_executor_fit_icar_captures_rho_and_pickles_estimator(tmp_path):
    import numpy as np

    from spatialrisk.document import DatasetSpec, ICARSpec, LocalRasterSpec, VariableId
    from spatialrisk.persistence import LocalFSEstimatorStore, LocalFSProjectStore
    from spatialrisk.predictors.executor import SessionExecutor
    from spatialrisk.sampling import Sampling
    from spatialrisk.session import ProjectSession
    from spatialrisk.variables.models import RasterType

    # 60 x 60 grid of 1 km pixels -> cellneigh(csize=10 km) yields a 6 x 6
    # spatial-cell grid (36 cells), small enough for a fast MCMC fit.
    rng = np.random.default_rng(0)
    tgt = tmp_path / "defor.tif"; dem = tmp_path / "dem.tif"
    dem_arr = rng.normal(size=(60, 60)).astype("float32")
    p = 1.0 / (1.0 + np.exp(-dem_arr))
    y_arr = (rng.random(size=(60, 60)) < p).astype("float32")
    _write_raster_m(tgt, y_arr); _write_raster_m(dem, dem_arr)

    store = LocalFSProjectStore(data_root=tmp_path)
    est_store = LocalFSEstimatorStore()
    doc_session = ProjectSession.create("fiticar", store=store,
                                        estimator_store=est_store)
    doc_session.add_local_raster(LocalRasterSpec(
        name="defor", path=str(tgt), raster_type=RasterType.continuous))
    doc_session.add_local_raster(LocalRasterSpec(
        name="dem", path=str(dem), raster_type=RasterType.continuous))
    doc_session.register_dataset(DatasetSpec(
        name="calib",
        target_ref=VariableId(source="raw", name="defor"),
        feature_refs=(VariableId(source="raw", name="dem"),),
        sampling=Sampling(strategy="random", n_samples=600, seed=1)))
    doc_session.register_model(ICARSpec(
        model_type="icar", name="m1", dataset_name="calib",
        formula="I(defor) + trial ~ scale(dem)",
        parameters={"csize": 10, "mcmc": 100, "burnin": 20,
                    "csize_interpolate": 2},
        sampling=None, samples_path=None,
        trained=False, n_samples=None, deviance=None,
        estimator_pickle=None, rho_path=None), key="icar_m1")

    ex = SessionExecutor()
    ex.fit(doc_session, "icar_m1")

    spec = doc_session._doc.models["icar_m1"]
    assert spec.trained is True
    assert spec.rho_path and Path(spec.rho_path).exists()
    assert spec.estimator_pickle and Path(spec.estimator_pickle).exists()
    assert spec.samples_path and Path(spec.samples_path).exists()
    assert spec.n_samples and spec.deviance is not None
    payload = est_store.load(spec.estimator_pickle)
    assert set(payload) >= {"ml_model", "formula", "samples_path"}


def _benchmark_apply_session(tmp_path, name):
    """Session with defor + forest_2015 + forest_edge + subj rasters."""
    from spatialrisk.document import DatasetSpec, LocalRasterSpec, VariableId
    from spatialrisk.persistence import LocalFSEstimatorStore, LocalFSProjectStore
    from spatialrisk.session import ProjectSession
    from spatialrisk.variables.models import RasterType

    rasters = {
        "defor": np.array([[0, 1], [1, 0]], dtype="float32"),
        "forest_2015": np.array([[1, 1], [1, 1]], dtype="float32"),
        "forest_edge": np.array([[10, 20], [30, 40]], dtype="float32"),
        "subj": np.array([[1, 2], [3, 4]], dtype="float32"),
    }
    store = LocalFSProjectStore(data_root=tmp_path)
    est_store = LocalFSEstimatorStore()
    sess = ProjectSession.create(name, store=store, estimator_store=est_store)
    for rname, arr in rasters.items():
        path = tmp_path / f"{rname}.tif"
        _write_raster(path, arr)
        sess.add_local_raster(LocalRasterSpec(
            name=rname, path=str(path), raster_type=RasterType.continuous))
    sess.register_dataset(DatasetSpec(
        name="calib",
        target_ref=VariableId(source="raw", name="defor"),
        feature_refs=(
            VariableId(source="raw", name="forest_2015"),
            VariableId(source="raw", name="forest_edge"),
            VariableId(source="raw", name="subj"),
        )))
    return sess


def test_executor_apply_jnr_writes_raster_and_registers(tmp_path, monkeypatch):
    import spatialrisk.rmj as rmj
    from spatialrisk.document import JNRSpec
    from spatialrisk.predictors.executor import SessionExecutor

    sess = _benchmark_apply_session(tmp_path, "applyjnr")
    sess.register_model(JNRSpec(
        model_type="jnr", name="m1", dataset_name="calib",
        parameters={"time_interval": 5, "blk_rows": 128},
        trained=True, dist_thresh=123.0,
        dist_bins=(0.0, 50.0, 123.0)), key="jnr_m1")

    def _fake_vulnerability_map(**kw):
        _write_raster(kw["output_file"], np.zeros((2, 2), dtype="float32"))

    monkeypatch.setattr(rmj, "vulnerability_map", _fake_vulnerability_map)
    monkeypatch.setattr(rmj.deforrate, "defrate_per_class", lambda **kw: None)

    out = tmp_path / "jnr_pred.tif"
    ex = SessionExecutor()
    ex.apply(sess, "jnr_m1", str(out))

    assert out.exists()
    preds = sess._doc.predictions
    assert any(p.model_key == "jnr_m1" for p in preds.values())


def test_executor_apply_mw_writes_raster_and_registers(tmp_path, monkeypatch):
    import spatialrisk.rmj as rmj
    from spatialrisk.document import MWSpec
    from spatialrisk.predictors.executor import SessionExecutor

    sess = _benchmark_apply_session(tmp_path, "applymw")

    # ldefrate rasters the trained MWSpec records (must exist on disk).
    ldefrate_files = {}
    for win in (5, 11):
        ldf = tmp_path / f"ldefrate_{win}.tif"
        _write_raster(ldf, np.zeros((2, 2), dtype="float32"))
        ldefrate_files[str(win)] = str(ldf)

    sess.register_model(MWSpec(
        model_type="mw", name="m1", dataset_name="calib",
        parameters={"time_interval": 5, "blk_rows": 256},
        trained=True, dist_thresh=222.0,
        win_size_list=(5, 11), ldefrate_files=ldefrate_files), key="mw_m1")

    def _fake_set_defor_cat_zero(**kw):
        _write_raster(kw["output_file"], np.zeros((2, 2), dtype="float32"))

    monkeypatch.setattr(rmj, "set_defor_cat_zero", _fake_set_defor_cat_zero)
    monkeypatch.setattr(rmj.deforrate, "defrate_per_cat", lambda **kw: None)

    out = tmp_path / "mw_pred.tif"
    ex = SessionExecutor()
    ex.apply(sess, "mw_m1", str(out))

    preds = sess._doc.predictions
    mw_preds = [p for p in preds.values() if p.model_key == "mw_m1"]
    assert mw_preds
    # MW registers one PredictionSpec per window size; the window is captured.
    assert mw_preds[-1].window in (5, 11)

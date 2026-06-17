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
    from spatialrisk.session import ProjectSession
    from spatialrisk.persistence import LocalFSProjectStore, LocalFSEstimatorStore
    from spatialrisk.document import (
        LocalRasterSpec, DatasetSpec, VariableId, GLMSpec)
    from spatialrisk.variables.models import RasterType
    from spatialrisk.sampling import Sampling
    from spatialrisk.predictors.executor import SessionExecutor

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

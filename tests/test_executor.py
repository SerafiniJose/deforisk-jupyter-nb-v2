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

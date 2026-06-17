import numpy as np
import pytest


class _SigmoidModel:
    def predict_proba(self, X):
        X = np.asarray(X, dtype=float)
        p = 1.0 / (1.0 + np.exp(-X[:, 1]))
        return np.column_stack([1.0 - p, p])


def _write_raster(path, values, nodata=None):
    import rasterio
    from rasterio.transform import from_origin

    arr = np.asarray(values, dtype="float32").reshape(1, -1)
    profile = {
        "driver": "GTiff", "height": 1, "width": arr.shape[1], "count": 1,
        "dtype": "float32", "nodata": nodata, "crs": "EPSG:4326",
        "transform": from_origin(0, 1, 1, 1),
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr, 1)
    return str(path)


def test_supervised_block_fn_uses_predict_proba_col1():
    from spatialrisk.predictors.blocks import supervised_block_fn

    fn = supervised_block_fn(_SigmoidModel())
    x_arr = np.array([[1.0, 0.5], [1.0, 2.0]])
    proba = fn(x_arr, np.array([True, True]), None, None, 1, 2)
    np.testing.assert_allclose(proba, 1.0 / (1.0 + np.exp(-x_arr[:, 1])))


def test_icar_block_fn_adds_rho_to_linear_predictor(tmp_path):
    import rasterio  # noqa: F401

    from spatialrisk.predictors.blocks import icar_block_fn

    pytest.importorskip("forestatrisk")
    rho_const = 0.5
    rho = _write_raster(tmp_path / "rho.tif", [rho_const] * 4)

    # full grid is 4 px wide; block_bounds covers the whole raster
    fn = icar_block_fn(betas=[0.0, 1.0], rho_path=rho)
    x_arr = np.array([[1.0, 0.0], [1.0, 1.0], [1.0, 2.0], [1.0, 3.0]])
    valid_mask = np.array([True, True, True, True])
    block_bounds = (0.0, 0.0, 4.0, 1.0)  # xmin, ymin, xmax, ymax
    proba = fn(x_arr, valid_mask, None, block_bounds, 1, 4)

    logit = np.array([0.0, 1.0, 2.0, 3.0]) + rho_const
    np.testing.assert_allclose(proba, 1.0 / (1.0 + np.exp(-logit)))

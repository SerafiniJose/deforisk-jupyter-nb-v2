"""Golden-output parity for the extracted SupervisedPredictor.apply().

Pins the pure function against the SAME forestatrisk-backed kernel the legacy
base.apply() uses (rescale of predict_block over valid pixels), on tiny rasters.
Imports the predictor by submodule path to dodge the broken package __init__.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _write_raster(path, values, nodata=None):
    import rasterio
    from rasterio.transform import from_origin

    arr = np.asarray(values, dtype="float32")
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    profile = {
        "driver": "GTiff",
        "height": arr.shape[0],
        "width": arr.shape[1],
        "count": 1,
        "dtype": "float32",
        "nodata": nodata,
        "crs": "EPSG:4326",
        "transform": from_origin(0, arr.shape[0], 1, 1),
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr, 1)
    return str(path)


def _design_info(formula="y ~ x"):
    from patsy import dmatrices

    df = pd.DataFrame({"y": [0, 1, 0, 1], "x": [0.0, 1.0, 2.0, 3.0]})
    _, x = dmatrices(formula, df, NA_action="drop")
    return x.design_info


def _sigmoid_block_fn(x_arr, valid_mask, window, block_bounds, n_rows, n_cols):
    # P(y=1) = sigmoid(design column 1) — mirrors test_apply's _SigmoidModel.
    return 1.0 / (1.0 + np.exp(-np.asarray(x_arr, dtype=float)[:, 1]))


def _rescale_expected(proba_valid, valid_idx, n_pixels):
    import forestatrisk as far

    out = np.zeros(n_pixels, dtype=np.uint16)
    out[valid_idx] = far.misc.rescale(
        np.asarray(proba_valid, dtype=float)
    ).astype(np.uint16)
    return out


def test_supervised_predictor_matches_rescaled_predict_block(tmp_path):
    import rasterio
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, 1.0, 2.0, 3.0]
    target = _write_raster(tmp_path / "target.tif", [1] * len(x_vals))
    feat = _write_raster(tmp_path / "x.tif", x_vals)

    out = SupervisedPredictor().apply(
        target_path=target,
        feature_paths={"x": feat},
        formula="y ~ x",
        design_info=_design_info(),
        predict_block_fn=_sigmoid_block_fn,
        mask_path=None,
        output_file=tmp_path / "out.tif",
    )

    with rasterio.open(out) as src:
        arr = src.read(1).ravel()
    p = 1.0 / (1.0 + np.exp(-np.array(x_vals)))
    np.testing.assert_array_equal(arr, _rescale_expected(p, [0, 1, 2, 3], 4))
    assert Path(out) == Path(tmp_path / "out.tif")


def test_supervised_predictor_feature_nodata_becomes_zero(tmp_path):
    import rasterio
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, -999.0, 2.0, 3.0]  # pixel 1 is nodata
    target = _write_raster(tmp_path / "target.tif", [1] * len(x_vals))
    feat = _write_raster(tmp_path / "x.tif", x_vals, nodata=-999.0)

    out = SupervisedPredictor().apply(
        target_path=target,
        feature_paths={"x": feat},
        formula="y ~ x",
        design_info=_design_info(),
        predict_block_fn=_sigmoid_block_fn,
        mask_path=None,
        output_file=tmp_path / "out.tif",
    )
    with rasterio.open(out) as src:
        arr = src.read(1).ravel()
    assert arr[1] == 0
    p = 1.0 / (1.0 + np.exp(-np.array([0.0, 2.0, 3.0])))
    np.testing.assert_array_equal(arr, _rescale_expected(p, [0, 2, 3], 4))


def test_supervised_predictor_mask_suppresses_pixels(tmp_path):
    import rasterio
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, 1.0, 2.0, 3.0]
    target = _write_raster(tmp_path / "target.tif", [1] * len(x_vals))
    feat = _write_raster(tmp_path / "x.tif", x_vals)
    mask = _write_raster(tmp_path / "mask.tif", [0, 1, 0, 1])  # suppress where ==1

    out = SupervisedPredictor().apply(
        target_path=target,
        feature_paths={"x": feat},
        formula="y ~ x",
        design_info=_design_info(),
        predict_block_fn=_sigmoid_block_fn,
        mask_path=mask,
        output_file=tmp_path / "out.tif",
        mask_value=1,
    )
    with rasterio.open(out) as src:
        arr = src.read(1).ravel()
    assert arr[1] == 0 and arr[3] == 0
    assert arr[0] != 0 and arr[2] != 0

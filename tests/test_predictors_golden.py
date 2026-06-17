"""Byte-for-byte parity: SupervisedPredictor vs the legacy base.apply block loop."""

import numpy as np
import pandas as pd
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


def _design_info(formula="y ~ x"):
    from patsy import dmatrices

    df = pd.DataFrame({"y": [0, 1, 0, 1], "x": [0.0, 1.0, 2.0, 3.0]})
    _, x = dmatrices(formula, df, NA_action="drop")
    return x.design_info


def _legacy_apply(target_path, feature_paths, design_info, predict_block_fn,
                  output_file, mask_path=None, mask_value=0):
    """Verbatim reproduction of base.py:300-360 (legacy reference kernel)."""
    from pathlib import Path

    import forestatrisk as far
    import rasterio
    from patsy.highlevel import build_design_matrices

    output_file = Path(output_file)
    with rasterio.open(target_path) as ref:
        profile = ref.profile.copy()
        target_transform = ref.transform
    profile.update(dtype="uint16", count=1, nodata=0)
    _mask_values = (
        (mask_value if isinstance(mask_value, (list, tuple)) else [mask_value])
        if mask_path is not None else None
    )
    with rasterio.open(output_file, "w", **profile) as dst:
        blockinfo = far.misc.makeblock(str(target_path))
        nblock, nblock_x = blockinfo[0], blockinfo[1]
        x_off, y_off, nx, ny = blockinfo[3], blockinfo[4], blockinfo[5], blockinfo[6]
        for b in range(nblock):
            px, py = b % nblock_x, b // nblock_x
            col_start, row_start = x_off[px], y_off[py]
            n_cols, n_rows = nx[px], ny[py]
            window = rasterio.windows.Window(col_start, row_start, n_cols, n_rows)
            block_bounds = rasterio.windows.bounds(window, target_transform)
            mask_invalid = np.zeros(n_rows * n_cols, dtype=bool)
            if mask_path is not None:
                with rasterio.open(mask_path) as ms:
                    mw = rasterio.windows.from_bounds(*block_bounds, ms.transform)
                    mb = ms.read(1, window=mw, out_shape=(n_rows, n_cols),
                                 resampling=rasterio.enums.Resampling.nearest)
                    mn = ms.nodata
                mask_invalid = np.isin(mb.ravel(), _mask_values)
                if mn is not None:
                    mask_invalid |= mb.ravel() == mn
            block_dict = {}
            for name, path in feature_paths.items():
                with rasterio.open(path) as src:
                    arr = src.read(1, window=window).astype(float)
                    if src.nodata is not None:
                        arr[arr == src.nodata] = np.nan
                block_dict[name] = arr.ravel()
            bdf = pd.DataFrame(block_dict)
            valid_mask = ~bdf.isnull().any(axis=1).to_numpy() & ~mask_invalid
            block_df = bdf[valid_mask]
            out_arr = np.zeros(n_rows * n_cols, dtype=np.uint16)
            if not block_df.empty:
                (x_block,) = build_design_matrices([design_info], block_df, NA_action="drop")
                x_arr = np.asarray(x_block)
                proba = predict_block_fn(x_arr, valid_mask, window, block_bounds, n_rows, n_cols)
                out_arr[valid_mask] = far.misc.rescale(
                    np.asarray(proba, dtype=float)).astype(np.uint16)
            dst.write(out_arr.reshape(n_rows, n_cols), 1, window=window)
    return output_file


def test_supervised_golden_matches_legacy(tmp_path):
    import rasterio

    from spatialrisk.predictors.blocks import supervised_block_fn
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    x_vals = [0.2, 0.8, 1.5, 2.5]
    target = _write_raster(tmp_path / "target.tif", [1] * 4)
    feat = _write_raster(tmp_path / "x.tif", x_vals)
    di = _design_info()
    fn = supervised_block_fn(_SigmoidModel())

    legacy = _legacy_apply(target, {"x": feat}, di, fn, tmp_path / "legacy.tif")
    new = SupervisedPredictor().apply(
        target_path=target, feature_paths={"x": feat}, formula="y ~ x",
        design_info=di, predict_block_fn=fn, mask_path=None,
        output_file=tmp_path / "new.tif",
    )
    with rasterio.open(legacy) as a, rasterio.open(new) as b:
        np.testing.assert_array_equal(a.read(1), b.read(1))


def test_icar_golden_matches_legacy(tmp_path):
    import rasterio

    from spatialrisk.predictors.blocks import icar_block_fn
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, 1.0, 2.0, 3.0]
    target = _write_raster(tmp_path / "target.tif", [1] * 4)
    feat = _write_raster(tmp_path / "x.tif", x_vals)
    rho = _write_raster(tmp_path / "rho.tif", [0.5] * 4)
    di = _design_info()
    fn = icar_block_fn(betas=[0.0, 1.0], rho_path=rho)

    legacy = _legacy_apply(target, {"x": feat}, di, fn, tmp_path / "legacy.tif")
    new = SupervisedPredictor().apply(
        target_path=target, feature_paths={"x": feat}, formula="y ~ x",
        design_info=di, predict_block_fn=fn, mask_path=None,
        output_file=tmp_path / "new.tif",
    )
    with rasterio.open(legacy) as a, rasterio.open(new) as b:
        np.testing.assert_array_equal(a.read(1), b.read(1))

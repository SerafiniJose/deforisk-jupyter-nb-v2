"""Golden-output tests for model.apply() raster prediction.

These pin the exact output of GLM/RF/iCAR ``apply()`` so the Template-Method
extraction (one shared block-iterate-and-write skeleton on BaseRiskModel + a
per-model ``_predict_block`` hook) is verified behaviour-preserving.

A stub ``_ml_model`` (sigmoid of the single feature) + a real patsy design_info
let us run the full raster pipeline on tiny GeoTIFFs without training a real
forestatrisk model. ``far.misc.rescale`` is element-wise deterministic, so the
expected UInt16 output is computed exactly.
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #
class _SigmoidModel:
    """Stub estimator: P(y=1) = sigmoid(feature), feature is design col 1."""

    def predict_proba(self, X):
        X = np.asarray(X, dtype=float)
        p = 1.0 / (1.0 + np.exp(-X[:, 1]))
        return np.column_stack([1.0 - p, p])


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


def _dataset(target_path, features):
    return SimpleNamespace(
        target=SimpleNamespace(name="y", path=str(target_path)),
        features=[SimpleNamespace(name=n, path=str(p)) for n, p in features],
    )


def _rescale_expected(proba_valid, valid_idx, n_pixels):
    import forestatrisk as far

    out = np.zeros(n_pixels, dtype=np.uint16)
    out[valid_idx] = far.misc.rescale(np.asarray(proba_valid, dtype=float)).astype(np.uint16)
    return out


def _make_supervised_model(model_cls, tmp_path, x_vals, feature_nodata=None):
    target = _write_raster(tmp_path / "target.tif", [1] * len(x_vals))
    feat = _write_raster(tmp_path / "x.tif", x_vals, nodata=feature_nodata)
    model = model_cls(name="t")
    model.dataset = _dataset(target, [("x", feat)])
    model._ml_model = _SigmoidModel()
    model._x_design_info = _design_info()
    return model


# --------------------------------------------------------------------------- #
# GLM / RF (shared predict_proba kernel)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("model_name", ["GLMModel", "RFModel"])
def test_supervised_apply_writes_rescaled_predict_proba(tmp_path, model_name):
    import rasterio
    import spatialrisk.mlmodels as mlmodels

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, 1.0, 2.0, 3.0]
    model = _make_supervised_model(getattr(mlmodels, model_name), tmp_path, x_vals)

    out_path = model.apply(output_file=tmp_path / "out.tif")

    with rasterio.open(out_path) as src:
        out = src.read(1).ravel()

    p = 1.0 / (1.0 + np.exp(-np.array(x_vals)))
    np.testing.assert_array_equal(out, _rescale_expected(p, [0, 1, 2, 3], 4))


def test_glm_and_rf_apply_are_identical(tmp_path):
    import rasterio
    from spatialrisk.mlmodels import GLMModel, RFModel

    pytest.importorskip("forestatrisk")
    x_vals = [0.2, 0.8, 1.5, 2.5]

    outs = {}
    for name, cls in (("glm", GLMModel), ("rf", RFModel)):
        d = tmp_path / name
        d.mkdir()
        model = _make_supervised_model(cls, d, x_vals)
        with rasterio.open(model.apply(output_file=d / "out.tif")) as src:
            outs[name] = src.read(1)

    np.testing.assert_array_equal(outs["glm"], outs["rf"])


def test_apply_feature_nodata_becomes_zero(tmp_path):
    import rasterio
    from spatialrisk.mlmodels import RFModel

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, -999.0, 2.0, 3.0]  # pixel 1 is nodata
    model = _make_supervised_model(RFModel, tmp_path, x_vals, feature_nodata=-999.0)

    with rasterio.open(model.apply(output_file=tmp_path / "out.tif")) as src:
        out = src.read(1).ravel()

    assert out[1] == 0  # nodata pixel suppressed
    valid_idx = [0, 2, 3]
    p = 1.0 / (1.0 + np.exp(-np.array([0.0, 2.0, 3.0])))
    np.testing.assert_array_equal(out, _rescale_expected(p, valid_idx, 4))


def test_apply_mask_suppresses_pixels(tmp_path):
    import rasterio
    from spatialrisk.mlmodels import RFModel

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, 1.0, 2.0, 3.0]
    model = _make_supervised_model(RFModel, tmp_path, x_vals)
    mask = _write_raster(tmp_path / "mask.tif", [0, 1, 0, 1])  # suppress where ==1

    with rasterio.open(model.apply(output_file=tmp_path / "out.tif", mask=mask, mask_value=1)) as src:
        out = src.read(1).ravel()

    assert out[1] == 0 and out[3] == 0
    assert out[0] != 0 and out[2] != 0


# --------------------------------------------------------------------------- #
# iCAR (logit + rho kernel)
# --------------------------------------------------------------------------- #
def test_icar_apply_uses_logit_plus_rho(tmp_path):
    import rasterio
    from spatialrisk.mlmodels import ICARModel

    pytest.importorskip("forestatrisk")
    x_vals = [0.0, 1.0, 2.0, 3.0]
    rho_const = 0.5
    target = _write_raster(tmp_path / "target.tif", [1, 1, 1, 1])
    feat = _write_raster(tmp_path / "x.tif", x_vals)
    rho = _write_raster(tmp_path / "rho.tif", [rho_const] * 4)

    model = ICARModel(name="t")
    model.dataset = _dataset(target, [("x", feat)])
    model._ml_model = {"betas": [0.0, 1.0]}  # logit = 0*intercept + 1*x + rho
    model._x_design_info = _design_info()
    model.rho_path = rho

    with rasterio.open(model.apply(output_file=tmp_path / "out.tif")) as src:
        out = src.read(1).ravel()

    logit = np.array(x_vals) + rho_const
    p = 1.0 / (1.0 + np.exp(-logit))
    np.testing.assert_array_equal(out, _rescale_expected(p, [0, 1, 2, 3], 4))


# --------------------------------------------------------------------------- #
# _predict_block hook (new) -- RED until the refactor lands
# --------------------------------------------------------------------------- #
def test_base_predict_block_default_uses_predict_proba():
    from spatialrisk.mlmodels import GLMModel

    model = GLMModel(name="t")
    model._ml_model = _SigmoidModel()
    x_arr = np.array([[1.0, 0.5], [1.0, 2.0]])

    proba = model._predict_block(
        x_arr, valid_mask=np.array([True, True]), window=None,
        block_bounds=None, n_rows=1, n_cols=2,
    )

    np.testing.assert_allclose(proba, 1.0 / (1.0 + np.exp(-x_arr[:, 1])))

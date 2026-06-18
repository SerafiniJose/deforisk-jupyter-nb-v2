import numpy as np
import pandas as pd
import pytest


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


def _design_info():
    from patsy import dmatrices

    df = pd.DataFrame({"y": [0, 1, 0, 1], "x": [0.0, 1.0, 2.0, 3.0]})
    _, x = dmatrices("y ~ x", df, NA_action="drop")
    return x.design_info


def test_supervised_apply_registers_once_after_loop(tmp_path):
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    target = _write_raster(tmp_path / "target.tif", [1, 1, 1, 1])
    feat = _write_raster(tmp_path / "x.tif", [0.0, 1.0, 2.0, 3.0])

    class _DS:
        name = "ds_2020"
        year = 2020
        target = type("V", (), {"name": "y", "year": 2020})()
        features = [type("V", (), {"name": "x", "year": None})()]

    seen = []

    out = SupervisedPredictor().apply(
        target_path=target,
        feature_paths={"x": feat},
        formula="y ~ x",
        design_info=_design_info(),
        predict_block_fn=lambda x, *a: 1.0 / (1.0 + np.exp(-x[:, 1])),
        mask_path=None,
        output_file=tmp_path / "out.tif",
        register_prediction=lambda **kw: seen.append(kw),
        model_key="glm_m1",
        dataset=_DS(),
        model_year=2020,
        model_snapshot={"model_type": "glm"},
    )

    assert len(seen) == 1  # exactly one registration, after the loop
    kw = seen[0]
    assert kw["model_key"] == "glm_m1"
    assert kw["dataset_name"] == "ds_2020"
    assert kw["year"] == 2020
    assert kw["window"] is None
    assert kw["path"] == str(out)
    assert kw["dataset_snapshot"]["feature_names"] == ["x"]


def test_supervised_apply_no_register_when_callback_absent(tmp_path):
    from spatialrisk.predictors.supervised import SupervisedPredictor

    pytest.importorskip("forestatrisk")
    target = _write_raster(tmp_path / "target.tif", [1, 1])
    feat = _write_raster(tmp_path / "x.tif", [0.0, 1.0])
    # No register_prediction passed -> must not raise.
    out = SupervisedPredictor().apply(
        target_path=target,
        feature_paths={"x": feat},
        formula="y ~ x",
        design_info=_design_info(),
        predict_block_fn=lambda x, *a: 1.0 / (1.0 + np.exp(-x[:, 1])),
        mask_path=None,
        output_file=tmp_path / "out.tif",
    )
    import rasterio

    with rasterio.open(out) as src:
        assert src.read(1).size == 2

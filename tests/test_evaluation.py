import types

import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin

from spatialrisk.evaluation import interval_from_target, label_for, make_square, validate_two_layer


def test_interval_from_target_parses_two_years():
    assert interval_from_target("forest_loss_2015_2020") == 5
    assert interval_from_target("forest_loss_2020_2024") == 4


def test_interval_from_target_handles_missing_years():
    assert interval_from_target("no_years_here") is None


def _pred(model_key, window=None):
    return types.SimpleNamespace(model_key=model_key, window=window)


def test_label_for_maps_family_and_window():
    assert label_for(_pred("glm_glm_v1")) == "GLM"
    assert label_for(_pred("rf_rf_v1")) == "RF"
    assert label_for(_pred("icar_icar_v1")) == "ICAR"
    assert label_for(_pred("jnr_calibration_jnr")) == "JNR"
    assert label_for(_pred("mw_calibration_mw", window=11)) == "MW_w11"


def _write_raster(path, array, pixel=30.0):
    """Write a single-band GeoTIFF (EPSG:3857, square pixels)."""
    array = np.asarray(array)
    transform = from_origin(0, array.shape[0] * pixel, pixel, pixel)
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="int32", crs="EPSG:3857", transform=transform, nodata=0,
    ) as dst:
        dst.write(array.astype("int32"), 1)
    return str(path)


def test_make_square_partitions_600x300_into_two_cells(tmp_path):
    r = _write_raster(tmp_path / "r.tif", np.ones((300, 600)))
    nsquare, nsquare_x, nsquare_y, x, y, nx, ny = make_square(r, 300)
    assert nsquare == 2 and nsquare_x == 2 and nsquare_y == 1
    assert x == [0, 300] and y == [0]
    assert nx == [300, 300] and ny == [300]


def test_make_square_handles_remainder(tmp_path):
    r = _write_raster(tmp_path / "r2.tif", np.ones((100, 250)))
    nsquare, nsquare_x, nsquare_y, x, y, nx, ny = make_square(r, 100)
    assert nsquare_x == 3 and nx == [100, 100, 50]   # 250 = 100+100+50
    assert nsquare_y == 1 and ny == [100]


def test_validate_two_layer_perfect_prediction(tmp_path):
    # 700 px wide -> make_square gives 3 cells [300,300,100]; the smaller cell makes
    # predicted/observed vary across cells so corrcoef (R2) is well-defined (=1.0).
    nrow, ncol, pixel = 300, 700, 30.0
    pix_area_ha = (pixel * pixel) / 10000.0          # 0.09 ha
    forest = np.ones((nrow, ncol), dtype="int32")     # all forest

    # 30% deforested per coarse cell (top 90 rows of each 300x300 block).
    defor = np.zeros((nrow, ncol), dtype="int32")
    defor[:90, :] = 1     # top 30% of rows deforested across all 700 cols

    risk = np.ones((nrow, ncol), dtype="int32")       # all category 1

    f = _write_raster(tmp_path / "forest.tif", forest, pixel)
    d = _write_raster(tmp_path / "defor.tif", defor, pixel)
    rk = _write_raster(tmp_path / "risk.tif", risk, pixel)

    # Per-cell: ndefor = 90*300 px; nfor = 300*300 px; cat-1 count = nfor.
    # predicted_ha = count * defor_dens * ti ; want == ndefor*pix_area_ha.
    time_interval = 5
    ndefor_px, nfor_px = 90 * 300, 300 * 300
    defor_dens = (ndefor_px * pix_area_ha) / (nfor_px * time_interval)
    tab = tmp_path / "defrate.csv"
    pd.DataFrame({"cat": [1], "defor_dens": [defor_dens]}).to_csv(tab, index=False)

    idx = validate_two_layer(
        defor_file=d, forest_file=f, riskmap_file=rk, tab_file_defor=str(tab),
        time_interval=time_interval, csize_coarse_grid=300,
        indices_file_pred=tmp_path / "indices.csv",
        tab_file_pred=tmp_path / "pred_obs.csv",
        fig_file_pred=tmp_path / "pred_obs.png",
        model_name="TEST", period="calibration",
    )
    assert idx["ncell"] == 3
    assert idx["RMSE"] == 0.0
    assert idx["wRMSE"] == 0.0
    assert idx["MedAE"] == 0.0
    assert idx["R2"] == 1.0
    assert (tmp_path / "indices.csv").exists()
    assert (tmp_path / "pred_obs.png").exists()

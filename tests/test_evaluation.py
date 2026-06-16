import types

import numpy as np
import pytest
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin

from spatialrisk.evaluation import interval_from_target, label_for, make_square


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

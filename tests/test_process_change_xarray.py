"""Tests for the generic loss/gain pixel math (process_change_xarray)."""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from spatialrisk.processing import process_change_xarray, process_forest_loss_xarray


def _write(path, arr, nodata=255):
    arr = np.asarray(arr, dtype="uint8")
    with rasterio.open(
        path, "w", driver="GTiff",
        height=arr.shape[0], width=arr.shape[1], count=1, dtype="uint8",
        crs="EPSG:4326", transform=from_origin(0, 2, 1, 1), nodata=nodata,
    ) as dst:
        dst.write(arr, 1)


def _read(path):
    with rasterio.open(path) as src:
        return src.read(1)


@pytest.fixture
def masks(tmp_path):
    # 2x2 covering all four present/absent transitions:
    # (1,1)=stayed present, (1,0)=loss, (0,1)=gain, (0,0)=stayed absent
    t1 = tmp_path / "t1.tif"
    t2 = tmp_path / "t2.tif"
    _write(t1, [[1, 1], [0, 0]])
    _write(t2, [[1, 0], [1, 0]])
    return t1, t2


def test_loss_encoding(masks, tmp_path):
    t1, t2 = masks
    out = tmp_path / "loss.tif"
    process_change_xarray(str(t1), str(t2), str(out), op="loss")
    # present->present=0, present->absent=1, absent-at-t1 -> 255
    assert _read(out).tolist() == [[0, 1], [255, 255]]


def test_gain_encoding_is_mirror(masks, tmp_path):
    t1, t2 = masks
    out = tmp_path / "gain.tif"
    process_change_xarray(str(t1), str(t2), str(out), op="gain")
    # absent->present=1, absent->absent=0, present-at-t1 -> 255
    assert _read(out).tolist() == [[255, 255], [1, 0]]


def test_output_nodata_metadata(masks, tmp_path):
    t1, t2 = masks
    out = tmp_path / "loss.tif"
    process_change_xarray(str(t1), str(t2), str(out), op="loss")
    with rasterio.open(out) as src:
        assert src.nodata == 255
        assert src.dtypes[0] == "uint8"


def test_forest_loss_wrapper_identical(masks, tmp_path):
    t1, t2 = masks
    a = tmp_path / "a.tif"
    b = tmp_path / "b.tif"
    process_forest_loss_xarray(str(t1), str(t2), str(a))
    process_change_xarray(str(t1), str(t2), str(b), op="loss")
    assert _read(a).tolist() == _read(b).tolist()


def test_invalid_op_raises(masks, tmp_path):
    t1, t2 = masks
    with pytest.raises(ValueError, match="op"):
        process_change_xarray(str(t1), str(t2), str(tmp_path / "x.tif"), op="delta")

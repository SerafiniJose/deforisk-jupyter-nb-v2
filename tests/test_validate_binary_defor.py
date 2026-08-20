"""The forest-loss layer must be binary 0/1 before MW or JNR consume it.

``deforrate`` treats ``defor == 1`` as the event and everything else — including
nodata — as "not deforested", so a wrong layer never raises, it silently trains
on a wrong numerator. These tests cover the guard that stops that.
"""

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")

from spatialrisk.rmj.deforrate import validate_binary_defor  # noqa: E402


def _write(path, array, nodata=None, dtype="uint8"):
    from rasterio.transform import from_origin

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype=dtype,
        nodata=nodata,
        crs="EPSG:3857",
        transform=from_origin(0, array.shape[0], 1, 1),
    ) as dst:
        dst.write(array.astype(dtype), 1)
    return path


def test_accepts_a_binary_zero_one_layer(tmp_path):
    """The contract case: only 0s and 1s."""
    arr = np.zeros((8, 8), dtype="uint8")
    arr[2:5, 2:5] = 1
    path = _write(tmp_path / "loss.tif", arr)

    validate_binary_defor(path)  # must not raise


def test_rejects_a_multi_period_categorical_layer(tmp_path):
    """The defostack case (1/2/3) — period-2 loss would vanish silently."""
    arr = np.array([[0, 1], [2, 3]], dtype="uint8")
    path = _write(tmp_path / "defostack.tif", arr)

    with pytest.raises(ValueError) as exc:
        validate_binary_defor(path, layer_name="defostack")

    msg = str(exc.value)
    assert "defostack" in msg
    assert "3" in msg, "the observed maximum must be reported"


def test_accepts_a_255_fill_when_nodata_is_declared(tmp_path):
    """A declared nodata fill is excluded from the value range."""
    arr = np.full((6, 6), 255, dtype="uint8")
    arr[1:3, 1:3] = 1
    arr[4, 4] = 0
    path = _write(tmp_path / "loss_nodata.tif", arr, nodata=255)

    validate_binary_defor(path)  # must not raise


def test_rejects_a_255_fill_when_nodata_is_undeclared(tmp_path):
    """Without a nodata declaration the fill is real data, and it is not 0/1."""
    arr = np.full((6, 6), 255, dtype="uint8")
    arr[1:3, 1:3] = 1
    path = _write(tmp_path / "loss_nofill.tif", arr)

    with pytest.raises(ValueError, match="255"):
        validate_binary_defor(path)


def test_rejects_a_layer_with_no_deforestation_pixels(tmp_path):
    """All-zero means zero events — training would fit on nothing."""
    arr = np.zeros((5, 5), dtype="uint8")
    path = _write(tmp_path / "empty.tif", arr)

    with pytest.raises(ValueError) as exc:
        validate_binary_defor(path, layer_name="empty_loss")

    assert "no" in str(exc.value).lower()
    assert "empty_loss" in str(exc.value)


def test_rejects_a_percent_cover_layer(tmp_path):
    """A 0-100 tree-cover layer keeps only the pixels equal to exactly 1."""
    arr = np.array([[0, 45], [100, 1]], dtype="uint8")
    path = _write(tmp_path / "tc.tif", arr)

    with pytest.raises(ValueError, match="100"):
        validate_binary_defor(path)


def test_reports_a_missing_file_clearly(tmp_path):
    """A path that cannot be opened fails as such, not as a value error."""
    with pytest.raises(FileNotFoundError):
        validate_binary_defor(tmp_path / "does_not_exist.tif")

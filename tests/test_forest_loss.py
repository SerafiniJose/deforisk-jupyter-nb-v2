"""Tests for the forest-loss / deforestation truth-table.

Canonical encoding (the ML-target convention used by the live pipeline in
``spatialrisk.processing.process_forest_loss_xarray``)::

    1   -> deforestation:    forest at t1 (==1) and non-forest at t2 (==0)
    0   -> remaining forest: forest at t1 and t2 (==1, ==1)
    255 -> nodata / any other pixel combination

The now-removed ``spatialrisk.geo_utils.process_forest_loss`` used the INVERTED
encoding (deforested -> 0, remaining -> 1) and even contradicted its own inline
comments. These tests pin the correct convention so that divergence can never
silently return.
"""

import numpy as np
import pytest


def test_truth_table_matches_canonical_deforestation_encoding():
    from spatialrisk.processing import compute_forest_loss

    nodata = 255
    #                      deforested  remaining  nonforest@t1  nodata@t1  nodata@t2
    forest_t1 = np.array([1,          1,         0,            255,       1],   dtype=np.int16)
    forest_t2 = np.array([0,          1,         0,            1,         255], dtype=np.int16)
    expected = np.array([1,           0,         255,          255,       255], dtype=np.uint8)

    out = compute_forest_loss(forest_t1, forest_t2, nodata, nodata)

    assert out.dtype == np.uint8
    np.testing.assert_array_equal(out, expected)


def test_deforestation_maps_to_one_not_zero():
    """Guard specifically against the inverted (deforested -> 0) encoding."""
    from spatialrisk.processing import compute_forest_loss

    out = compute_forest_loss(
        np.array([1], dtype=np.int16),  # forest at t1
        np.array([0], dtype=np.int16),  # gone at t2  => deforested
        255,
        255,
    )
    assert out.tolist() == [1]


def test_backend_agnostic_with_xarray_where():
    """The helper must accept ``xr.where`` so the xarray/dask path stays lazy."""
    xr = pytest.importorskip("xarray")
    from spatialrisk.processing import compute_forest_loss

    t1 = xr.DataArray(np.array([1, 1, 0], dtype=np.int16))
    t2 = xr.DataArray(np.array([0, 1, 0], dtype=np.int16))

    out = compute_forest_loss(t1, t2, 255, 255, where=xr.where)

    assert out.values.tolist() == [1, 0, 255]


def test_process_forest_loss_xarray_end_to_end(tmp_path):
    """Integration: the live raster pipeline writes the canonical encoding to disk."""
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin

    from spatialrisk.processing import process_forest_loss_xarray

    #                   deforested  stable-forest
    #                   non-forest  nodata-in-t2
    forest_t1 = np.array([[1, 1], [0, 1]], dtype=np.uint8)
    forest_t2 = np.array([[0, 1], [0, 255]], dtype=np.uint8)
    expected = np.array([[1, 0], [255, 255]], dtype=np.uint8)

    profile = {
        "driver": "GTiff",
        "height": 2,
        "width": 2,
        "count": 1,
        "dtype": "uint8",
        "nodata": 255,
        "crs": "EPSG:4326",
        "transform": from_origin(0, 2, 1, 1),
    }
    p1, p2, out = tmp_path / "t1.tif", tmp_path / "t2.tif", tmp_path / "loss.tif"
    for path, arr in ((p1, forest_t1), (p2, forest_t2)):
        with rasterio.open(path, "w", **profile) as dst:
            dst.write(arr, 1)

    process_forest_loss_xarray(str(p1), str(p2), str(out))

    with rasterio.open(out) as src:
        result = src.read(1)
    np.testing.assert_array_equal(result, expected)

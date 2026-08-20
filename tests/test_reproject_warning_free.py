"""Reprojection must not spam the task log with third-party warnings.

Two warnings surfaced on every processing run (printed once each, so they
landed on whichever variable happened to be reprojected first):

* ``FutureWarning: Supplying chunks as dimension-order tuples is deprecated``
  — rioxarray converts ``chunks="auto"`` into a tuple internally before calling
  ``.chunk()``. Passing the dimension-name dict instead avoids the deprecated
  path entirely.
* ``NotGeoreferencedWarning: Dataset has no geotransform`` — GDAL complaining
  about the temporary in-memory dataset ``odc.geo`` builds per warp block. The
  real georeferencing travels alongside as ``src_transform``/``dst_transform``,
  so the warning is noise; ``xr_reproject`` silences just this category for the
  duration of the warp.
"""

import warnings

import numpy as np
import odc.geo.xr  # noqa: F401  # registers the .odc accessor
import rasterio
from odc.geo.geobox import GeoBox
from rasterio.errors import NotGeoreferencedWarning
from rasterio.transform import from_origin

from spatialrisk.geo_utils import RASTER_CHUNKS, xr_reproject


def _write_raster(path, data, res=0.1):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=from_origin(-55.0, -24.0, res, res),
        nodata=255,
    ) as dst:
        dst.write(data, 1)


def test_raster_chunks_uses_dimension_names():
    """The shared chunk spec is a dim-name dict, not ``"auto"``/a tuple."""
    assert isinstance(RASTER_CHUNKS, dict)
    assert set(RASTER_CHUNKS) == {"band", "x", "y"}


def test_reprojection_emits_no_third_party_warnings(tmp_path):
    """A coarse-source warp runs clean: no FutureWarning, no NotGeoreferenced."""
    src = tmp_path / "coarse.tif"
    _write_raster(src, np.full((8, 8), 1500, dtype=np.int16))
    geobox = GeoBox.from_bbox(
        (-55.0, -24.8, -54.2, -24.0), crs="EPSG:4326", resolution=0.001
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        xr_reproject(
            raster_path=str(src),
            geobox=geobox,
            resampling_method="bilinear",
            output_path=str(tmp_path / "out.tif"),
        )

    offenders = [
        w
        for w in caught
        if issubclass(w.category, NotGeoreferencedWarning)
        or (issubclass(w.category, FutureWarning) and "chunks" in str(w.message))
    ]
    assert not offenders, [str(w.message) for w in offenders]


def test_get_base_geobox_emits_no_chunk_futurewarning(tmp_path):
    """``get_base_geobox`` opens the base raster without the deprecated spec."""
    import rioxarray

    src = tmp_path / "base.tif"
    _write_raster(src, np.full((8, 8), 1, dtype=np.int16))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rioxarray.open_rasterio(str(src), chunks=RASTER_CHUNKS, cache=False, lock=False)

    assert not [
        w
        for w in caught
        if issubclass(w.category, FutureWarning) and "chunks" in str(w.message)
    ]

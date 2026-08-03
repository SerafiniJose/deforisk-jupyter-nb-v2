"""``xr_reproject`` must pick the DEFLATE predictor matching the output dtype.

Predictor 2 (horizontal differencing) is defined for integer samples;
predictor 3 (floating-point differencing) is the float variant. The old code
hardcoded 2, which is wrong for the float outputs continuous layers produce
(libtiff either rejects or mis-compresses 64-bit samples with predictor 2).
"""

from pathlib import Path

import numpy as np
import odc.geo.xr  # noqa: F401  # registers the .odc accessor
import rasterio
import rioxarray
from rasterio.transform import from_origin

from spatialrisk.geo_utils import xr_reproject


def _write_raster(path: Path, data: np.ndarray, nodata):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=from_origin(-55.0, -24.0, 0.01, 0.01),
        nodata=nodata,
    ) as dst:
        dst.write(data, 1)


def _reproject_identity(src_path: Path, out_path: Path):
    """Reproject onto the raster's own geobox (grid-identical warp)."""
    da = rioxarray.open_rasterio(str(src_path))
    xr_reproject(
        raster_path=str(src_path),
        geobox=da.odc.geobox,
        resampling_method="nearest",
        output_path=str(out_path),
    )


def _predictor(path: Path) -> str:
    with rasterio.open(path) as src:
        return src.tags(ns="IMAGE_STRUCTURE").get("PREDICTOR")


def test_float_output_gets_float_predictor(tmp_path):
    """Float outputs are written with predictor 3."""
    src = tmp_path / "float.tif"
    _write_raster(src, np.full((32, 32), 26.5, dtype=np.float64), nodata=-32768.0)
    out = tmp_path / "float_out.tif"
    _reproject_identity(src, out)

    assert _predictor(out) == "3"


def test_integer_output_keeps_horizontal_predictor(tmp_path):
    """Integer outputs keep predictor 2."""
    src = tmp_path / "int.tif"
    _write_raster(src, np.full((32, 32), 200, dtype=np.int16), nodata=-32768)
    out = tmp_path / "int_out.tif"
    _reproject_identity(src, out)

    assert _predictor(out) == "2"

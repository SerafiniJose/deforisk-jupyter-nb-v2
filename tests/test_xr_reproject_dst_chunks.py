"""``xr_reproject`` must not inherit its output chunking from a tiny source.

``odc.geo.xr.xr_reproject`` defaults the *destination* chunk shape to the
*source* array's chunk shape (``odc/geo/_dask.py``: ``if chunks is None: chunks
= src.chunksize``). A coarse source such as ERA5-Land precipitation is a single
~55x54-pixel chunk, so warping it onto a 20166x19960 30 m grid produced 135790
destination blocks — one full GDAL warp setup each (~28 s for a single 4096px
tile, and a stream of ``NotGeoreferencedWarning``s from the temporary in-memory
block datasets). Pinning the destination tile size decouples output chunking
from source resolution.
"""

import numpy as np
import odc.geo.xr  # noqa: F401  # registers the .odc accessor
import rasterio
from odc.geo.geobox import GeoBox
from rasterio.transform import from_origin

from spatialrisk.geo_utils import DST_CHUNK, xr_reproject


def _write_raster(path, data, nodata=None, res=0.1):
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
        nodata=nodata,
    ) as dst:
        dst.write(data, 1)


def test_coarse_source_does_not_shrink_output_chunks(tmp_path, monkeypatch):
    """A tiny coarse source is upsampled with full-size destination tiles."""
    src = tmp_path / "coarse.tif"
    _write_raster(src, np.full((8, 8), 1500, dtype=np.int16), nodata=255)

    # A fine target grid over the same footprint: 800x800 at 0.001 deg.
    geobox = GeoBox.from_bbox(
        (-55.0, -24.8, -54.2, -24.0), crs="EPSG:4326", resolution=0.001
    )

    seen = {}
    import odc.geo.xr as odc_xr

    real = odc_xr.xr_reproject

    def spy(*args, **kwargs):
        out = real(*args, **kwargs)
        seen["chunks"] = out.chunks
        return out

    monkeypatch.setattr("spatialrisk.geo_utils.xr.xr_reproject", spy)
    xr_reproject(
        raster_path=str(src),
        geobox=geobox,
        resampling_method="bilinear",
        output_path=str(tmp_path / "out.tif"),
    )

    # Destination tiles come from DST_CHUNK, not from the 8x8 source.
    y_chunk, x_chunk = seen["chunks"][-2][0], seen["chunks"][-1][0]
    assert (y_chunk, x_chunk) == (min(DST_CHUNK, 800), min(DST_CHUNK, 800))


def test_output_stays_georeferenced_and_valued(tmp_path):
    """The pinned chunking still produces a correct, georeferenced raster."""
    src = tmp_path / "coarse.tif"
    _write_raster(src, np.full((8, 8), 1500, dtype=np.int16), nodata=255)
    geobox = GeoBox.from_bbox(
        (-55.0, -24.8, -54.2, -24.0), crs="EPSG:4326", resolution=0.001
    )
    out = tmp_path / "out.tif"

    xr_reproject(
        raster_path=str(src),
        geobox=geobox,
        resampling_method="bilinear",
        output_path=str(out),
    )

    with rasterio.open(out) as ds:
        assert ds.crs.to_epsg() == 4326
        assert ds.shape == geobox.shape
        data = ds.read(1)
    assert np.isclose(data[data != 255].mean(), 1500, rtol=1e-3)

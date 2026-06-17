"""remap_categorical_to_binary must not depend on a running dask scheduler.

The raster write previously used dask.distributed.Lock("rio"), which requires a
live Client/scheduler. A single-process write only needs a local lock. This
test exercises the real function end-to-end on a tiny on-disk GeoTIFF, with NO
dask Client started.
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from spatialrisk.xarray.remap_xarray import remap_categorical_to_binary


def _write_categorical(path):
    data = np.array([[1, 2], [3, 4]], dtype="uint8")
    transform = from_origin(0, 2, 1, 1)
    with rasterio.open(
        path, "w", driver="GTiff", height=2, width=2, count=1,
        dtype="uint8", crs="EPSG:4326", transform=transform, nodata=255,
    ) as dst:
        dst.write(data, 1)


def test_remap_runs_without_dask_client(tmp_path):
    src = tmp_path / "cat.tif"
    out = tmp_path / "bin.tif"
    _write_categorical(src)

    remap_categorical_to_binary(
        input_path=src,
        output_path=out,
        one_values=[1, 2],
        zero_values=[3, 4],
        nodata_value=255,
    )

    with rasterio.open(out) as r:
        arr = r.read(1)
    # values 1,2 -> 1 ; values 3,4 -> 0
    assert arr.tolist() == [[1, 1], [0, 0]]


def test_remap_does_not_import_dask_distributed_lock():
    import inspect

    import spatialrisk.xarray.remap_xarray as m

    src = inspect.getsource(m)
    assert "dask.distributed" not in src
    assert 'Lock("rio")' not in src

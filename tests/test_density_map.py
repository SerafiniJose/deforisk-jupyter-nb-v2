"""Continuous styling for the deforestation-density raster."""

import numpy as np

from gui.scripts.density_map import (
    density_colormap,
    density_layer_key,
    density_value_range,
)


def test_layer_keys_are_namespaced():
    """Density layers never collide with prediction layers on the map."""
    assert density_layer_key("reserve_abc123") == "density_reserve_abc123"


def test_colormap_is_a_matplotlib_colormap_object():
    """get_leaflet_tile_layer rejects dict colormaps — it needs the object."""
    from matplotlib.colors import Colormap

    assert isinstance(density_colormap(), Colormap)


def test_value_range_ignores_nodata(tmp_path):
    """vmin/vmax come from the valid pixels, not the -9999 fill."""
    from osgeo import gdal

    from spatialrisk.allocation import DENSITY_NODATA

    path = tmp_path / "d.tif"
    ds = gdal.GetDriverByName("GTiff").Create(str(path), 4, 4, 1, gdal.GDT_Float64)
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(DENSITY_NODATA)
    arr = np.full((4, 4), DENSITY_NODATA)
    arr[0, 0] = 0.25
    arr[1, 1] = 0.75
    band.WriteArray(arr)
    ds = None

    vmin, vmax = density_value_range(path)

    assert vmin == 0.25
    assert vmax == 0.75


def test_value_range_falls_back_when_every_pixel_is_nodata(tmp_path):
    """An all-nodata raster still yields a usable range instead of raising."""
    from osgeo import gdal

    from spatialrisk.allocation import DENSITY_NODATA

    path = tmp_path / "empty.tif"
    ds = gdal.GetDriverByName("GTiff").Create(str(path), 4, 4, 1, gdal.GDT_Float64)
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(DENSITY_NODATA)
    band.WriteArray(np.full((4, 4), DENSITY_NODATA))
    ds = None

    assert density_value_range(path) == (0.0, 1.0)

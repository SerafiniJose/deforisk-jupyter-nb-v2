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


def test_add_density_on_map_returns_layer_and_value_range(monkeypatch, tmp_path):
    """The range it already computed is handed back so the legend can reuse it."""
    from gui.scripts import density_map

    monkeypatch.setattr(density_map, "density_value_range", lambda path: (0.5, 7.25))

    class FakeClient:
        def center(self):
            """Fixed map center used only when fit_bounds is requested."""
            return (0, 0)

        default_zoom = 5

    class FakeMap:
        def __init__(self):
            """Track the keys of layers added to this fake map."""
            self.added = []

        def remove_layer(self, key, none_ok=False):
            """No-op: nothing has been added yet in this test."""

        def add_layer(self, layer, key=None):
            """Record the layer key that was added."""
            self.added.append(key)

    fake_layer = object()
    monkeypatch.setitem(
        __import__("sys").modules,
        "localtileserver",
        type(
            "M",
            (),
            {
                "TileClient": lambda path: FakeClient(),
                "get_leaflet_tile_layer": lambda *a, **k: fake_layer,
            },
        ),
    )

    layer, value_range = density_map.add_density_on_map(
        FakeMap(), tmp_path / "d.tif", key="density_x", layer_name="run"
    )
    assert layer is fake_layer
    assert value_range == (0.5, 7.25)


def test_drop_density_layer_removes_layer_and_legend():
    """toggle-off and delete both go through _drop_density_layer."""
    from gui.store.state_manager import app_state
    from gui.tile import toolbox_tile

    removed = []

    class FakeMap:
        def remove_layer(self, key, none_ok=False):
            """Record the key removed from the fake map."""
            removed.append(key)

    app_state.clear_legends()
    app_state.register_legends(toolbox_tile._density_legend("run1", "Run 1", 0.0, 3.0))
    toolbox_tile._drop_density_layer(FakeMap(), "density_run1")

    assert removed == ["density_run1"]
    assert app_state.layer_legends.value == ()


def test_density_legend_is_keyed_by_the_map_layer_key():
    """The legend's layer_id matches the key the raster was added under."""
    from gui.scripts.density_map import density_layer_key
    from gui.tile import toolbox_tile

    legend = toolbox_tile._density_legend("run1", "Run 1", 0.0, 3.0)
    assert legend.layer_id == density_layer_key("run1")
    assert legend.label.literal == "Run 1"
    assert [label.literal for label in legend.spec.labels] == ["0", "3"]

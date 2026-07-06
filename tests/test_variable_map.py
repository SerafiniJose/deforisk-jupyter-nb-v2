"""``add_raster_var_on_map`` renders a downloaded variable through
localtileserver with its catalogue palette + pinned range + the file's nodata,
and registers the layer under its key (replacing any prior layer)."""

import gui.scripts.variable_map as vmap


class FakeClient:
    def __init__(self, path):
        self.path = path

    def center(self):
        return (0.0, 0.0)

    default_zoom = 5


class FakeMap:
    def __init__(self):
        self.removed = []
        self.added = []
        self.center = None
        self.zoom = None

    def remove_layer(self, key, none_ok=False):
        self.removed.append(key)

    def add_layer(self, layer, key=""):
        self.added.append((layer, key))


def _patch_localtileserver(monkeypatch):
    captured = {}

    def fake_get_layer(client, **kwargs):
        captured.update(kwargs)
        return "FAKE_LAYER"

    import localtileserver

    monkeypatch.setattr(localtileserver, "TileClient", FakeClient, raising=False)
    monkeypatch.setattr(
        localtileserver, "get_leaflet_tile_layer", fake_get_layer, raising=False
    )
    return captured


def _binary_mask_tif(path, nodata=255):
    """Write a 2x2 single-band GeoTIFF (values 0/1) with a nodata tag."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    data = np.array([[0, 1], [1, 0]], dtype="uint8")
    with rasterio.open(
        path, "w", driver="GTiff", height=2, width=2, count=1,
        dtype="uint8", crs="EPSG:4326", transform=from_origin(0, 2, 1, 1),
        nodata=nodata,
    ) as dst:
        dst.write(data, 1)


def _named(type_name, **attrs):
    return type(type_name, (), attrs)()


def test_predefined_var_rendered_with_catalogue_palette_and_file_nodata(monkeypatch, tmp_path):
    captured = _patch_localtileserver(monkeypatch)
    tif = tmp_path / "rivers.tif"
    _binary_mask_tif(tif, nodata=255)

    var = _named("LocalRasterVar", name="rivers",
                 raster_type=_named("RT", value="categorical"))
    fake_map = FakeMap()

    layer = vmap.add_raster_var_on_map(
        fake_map, str(tif), var=var, layer_name="rivers", key="var_rivers",
    )

    from matplotlib.colors import Colormap

    assert layer == "FAKE_LAYER"
    # catalogue palette (rivers: white background -> blue), pinned to [0, 1]
    assert isinstance(captured["colormap"], Colormap)
    assert captured["vmin"] == 0 and captured["vmax"] == 1
    assert tuple(round(x * 255) for x in captured["colormap"](0.0)[:3]) == (255, 255, 255)
    # the file's nodata tag is honoured so the unmasked 255 fill isn't painted
    assert captured["nodata"] == 255
    # layer registered under key, replacing any prior layer
    assert fake_map.removed == ["var_rivers"]
    assert fake_map.added[0][1] == "var_rivers"


def test_fit_bounds_false_does_not_recenter(monkeypatch, tmp_path):
    _patch_localtileserver(monkeypatch)
    tif = tmp_path / "altitude.tif"
    _binary_mask_tif(tif, nodata=None)

    var = _named("LocalRasterVar", name="altitude",
                 raster_type=_named("RT", value="continuous"))
    fake_map = FakeMap()

    vmap.add_raster_var_on_map(
        fake_map, str(tif), var=var, layer_name="altitude", key="var_altitude",
        fit_bounds=False,
    )

    assert fake_map.center is None and fake_map.zoom is None

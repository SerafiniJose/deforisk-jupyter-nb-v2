"""Predictions are drawn with the QGIS-faithful palette (pinned vmin/vmax), and
overviews are built only when the opt-in flag is set."""

import gui.scripts.prediction_map as pm


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
    """Stub TileClient + get_leaflet_tile_layer; capture the tile-layer kwargs."""
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


def test_prediction_added_with_pinned_far_palette(monkeypatch, tmp_path):
    captured = _patch_localtileserver(monkeypatch)
    tif = tmp_path / "p.tif"
    tif.write_bytes(b"")
    fake_map = FakeMap()

    layer = pm.add_prediction_on_map(
        fake_map,
        str(tif),
        model_key="glm_m1",
        layer_name="glm_m1__d",
        key="pred_glm_m1__d",
    )

    assert layer == "FAKE_LAYER"
    assert captured["vmin"] == 1 and captured["vmax"] == 65535
    assert captured["nodata"] == 0
    assert captured["colormap"][0][:3] == (34, 139, 34)   # FAR green
    assert fake_map.removed == ["pred_glm_m1__d"]          # replaced existing
    assert fake_map.added[0][1] == "pred_glm_m1__d"


def test_overviews_built_only_when_flag_set(monkeypatch, tmp_path):
    _patch_localtileserver(monkeypatch)
    calls = []
    monkeypatch.setattr(
        "spatialrisk.overviews.ensure_overviews",
        lambda p, *a, **k: calls.append(p) or True,
    )
    tif = tmp_path / "p.tif"
    tif.write_bytes(b"")

    pm.add_prediction_on_map(
        FakeMap(), str(tif), model_key="mw_5",
        layer_name="n", key="k", build_overviews=False,
    )
    assert calls == []  # flag off -> no build

    pm.add_prediction_on_map(
        FakeMap(), str(tif), model_key="mw_5",
        layer_name="n", key="k", build_overviews=True,
    )
    assert calls == [str(tif)]  # flag on -> built once


def test_inference_tile_uses_palette_helper_and_overview_option():
    """Predictions route through the QGIS-faithful helper (not bare add_raster),
    overviews are an opt-in checkbox, and the add runs off the Solara loop."""
    import inspect

    from gui.tile import inference_tile

    src = inspect.getsource(inference_tile.InferenceTile)
    assert "add_prediction_on_map" in src         # value-pinned palette path
    assert "map_.add_raster(" not in src           # no more bare grayscale add
    assert "gen_overviews" in src                  # opt-in overviews reactive
    assert "Generate overviews" in src             # checkbox label
    assert "build_overviews=" in src               # flag forwarded to helper
    assert "to_thread" in src                       # add offloaded to a thread

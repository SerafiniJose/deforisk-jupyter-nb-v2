"""Pure-logic tests for the PMTiles sample map helper (no server / no map)."""
from gui.scripts.pmtiles_map import (
    _SOURCE_LAYER,
    add_sample_pmtiles_on_map,
    build_sample_circle_style,
    remove_sample_pmtiles_from_map,
)


class _FakeMap:
    def __init__(self):
        self.removed = []
        self.added = []

    def remove_layer(self, key, none_ok=False):
        self.removed.append(key)

    def add_layer(self, layer, key=None):
        self.added.append((layer, key))


class _FakeTileClient:
    """Stands in for vectortileserver.client.TileClient (no server spawned)."""

    pmtiles_url = "http://127.0.0.1:9999/pmtiles?filePath=/tmp/s.pmtiles"

    def __init__(self, data_source):
        self.data_source = data_source
        self.loopback_enabled = False
        _FakeTileClient.last = self

    def enable_jupyter_loopback(self):
        self.loopback_enabled = True


def test_add_uses_vectortileserver_client(monkeypatch):
    """The tile server dep is PyPI ``vectortileserver``.

    The module was renamed from the git-era ``pyvectortiles`` — importing the
    old name is the SEPAL breakage.
    """
    import vectortileserver.client

    monkeypatch.setattr(vectortileserver.client, "TileClient", _FakeTileClient)
    m = _FakeMap()
    layer = add_sample_pmtiles_on_map(m, "/tmp/s.pmtiles", "Sample 1", "sample_s1")

    client = _FakeTileClient.last
    assert client.data_source == "/tmp/s.pmtiles"
    # 0.2.2+ owns the loopback bridge wiring (port + prefix probe); the app must
    # call it rather than hand-intercepting localhost (host default is now
    # 127.0.0.1, which intercept_localhost would miss).
    assert client.loopback_enabled
    assert layer.url == client.pmtiles_url
    assert layer.style["sources"]["sample"]["url"] == client.pmtiles_url
    assert m.removed == ["sample_s1"]
    assert m.added == [(layer, "sample_s1")]


def test_style_colors_by_strata():
    """One filtered layer per class.

    protomaps-leaflet's json_style has no expression evaluator (a
    ["match", ...] circle-color renders black), but it does support legacy
    filters (==, !=, in, all...).
    """
    style = build_sample_circle_style("http://x/pmtiles?filePath=f")
    event, forest = style["layers"]
    for layer in (event, forest):
        assert layer["type"] == "circle"
        assert layer["source-layer"] == _SOURCE_LAYER
    assert event["filter"] == ["==", "strata", 1]
    assert event["paint"]["circle-color"] == "#d62728"
    assert forest["filter"] == ["!=", "strata", 1]
    assert forest["paint"]["circle-color"] == "#2ca02c"


def test_style_paint_values_are_protomaps_safe():
    """Guard against reintroducing a MapLibre expression paint value.

    json_style copies paint values verbatim into canvas styles — any list/dict
    silently renders black.
    """
    style = build_sample_circle_style("http://x/pmtiles?filePath=f")
    for layer in style["layers"]:
        for key, value in layer["paint"].items():
            assert isinstance(
                value, (str, int, float)
            ), f"{layer['id']}.{key} must be a scalar, got {type(value)}"


def test_remove_delegates_to_map():
    """Removal is a plain keyed delegate to the map."""
    m = _FakeMap()
    remove_sample_pmtiles_from_map(m, "sample_s1")
    assert m.removed == ["sample_s1"]

"""Pure-logic tests for the PMTiles sample map helper (no server / no map)."""
from gui.scripts.pmtiles_map import (
    build_sample_circle_style, remove_sample_pmtiles_from_map, _SOURCE_LAYER)


class _FakeMap:
    def __init__(self):
        self.removed = []
    def remove_layer(self, key, none_ok=False):
        self.removed.append(key)


def test_style_colors_by_strata():
    style = build_sample_circle_style("http://x/pmtiles?filePath=f")
    layer = style["layers"][0]
    assert layer["type"] == "circle"
    assert layer["source-layer"] == _SOURCE_LAYER
    color = layer["paint"]["circle-color"]
    assert color[:2] == ["match", ["get", "strata"]]
    assert "#d62728" in color and "#2ca02c" in color


def test_remove_delegates_to_map():
    m = _FakeMap()
    remove_sample_pmtiles_from_map(m, "sample_s1")
    assert m.removed == ["sample_s1"]

"""Pure-logic tests for the PMTiles sample map helper (no server / no map)."""
from gui.scripts.pmtiles_map import (
    build_sample_circle_style, remove_sample_pmtiles_from_map, _SOURCE_LAYER)


class _FakeMap:
    def __init__(self):
        self.removed = []
    def remove_layer(self, key, none_ok=False):
        self.removed.append(key)


def test_style_colors_by_strata():
    """One filtered layer per class: protomaps-leaflet's json_style has no
    expression evaluator (a ["match", ...] circle-color renders black), but it
    does support legacy filters (==, !=, in, all...)."""
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
    """json_style copies paint values verbatim into canvas styles — any
    list/dict (MapLibre expression) silently renders black. Guard against
    reintroducing one."""
    style = build_sample_circle_style("http://x/pmtiles?filePath=f")
    for layer in style["layers"]:
        for key, value in layer["paint"].items():
            assert isinstance(value, (str, int, float)), (
                f"{layer['id']}.{key} must be a scalar, got {type(value)}")


def test_remove_delegates_to_map():
    m = _FakeMap()
    remove_sample_pmtiles_from_map(m, "sample_s1")
    assert m.removed == ["sample_s1"]

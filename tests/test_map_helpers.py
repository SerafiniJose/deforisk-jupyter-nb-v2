import sys
import types

from gui.scripts.map_helpers import (
    clear_project_overlays,
    draw_aoi_on_map,
    zoom_map_to_aoi,
)


class FakeMap:
    """Records the SepalMap zoom/layer calls made against it."""

    def __init__(self):
        self.zoom_bounds_calls = []
        self.zoom_ee_object_calls = []
        self.added_layers = []          # list of (layer, key)
        self.removed_layer_keys = []
        self.remove_all_calls = []      # list of {"base": ..., "keep_names": ...}

    def remove_all(self, base=False, keep_names=None):
        self.remove_all_calls.append({"base": base, "keep_names": keep_names})

    def zoom_bounds(self, bounds):
        self.zoom_bounds_calls.append(bounds)

    def zoom_ee_object(self, item):
        self.zoom_ee_object_calls.append(item)

    def add_layer(self, layer, key=None):
        self.added_layers.append((layer, key))

    def remove_layer(self, key, none_ok=False):
        self.removed_layer_keys.append(key)


class FakeGdf:
    def __init__(self, total_bounds):
        self.total_bounds = total_bounds


class FakeAoi:
    def __init__(self, gdf=None, feature_collection=None):
        self.gdf = gdf
        self.feature_collection = feature_collection


def test_vector_aoi_zooms_to_total_bounds():
    m = FakeMap()
    bounds = (12.403, 43.893, 12.517, 43.993)
    aoi = FakeAoi(gdf=FakeGdf(bounds))

    assert zoom_map_to_aoi(m, aoi) is True
    assert m.zoom_bounds_calls == [bounds]
    assert m.zoom_ee_object_calls == []


def test_gee_only_aoi_zooms_to_feature_collection():
    m = FakeMap()
    fc = object()  # stand-in for an ee.FeatureCollection
    aoi = FakeAoi(gdf=None, feature_collection=fc)

    assert zoom_map_to_aoi(m, aoi) is True
    assert m.zoom_ee_object_calls == [fc]
    assert m.zoom_bounds_calls == []


def test_gdf_takes_precedence_over_feature_collection():
    m = FakeMap()
    bounds = (0.0, 0.0, 1.0, 1.0)
    aoi = FakeAoi(gdf=FakeGdf(bounds), feature_collection=object())

    assert zoom_map_to_aoi(m, aoi) is True
    assert m.zoom_bounds_calls == [bounds]
    assert m.zoom_ee_object_calls == []


def test_aoi_without_geometry_or_fc_is_noop():
    m = FakeMap()
    aoi = FakeAoi(gdf=None, feature_collection=None)

    assert zoom_map_to_aoi(m, aoi) is False
    assert m.zoom_bounds_calls == []
    assert m.zoom_ee_object_calls == []


def test_none_aoi_is_noop():
    m = FakeMap()
    assert zoom_map_to_aoi(m, None) is False
    assert m.zoom_bounds_calls == []


def test_none_map_is_noop():
    aoi = FakeAoi(gdf=FakeGdf((0.0, 0.0, 1.0, 1.0)))
    assert zoom_map_to_aoi(None, aoi) is False


# --- draw_aoi_on_map --------------------------------------------------------

def _fake_pysepal_mapping(monkeypatch, recorder):
    """Inject a stub pysepal.mapping so draw_aoi_on_map's lazy import is cheap."""
    module = types.ModuleType("pysepal.mapping")

    def get_ipygeojson(gdf, name, style):
        recorder.append((gdf, name, style))
        return f"layer:{name}"

    module.get_ipygeojson = get_ipygeojson
    monkeypatch.setitem(sys.modules, "pysepal.mapping", module)


def test_draw_replaces_existing_layer_and_adds_geojson(monkeypatch):
    calls = []
    _fake_pysepal_mapping(monkeypatch, calls)

    m = FakeMap()
    gdf = FakeGdf((0.0, 0.0, 1.0, 1.0))
    aoi = FakeAoi(gdf=gdf)
    aoi.name = "san_marino"

    assert draw_aoi_on_map(m, aoi) is True
    assert m.removed_layer_keys == ["aoi"]            # clears stale layer first
    assert m.added_layers == [("layer:san_marino", "aoi")]
    assert calls == [(gdf, "san_marino", None)]


def test_draw_without_geometry_is_noop(monkeypatch):
    calls = []
    _fake_pysepal_mapping(monkeypatch, calls)

    m = FakeMap()
    assert draw_aoi_on_map(m, FakeAoi(gdf=None)) is False
    assert m.added_layers == []
    assert calls == []


def test_draw_none_inputs_are_noop():
    assert draw_aoi_on_map(None, FakeAoi(gdf=FakeGdf((0, 0, 1, 1)))) is False
    assert draw_aoi_on_map(FakeMap(), None) is False


def test_clear_project_overlays_removes_non_base_layers():
    """Project switch must drop every app overlay (variables, samples,
    predictions, old AOI) while keeping basemaps (base layers)."""
    m = FakeMap()
    clear_project_overlays(m)
    assert m.remove_all_calls == [{"base": False, "keep_names": None}]


def test_clear_project_overlays_noop_without_map():
    clear_project_overlays(None)  # must not raise

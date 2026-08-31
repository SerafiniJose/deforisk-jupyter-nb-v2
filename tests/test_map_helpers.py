"""Tests for gui.scripts.map_helpers — the map-interaction helpers.

The SepalMap is faked (it pulls in the whole ipyleaflet/pysepal stack), so these
tests assert on the calls the helpers make rather than on rendered layers.
"""

import sys
import types

from gui.scripts.map_helpers import (
    GOOGLE_SATELLITE,
    add_satellite_basemap,
    clear_project_overlays,
    draw_aoi_on_map,
    sync_draw_control_visibility,
    zoom_map_to_aoi,
)


class FakeMap:
    """Records the SepalMap zoom/layer calls made against it."""

    def __init__(self):
        """Start with an empty log of every recorded call."""
        self.zoom_bounds_calls = []
        self.zoom_ee_object_calls = []
        self.added_layers = []  # list of (layer, key)
        self.removed_layer_keys = []
        self.remove_all_calls = []  # list of {"base": ..., "keep_names": ...}

    def remove_all(self, base=False, keep_names=None):
        """Log a bulk layer removal."""
        self.remove_all_calls.append({"base": base, "keep_names": keep_names})

    def zoom_bounds(self, bounds):
        """Log a zoom to a WGS84 bounding box."""
        self.zoom_bounds_calls.append(bounds)

    def zoom_ee_object(self, item):
        """Log a zoom to an Earth Engine object."""
        self.zoom_ee_object_calls.append(item)

    def add_layer(self, layer, key=None):
        """Log a layer addition."""
        self.added_layers.append((layer, key))

    def remove_layer(self, key, none_ok=False):
        """Log a layer removal by key."""
        self.removed_layer_keys.append(key)


class FakeGdf:
    """Duck-typed stand-in for a GeoDataFrame (bounds only)."""

    def __init__(self, total_bounds):
        """Hand back the canned WGS84 ``total_bounds``."""
        self.total_bounds = total_bounds


class FakeAoi:
    """Duck-typed stand-in for pysepal's AoiResult."""

    def __init__(self, gdf=None, feature_collection=None):
        """Carry either a vector geometry, a GEE collection, or neither."""
        self.gdf = gdf
        self.feature_collection = feature_collection


def test_vector_aoi_zooms_to_total_bounds():
    """A vector AOI frames the map on its WGS84 total_bounds."""
    m = FakeMap()
    bounds = (12.403, 43.893, 12.517, 43.993)
    aoi = FakeAoi(gdf=FakeGdf(bounds))

    assert zoom_map_to_aoi(m, aoi) is True
    assert m.zoom_bounds_calls == [bounds]
    assert m.zoom_ee_object_calls == []


def test_gee_only_aoi_zooms_to_feature_collection():
    """A GEE-lazy AOI frames the map through zoom_ee_object."""
    m = FakeMap()
    fc = object()  # stand-in for an ee.FeatureCollection
    aoi = FakeAoi(gdf=None, feature_collection=fc)

    assert zoom_map_to_aoi(m, aoi) is True
    assert m.zoom_ee_object_calls == [fc]
    assert m.zoom_bounds_calls == []


def test_gdf_takes_precedence_over_feature_collection():
    """Local geometry wins over the GEE round-trip when both are present."""
    m = FakeMap()
    bounds = (0.0, 0.0, 1.0, 1.0)
    aoi = FakeAoi(gdf=FakeGdf(bounds), feature_collection=object())

    assert zoom_map_to_aoi(m, aoi) is True
    assert m.zoom_bounds_calls == [bounds]
    assert m.zoom_ee_object_calls == []


def test_aoi_without_geometry_or_fc_is_noop():
    """An AOI carrying nothing to zoom to leaves the map alone."""
    m = FakeMap()
    aoi = FakeAoi(gdf=None, feature_collection=None)

    assert zoom_map_to_aoi(m, aoi) is False
    assert m.zoom_bounds_calls == []
    assert m.zoom_ee_object_calls == []


def test_none_aoi_is_noop():
    """No AOI, no zoom."""
    m = FakeMap()
    assert zoom_map_to_aoi(m, None) is False
    assert m.zoom_bounds_calls == []


def test_none_map_is_noop():
    """No map, no zoom."""
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
    """Drawing an AOI clears the stale layer under the same key first."""
    calls = []
    _fake_pysepal_mapping(monkeypatch, calls)

    m = FakeMap()
    gdf = FakeGdf((0.0, 0.0, 1.0, 1.0))
    aoi = FakeAoi(gdf=gdf)
    aoi.name = "san_marino"

    assert draw_aoi_on_map(m, aoi) is True
    assert m.removed_layer_keys == ["aoi"]  # clears stale layer first
    assert m.added_layers == [("layer:san_marino", "aoi")]
    assert calls == [(gdf, "san_marino", None)]


def test_draw_without_geometry_is_noop(monkeypatch):
    """A GEE-lazy AOI has no geometry to draw."""
    calls = []
    _fake_pysepal_mapping(monkeypatch, calls)

    m = FakeMap()
    assert draw_aoi_on_map(m, FakeAoi(gdf=None)) is False
    assert m.added_layers == []
    assert calls == []


def test_draw_none_inputs_are_noop():
    """Neither a missing map nor a missing AOI raises."""
    assert draw_aoi_on_map(None, FakeAoi(gdf=FakeGdf((0, 0, 1, 1)))) is False
    assert draw_aoi_on_map(FakeMap(), None) is False


def test_clear_project_overlays_removes_non_base_layers():
    """Drop every app overlay on a project switch, keep the basemaps.

    Overlays are the variables, samples, predictions and old AOI; base layers
    (basemaps) must survive.
    """
    m = FakeMap()
    clear_project_overlays(m)
    assert m.remove_all_calls == [{"base": False, "keep_names": None}]


def test_clear_project_overlays_noop_without_map():
    """Clearing overlays without a map must not raise."""
    clear_project_overlays(None)  # must not raise


# --- add_satellite_basemap --------------------------------------------------

SATELLITE_NAME = "Google Satellite"


class FakeTileLayer:
    """Duck-typed stand-in for an ipyleaflet TileLayer."""

    def __init__(self, name, base=True, visible=True):
        """Carry the traits the basemap helper reads and writes."""
        self.name = name
        self.base = base
        self.visible = visible


class FakeBasemapMap:
    """Stand-in for the SepalMap basemap API (layers tuple + add_basemap)."""

    def __init__(self, layers=()):
        """Start from the given base layers, logging nothing yet."""
        self.layers = tuple(layers)
        self.add_basemap_calls = []

    def add_basemap(self, basemap):
        """Append a base layer the way SepalMap.add_basemap does."""
        self.add_basemap_calls.append(basemap)
        self.layers = self.layers + (FakeTileLayer(SATELLITE_NAME),)


def _fake_pysepal_basemaps(monkeypatch):
    """Stub pysepal.mapping.basemaps so the lazy name lookup stays cheap."""
    module = types.ModuleType("pysepal.mapping.basemaps")
    module.basemap_tiles = {GOOGLE_SATELLITE: FakeTileLayer(SATELLITE_NAME)}
    monkeypatch.setitem(sys.modules, "pysepal.mapping.basemaps", module)


def test_satellite_is_added_hidden_next_to_the_cartodb_base(monkeypatch):
    """The user gets a second basemap to pick from.

    The theme-driven CartoDB layer stays the one actually drawn, so the map
    looks unchanged until satellite is selected.
    """
    _fake_pysepal_basemaps(monkeypatch)
    cartodb = FakeTileLayer("CartoDB.Positron")
    m = FakeBasemapMap([cartodb])

    assert add_satellite_basemap(m) is True
    assert m.add_basemap_calls == [GOOGLE_SATELLITE]

    bases = [lyr.name for lyr in m.layers if lyr.base]
    assert bases == ["CartoDB.Positron", SATELLITE_NAME]
    assert cartodb.visible is True
    assert m.layers[-1].visible is False


def test_satellite_is_not_added_twice(monkeypatch):
    """Re-running on the session-memoized map is a no-op."""
    _fake_pysepal_basemaps(monkeypatch)
    m = FakeBasemapMap([FakeTileLayer("CartoDB.Positron")])
    add_satellite_basemap(m)

    assert add_satellite_basemap(m) is False
    assert m.add_basemap_calls == [GOOGLE_SATELLITE]
    assert len(m.layers) == 2


def test_add_satellite_basemap_noop_without_map():
    """No map, no basemap."""
    assert add_satellite_basemap(None) is False


# ---------------------------------------------------------------------------
# sync_draw_control_visibility — AOI draw control shown only on the AOI tab
# ---------------------------------------------------------------------------


class FakeDrawControl:
    """Duck-typed GeomanDrawControl that records data-destroying calls."""

    def __init__(self):
        """Start with drawn data and no destructive calls."""
        self.data = [{"type": "Feature"}]
        self.cleared = 0

    def clear(self):
        """Log a data wipe (the helper must never trigger one)."""
        self.cleared += 1
        self.data = []


class FakeDcMap:
    """Duck-typed SepalMap: a dc plus the controls add/remove API."""

    def __init__(self, dc_on_map=True):
        """Optionally start with the draw control mounted."""
        self.dc = FakeDrawControl()
        self.controls = [self.dc] if dc_on_map else []

    def add_control(self, control):
        """Mount a control."""
        self.controls.append(control)

    def remove_control(self, control):
        """Unmount a control."""
        self.controls.remove(control)


def test_leaving_aoi_tab_hides_the_draw_control():
    """Leaving the AOI tab pulls the mounted dc off the map and remembers it."""
    m = FakeDcMap(dc_on_map=True)

    hidden = sync_draw_control_visibility(m, aoi_active=False, was_hidden=False)

    assert hidden is True
    assert m.dc not in m.controls


def test_returning_to_aoi_tab_restores_the_draw_control():
    """Coming back re-mounts a previously hidden dc with its data intact."""
    m = FakeDcMap(dc_on_map=False)

    hidden = sync_draw_control_visibility(m, aoi_active=True, was_hidden=True)

    assert hidden is False
    assert m.dc in m.controls
    assert m.dc.data  # drawn shape survives the round trip
    assert m.dc.cleared == 0


def test_helper_never_wipes_drawn_data():
    """Neither direction may call dc.clear() (hide()/show() do — helper must not)."""
    m = FakeDcMap(dc_on_map=True)

    hidden = sync_draw_control_visibility(m, aoi_active=False, was_hidden=False)
    sync_draw_control_visibility(m, aoi_active=True, was_hidden=hidden)

    assert m.dc.cleared == 0
    assert m.dc.data


def test_leaving_without_a_mounted_dc_reports_not_hidden():
    """Non-DRAW methods have no dc on the map; nothing to hide or restore."""
    m = FakeDcMap(dc_on_map=False)

    assert sync_draw_control_visibility(m, aoi_active=False, was_hidden=False) is False


def test_project_switch_while_away_drops_the_hidden_flag():
    """A project load can unmount the dc while away; the flag must not linger.

    Only a project switch may drop it: the load's restore decides the dc's
    fate itself, so a remembered hide from the previous project is stale.
    """
    m = FakeDcMap(dc_on_map=False)

    assert (
        sync_draw_control_visibility(
            m, aoi_active=False, was_hidden=True, project_switched=True
        )
        is False
    )


def test_rerun_while_away_preserves_the_hidden_flag():
    """Effect re-runs while away (non-AOI tab moves, loading flips) keep the flag.

    Regression: recomputing the flag from the map on every away-run clobbered
    it (dc already absent -> False), so the return-side re-add never fired and
    the toolbar appeared exactly once per project load.
    """
    m = FakeDcMap(dc_on_map=True)

    hidden = sync_draw_control_visibility(m, aoi_active=False, was_hidden=False)
    assert hidden is True  # left the AOI tab: dc removed, remembered

    # moving between two non-AOI tabs / a loading flip: dc absent, no switch
    hidden = sync_draw_control_visibility(m, aoi_active=False, was_hidden=hidden)
    assert hidden is True  # memory preserved

    hidden = sync_draw_control_visibility(m, aoi_active=True, was_hidden=hidden)
    assert hidden is False
    assert m.dc in m.controls  # toolbar comes back
    assert m.dc.cleared == 0 and m.dc.data


def test_project_switch_on_return_skips_the_readd():
    """Returning in the same run a switch landed lets the restore decide."""
    m = FakeDcMap(dc_on_map=False)

    hidden = sync_draw_control_visibility(
        m, aoi_active=True, was_hidden=True, project_switched=True
    )
    assert hidden is False
    assert m.controls == []  # the new project's restore owns the dc now


def test_returning_without_hidden_flag_adds_nothing():
    """Coming back after leaving a non-DRAW selection leaves the map alone."""
    m = FakeDcMap(dc_on_map=False)

    assert sync_draw_control_visibility(m, aoi_active=True, was_hidden=False) is False
    assert m.controls == []


def test_sync_draw_control_noop_without_map():
    """No map (or no dc) passes the flag through untouched."""
    assert sync_draw_control_visibility(None, aoi_active=False, was_hidden=True) is True

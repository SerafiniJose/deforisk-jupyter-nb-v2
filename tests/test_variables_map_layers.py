"""Tests for displaying local variable layers on the map and the black/white
visualization that replaced randomVisualizer."""

import asyncio
import inspect


def _named(name, **attrs):
    """Build a throwaway object whose ``type(obj).__name__`` equals ``name``."""
    return type(name, (), attrs)()


def test_is_mappable_predicate():
    """A variable is mappable if it carries a GEE image or is a local raster/vector."""
    from gui.scripts.map_helpers import is_mappable

    assert is_mappable(_named("GEEVar", gee_images=["img"])) is True
    assert is_mappable(_named("LocalRasterVar", gee_images=None)) is True
    assert is_mappable(_named("LocalVectorVar", gee_images=None)) is True
    # A GEE variable with no fetched image and an unknown type are not mappable.
    assert is_mappable(_named("GEEVar", gee_images=None)) is False
    assert is_mappable(_named("SomethingElse", gee_images=None)) is False


def test_styled_layer_categorical_is_black_white_not_random():
    """Categorical rasters must render as a 0=black, 1=white palette — never the
    old ``ee.Image.randomVisualizer()`` (random RGB)."""
    from gui.tile.variables_tile import _styled_layer

    class FakeImage:
        def randomVisualizer(self):  # pragma: no cover - must never be reached
            raise AssertionError("randomVisualizer must not be used")

    var = _named("LocalRasterVar", raster_type=_named("RT", value="categorical"))
    image = FakeImage()

    out_image, vis = asyncio.run(_styled_layer(image, var, None))

    assert out_image is image
    assert vis.get("palette") == ["000000", "ffffff"]
    assert vis.get("min") == 0
    assert vis.get("max") == 1


def test_add_vector_on_map_registers_layer_under_key(tmp_path):
    """A local vector file is drawn as a GeoJSON layer registered under its key
    (replacing any prior layer with the same key)."""
    import geopandas as gpd
    from shapely.geometry import box

    from gui.scripts.map_helpers import add_vector_on_map

    gdf = gpd.GeoDataFrame({"geometry": [box(0, 0, 1, 1)]}, crs="EPSG:4326")
    path = tmp_path / "v.geojson"
    gdf.to_file(path, driver="GeoJSON")

    calls = {}

    class FakeMap:
        def remove_layer(self, key, none_ok=False):
            calls["removed"] = key

        def add_layer(self, layer, key=""):
            calls["added"] = key

    add_vector_on_map(FakeMap(), str(path), "myvar", "var_myvar")

    assert calls["removed"] == "var_myvar"
    assert calls["added"] == "var_myvar"


def test_variables_tile_does_not_render_derived_list():
    """The Variables tile must not show derived (processed) variables — that list
    now lives only in the Process tile."""
    from gui.tile import variables_tile

    src = inspect.getsource(variables_tile.VariablesTile)
    assert "DerivedVariableList" not in src

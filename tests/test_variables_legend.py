"""Source-variable layers publish and withdraw legends."""

from gui.tile import variables_tile


class _Var:
    """Minimal stand-in for a source variable object."""

    def __init__(self, name="slope", raster_type="continuous"):
        """Build a fake variable with the fields the legend helpers read."""
        self.name = name
        self.raster_type = raster_type
        self.tags = []
        self.processing_history = []
        self.path = "/tmp/x.tif"


def _fake_legend_port():
    """A minimal LegendPort double that records calls.

    Mirrors tests/test_density_map.py and tests/test_inference_map_layers.py's
    ``_fake_legend_port`` — tiles get an explicit handle, not the app_state
    singleton.
    """
    from gui.scripts.legend_registry import LegendPort

    registered = []
    unregistered = []
    return (
        LegendPort(
            register=lambda *legends: registered.extend(legends),
            unregister=lambda *ids: unregistered.extend(ids),
            generation=lambda: 0,
        ),
        registered,
        unregistered,
    )


def test_var_legend_for_a_gee_layer_uses_the_vis_palette():
    """A GEE-backed layer's legend uses the vis dict's palette/min/max."""
    vis = {"palette": ["1a9850", "ffffbf", "d73027"], "min": 0, "max": 60}
    legend = variables_tile._var_legend(
        "slope", _Var(), vis=vis, render_kind="catalogue_palette"
    )
    assert legend.layer_id == variables_tile._map_layer_key("slope")
    assert legend.spec.kind == "gradient"
    assert len(legend.spec.colors) == 256


def test_var_legend_for_a_local_raster_uses_the_style_resolver():
    """A local raster's legend resolves its style instead of a vis dict.

    Uses a name outside the predefined catalogue so the label falls back to
    the literal key (see the next test for the catalogue-match case).
    """
    legend = variables_tile._var_legend("custom_raster", _Var(name="custom_raster"))
    assert legend.spec.kind == "gradient"
    assert legend.label.literal == "custom_raster"


def test_var_legend_label_prefers_the_catalogue_key_when_predefined():
    """A predefined variable's label uses the catalogue's label_key."""
    legend = variables_tile._var_legend("slope", _Var())
    assert legend.label.key in ("", "vars.predefined.slope")


def test_drop_from_map_removes_the_layer_and_unregisters_the_legend():
    """_drop_from_map is the single chokepoint: map removal + unregister."""
    removed = []

    class FakeMap:
        def remove_layer(self, key, none_ok=False):
            """Record the removed layer key."""
            removed.append(key)

    port, _registered, unregistered = _fake_legend_port()
    variables_tile.vars_on_map.set({"slope"})

    variables_tile._drop_from_map("slope", FakeMap(), port)

    assert removed == [variables_tile._map_layer_key("slope")]
    assert unregistered == [variables_tile._map_layer_key("slope")]
    assert "slope" not in variables_tile.vars_on_map.value


def test_drop_from_map_tolerates_a_missing_port():
    """A None legend_port is a no-op, not a crash — tiles render without one."""
    removed = []

    class FakeMap:
        def remove_layer(self, key, none_ok=False):
            """Record the removed layer key."""
            removed.append(key)

    variables_tile.vars_on_map.set({"slope"})

    variables_tile._drop_from_map("slope", FakeMap(), None)

    assert removed == [variables_tile._map_layer_key("slope")]
    assert "slope" not in variables_tile.vars_on_map.value

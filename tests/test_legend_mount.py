"""The legend component reflects the registry and translates at render time."""

from gui.scripts.legend_data import Label, LegendSpec
from gui.scripts.legend_registry import LayerLegend


def _legend(layer_id, title_key):
    """Build a minimal gradient LayerLegend for the given layer/title key."""
    return LayerLegend(
        layer_id=layer_id,
        label=Label(literal=layer_id),
        spec=LegendSpec(
            kind="gradient",
            title=Label(key=title_key),
            colors=("#000000", "#ffffff"),
            labels=(Label(key="legend.risk.low"), Label(key="legend.risk.high")),
        ),
    )


def test_map_legend_props_track_the_registry():
    """The component's props are derived, not cached: selection drives the body."""
    from gui.solara_app import legend_props
    from gui.store.state_manager import AppState

    def fake_t(key, **fmt):
        """Return a marker string identifying the translation key used."""
        return f"<{key}>"

    state = AppState()
    state.register_legends(_legend("a", "legend.prediction.title"))
    state.register_legends(_legend("b", "legend.density.title"))

    props = legend_props(state, fake_t)
    assert props["selected"] == "b"
    assert [option["value"] for option in props["selector_options"]] == ["a", "b"]
    assert props["legend_data"]["gradients"][0]["title"] == "<legend.density.title>"

    state.selected_legend.set("a")
    props = legend_props(state, fake_t)
    assert props["legend_data"]["gradients"][0]["title"] == "<legend.prediction.title>"


def test_map_legend_props_are_empty_when_nothing_is_registered():
    """With no registered legends, props render nothing and select nothing."""
    from gui.solara_app import legend_props
    from gui.store.state_manager import AppState

    props = legend_props(AppState(), lambda key, **fmt: key)
    assert props["legend_data"] == {}
    assert props["selector_options"] == []
    assert props["selected"] == ""


def _find_legend_widget(widget):
    """Depth-first search for the widget exposing pysepal's legend traits.

    Walks ``children`` (reacton's rendered ipywidgets tree) rather than
    asserting on a specific class, so a component_vue rename doesn't need a
    matching test rewrite — only its trait names matter here.
    """
    if hasattr(widget, "trait_names") and "legend_data" in widget.trait_names():
        return widget
    for child in list(getattr(widget, "children", []) or []):
        found = _find_legend_widget(child)
        if found is not None:
            return found
    return None


def test_map_legend_renders_with_populated_props():
    """MapLegend actually renders pysepal's LegendComponent with live data.

    Regression guard for the seam ``legend_props`` alone can't cover: a
    misspelled or renamed pysepal ``component_vue`` prop (``legend_data``,
    ``selector_options``, ``selected``) would fail silently at runtime while
    the pure-function test above stayed green. This renders ``MapLegend``
    with a populated ``AppState`` and asserts the realized widget's own
    trait values, not just what ``legend_props`` computed.
    """
    import reacton

    from gui.solara_app import MapLegend
    from gui.store.state_manager import AppState

    state = AppState()
    state.register_legends(_legend("a", "legend.prediction.title"))
    state.register_legends(_legend("b", "legend.density.title"))

    box, rc = reacton.render(MapLegend(state))
    try:
        widget = _find_legend_widget(box)
        assert widget is not None, "no widget exposing legend_data was rendered"
        assert widget.selected == "b"
        assert [opt["value"] for opt in widget.selector_options] == ["a", "b"]
        assert widget.legend_data["gradients"][0]["title"]
    finally:
        rc.close()

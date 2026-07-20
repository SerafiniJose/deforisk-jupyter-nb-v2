"""Unit tests for the shared ProductTable widget's pure helpers."""


def test_action_icons_standardized():
    from gui.widget.product_table import action_icon

    assert action_icon("map_toggle", is_on=False) == "mdi-map-plus"
    assert action_icon("map_toggle", is_on=True) == "mdi-map-minus"
    assert action_icon("edit") == "mdi-pencil-outline"
    assert action_icon("delete") == "mdi-delete-outline"
    assert action_icon("download") == "mdi-cloud-download-outline"
    assert action_icon("cancel") == "mdi-stop-circle"
    assert action_icon("open") == "mdi-table-eye"
    assert action_icon("dismiss") == "mdi-close"


def test_action_colors_are_theme_tokens_never_literal_grey():
    """The off-state map toggle must not be a literal grey.

    "grey darken-1" renders nearly black on the dark theme, which reads as a
    *disabled* button — the icon is enabled and clickable. Returning None lets
    Vuetify use the theme's default text colour (white on dark, dark on light),
    matching the edit/delete icons beside it.
    """
    from gui.widget.product_table import action_color

    assert action_color("map_toggle", is_on=False) is None
    assert action_color("map_toggle", is_on=True) == "primary"
    assert action_color("edit") is None
    assert action_color("delete") is None
    assert action_color("download") == "primary"
    assert action_color("open") == "primary"
    # An explicit per-action override still wins.
    assert action_color("delete", override="error") == "error"


def test_grid_style_builds_template_columns():
    from gui.widget.product_table import grid_style

    style = grid_style(["minmax(0,1fr)", "90px", "112px"])
    assert "display:grid" in style
    assert "grid-template-columns:minmax(0,1fr) 90px 112px;" in style


def test_status_maps_cover_all_states():
    from gui.widget.product_table import STATUS_COLORS, STATUS_ICONS

    for status in ("running", "ready", "trained", "completed", "failed", "cancelled"):
        assert status in STATUS_ICONS
        assert status in STATUS_COLORS
    assert STATUS_ICONS["running"] == "mdi-loading mdi-spin"
    assert STATUS_ICONS["completed"] == "mdi-check-circle"
    # Status tones are Vuetify theme tokens, not literal colours, so they track
    # the app palette in light and dark ("cancelled" stays a neutral grey).
    assert STATUS_COLORS["failed"] == "error"
    assert STATUS_COLORS["ready"] == "success"


def test_product_table_component_importable():
    from gui.widget.product_table import ProductTable

    assert callable(ProductTable)

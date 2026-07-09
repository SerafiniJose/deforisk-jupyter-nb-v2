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


def test_grid_style_builds_template_columns():
    from gui.widget.product_table import grid_style

    style = grid_style(["minmax(0,1fr)", "90px", "112px"])
    assert "display:grid" in style
    assert "grid-template-columns:minmax(0,1fr) 90px 112px;" in style


def test_status_maps_cover_all_states():
    from gui.widget.product_table import STATUS_COLORS, STATUS_ICONS

    for status in ("running", "ready", "trained", "failed", "cancelled"):
        assert status in STATUS_ICONS
        assert status in STATUS_COLORS
    assert STATUS_ICONS["running"] == "mdi-loading mdi-spin"
    assert STATUS_COLORS["failed"] == "red"


def test_product_table_component_importable():
    from gui.widget.product_table import ProductTable

    assert callable(ProductTable)

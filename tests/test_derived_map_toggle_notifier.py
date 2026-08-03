"""The shared derived-layer toggle reports through a notifier, not a reactive.

The hook stays free of use_notifications() so it is callable from a test with no
NotificationProvider mounted — the caller injects its own notifier.
"""

import inspect

from gui.tile.derived_map import use_derived_map_toggle


def test_hook_takes_a_notifier():
    """Verify the hook takes project, map_, and notifier parameters."""
    params = list(inspect.signature(use_derived_map_toggle).parameters)
    assert params == ["project", "map_", "notifier"]


def test_hook_toasts_the_toggle_failure():
    """Verify the hook uses notifier.error() with ERROR_TOAST_TIMEOUT."""
    src = inspect.getsource(use_derived_map_toggle)
    assert "process_error" not in src
    assert "notifier.error(" in src
    assert "ERROR_TOAST_TIMEOUT" in src
    assert "tiles.variables.error_toggle_map" in src


def test_both_tiles_pass_their_notifier():
    """Verify both tiles pass their notifier object to the hook."""
    from gui.tile import postprocess_tile, process_tile

    for module in (process_tile, postprocess_tile):
        src = inspect.getsource(module)
        assert "use_derived_map_toggle(project, map_, notifications)" in src


def test_processed_toggle_labels_layers_by_origin():
    """The shared processed toggle must label its layers with an origin marker.

    It serves both the Harmonization and Derived-layers tabs, so it
    classifies per variable rather than using one fixed marker.
    """
    src = inspect.getsource(use_derived_map_toggle)
    assert "processed_layer_label(p, key)" in src
    assert "layer_name=key," not in src  # bare key must be gone
    assert "add_vector_on_map, map_, str(var.path), key," not in src

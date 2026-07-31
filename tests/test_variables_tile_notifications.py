"""The Variables tile reports failures as pysepal toasts, not app_state reactives.

Rendering the whole tile needs a project, a map and the GEE stack, so the wiring
is asserted at the source level — the same approach as test_process_tile_wiring
and test_delete_confirm.
"""

import inspect

from gui.tile.variables_tile import VariablesTile


def test_variables_tile_has_no_process_error_parameter():
    """VariablesTile signature no longer accepts process_error parameter."""
    sig = inspect.signature(VariablesTile.f)  # .f — the undecorated component
    assert "process_error" not in sig.parameters


def test_variables_tile_toasts_every_failure_path():
    """All failure paths in VariablesTile use notifications.error, not process_error."""
    src = inspect.getsource(VariablesTile.f)
    assert "process_error" not in src, "no reactive writes may survive"
    # Download is a tracked job: its message goes through tracked_job.
    assert "error_format=" in src
    # Every direct toast passes the shared dwell constant.
    assert src.count("notifications.error(") == src.count("ERROR_TOAST_TIMEOUT")
    for key in (
        "tiles.variables.error_download",
        "tiles.variables.error_toggle_map",
        "tiles.variables.error_no_project",
        "tiles.variables.error_base_raster_reset",
        "tiles.variables.error_base_raster_removed",
        "tiles.variables.error_add_variable",
        "tiles.variables.error_save_variable",
    ):
        assert key in src, f"{key} lost its call site"

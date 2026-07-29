"""ToolboxTile wiring (browserless, structural)."""

import inspect

import solara

from gui.tile import toolbox_tile


def test_module_exposes_job_and_map_reactives():
    """Both survive re-renders, so the shell can reset them on project switch."""
    assert isinstance(toolbox_tile.allocation_jobs, solara.Reactive)
    assert isinstance(toolbox_tile.density_on_map, solara.Reactive)
    assert toolbox_tile.allocation_jobs.value == []


def test_worker_runs_in_context_with_tracked_job_and_writing():
    """The worker follows the app's background-job contract."""
    src = inspect.getsource(toolbox_tile)
    assert "spawn_in_context" in src
    assert "tracked_job" in src
    assert "writing(" in src
    assert "update_job" in src


def test_tile_takes_project_reactive_not_app_state():
    """Tiles receive the project reactive directly (see the tile contract)."""
    sig = inspect.signature(toolbox_tile.ToolboxTile.f)
    assert list(sig.parameters)[0] == "project"
    assert "app_state" not in inspect.getsource(toolbox_tile)


def test_tile_mounts_with_a_project():
    """The tile renders end to end without a map or a client."""
    import reacton

    from gui.i18n import t
    from spatialrisk.project import Project

    t("common.cancel")  # warm the translator before the first render
    box, _rc = reacton.render(
        toolbox_tile.ToolboxTile(project=solara.reactive(Project(project_name="p")))
    )
    assert box is not None

"""Dirty/last-saved tracking and project-switch resets on AppState."""

from datetime import datetime

from gui.store.state_manager import AppState


class _P:
    def __init__(self, name="p"):
        self.project_name = name


def test_starts_clean():
    """A fresh AppState has no dirty flag and no last-saved time."""
    s = AppState()
    assert s.project_dirty.value is False
    assert s.last_saved.value is None


def test_mutation_marks_dirty():
    """Setting a project marks the state dirty."""
    s = AppState()
    s.project.set(_P())
    assert s.project_dirty.value is True


def test_mark_saved_clears_dirty_and_records_time():
    """mark_saved clears dirty and stamps last_saved."""
    s = AppState()
    s.project.set(_P())
    when = datetime(2026, 6, 17, 12, 0, 0)
    s.mark_saved(when)
    assert s.project_dirty.value is False
    assert s.last_saved.value == when


def test_load_state_is_clean():
    """load_project_state installs a project without marking it dirty."""
    s = AppState()
    when = datetime(2026, 6, 17, 12, 0, 0)
    s.load_project_state(_P("loaded"), when)
    assert s.project.value is not None
    assert s.project_dirty.value is False
    assert s.last_saved.value == when


def test_new_project_resets_context_and_marks_dirty():
    """new_project_state installs a dirty, unsaved project and resets context."""
    s = AppState()
    s.aoi_result.set("stale-aoi")
    s.process_error.set("old-error")
    s.new_project_state(_P("fresh"))
    assert s.project.value.project_name == "fresh"
    assert s.project_dirty.value is True
    assert s.last_saved.value is None
    assert s.aoi_result.value is None
    assert s.process_error.value is None


def test_new_project_bumps_loaded_signal():
    """New project must fire the same on-switch effects as a load.

    Map/overlay reset, job-list rebuild — all driven by project_loaded_signal.
    """
    s = AppState()
    before = s.project_loaded_signal.value
    s.new_project_state(_P("fresh"))
    assert s.project_loaded_signal.value == before + 1


def test_new_project_resets_aoi_result():
    """new_project_state clears any previously captured AOI result."""
    from gui.store.state_manager import AppState
    from spatialrisk.project import Project

    s = AppState()
    s.aoi_result.set(object())
    s.new_project_state(Project(project_name="p"))
    assert s.aoi_result.value is None


def test_close_project_state_returns_to_empty():
    """close_project_state tears the open project back down to no-project."""
    s = AppState()
    s.project.set(_P("GUY"))
    s.last_saved.set(datetime(2026, 7, 14, 12, 0, 0))
    s.aoi_result.set(object())
    before = s.project_loaded_signal.value

    s.close_project_state()

    assert s.project.value is None
    assert s.project_dirty.value is False  # the subscription clears it
    assert s.last_saved.value is None
    assert s.aoi_result.value is None
    # The bump re-runs the shell's on-switch effects (map overlays, jobs, log).
    assert s.project_loaded_signal.value == before + 1


def test_close_project_state_leaves_status_message_for_the_caller():
    """close_project_state does not clear status_message.

    close() is followed by "Project 'X' deleted." — clearing here would race it.
    """
    s = AppState()
    s.project.set(_P("GUY"))
    s.status_message.set("Project 'GUY' deleted.")
    s.close_project_state()
    assert s.status_message.value == "Project 'GUY' deleted."

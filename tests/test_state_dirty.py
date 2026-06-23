from datetime import datetime

from gui.store.state_manager import AppState


class _P:
    def __init__(self, name="p"):
        self.project_name = name


def test_starts_clean():
    s = AppState()
    assert s.project_dirty.value is False
    assert s.last_saved.value is None


def test_mutation_marks_dirty():
    s = AppState()
    s.project.set(_P())
    assert s.project_dirty.value is True


def test_mark_saved_clears_dirty_and_records_time():
    s = AppState()
    s.project.set(_P())
    when = datetime(2026, 6, 17, 12, 0, 0)
    s.mark_saved(when)
    assert s.project_dirty.value is False
    assert s.last_saved.value == when


def test_load_state_is_clean():
    s = AppState()
    when = datetime(2026, 6, 17, 12, 0, 0)
    s.load_project_state(_P("loaded"), when)
    assert s.project.value is not None
    assert s.project_dirty.value is False
    assert s.last_saved.value == when


def test_new_project_resets_context_and_marks_dirty():
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
    """New project must fire the same on-switch effects as a load (map/overlay
    reset, job-list rebuild), all driven by project_loaded_signal."""
    s = AppState()
    before = s.project_loaded_signal.value
    s.new_project_state(_P("fresh"))
    assert s.project_loaded_signal.value == before + 1


def test_new_project_resets_aoi_asset():
    from gui.store.state_manager import AppState
    from spatialrisk.project import Project

    s = AppState()
    s.aoi_asset.set({"asset_id": "users/me/x", "type": "TABLE", "column": "ALL", "value": None})
    s.new_project_state(Project(project_name="p"))
    assert s.aoi_asset.value is None

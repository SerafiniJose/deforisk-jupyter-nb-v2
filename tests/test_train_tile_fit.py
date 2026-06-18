# tests/test_train_tile_fit.py
import types
from pathlib import Path

from gui.tile.train_tile import build_fit_kwargs


def _dataset():
    target = types.SimpleNamespace(name="forest_loss_2015_2020")
    return types.SimpleNamespace(name="calibration", target=target)


def _project():
    return types.SimpleNamespace(folders=types.SimpleNamespace(
        rmj_mw=Path("/tmp/rmj_mw"), rmj_bm=Path("/tmp/rmj_bm")))


def test_ml_models_get_empty_fit_kwargs():
    assert build_fit_kwargs("glm", _dataset(), _project()) == {}
    assert build_fit_kwargs("rf", _dataset(), _project()) == {}
    assert build_fit_kwargs("icar", _dataset(), _project()) == {}


def test_mw_gets_time_interval_and_folder():
    kw = build_fit_kwargs("mw", _dataset(), _project())
    assert kw["time_interval"] == 5
    assert kw["folder"] == Path("/tmp/rmj_mw")


def test_jnr_gets_folder_only():
    kw = build_fit_kwargs("benchmark", _dataset(), _project())
    assert kw["folder"] == Path("/tmp/rmj_bm")
    assert "time_interval" not in kw     # JNR.fit() has no time_interval arg


def test_job_status_update_notifies_subscribers():
    """Regression: marking a job completed must fire reactive subscribers so the
    UI re-renders and the spinner stops.

    The original code mutated job dicts in place inside a shallow-copied list,
    so the old and new lists shared the same dicts. Solara's set() short-circuits
    when equals_extra(old, new) is True and never fires its listeners, leaving
    every model's status card stuck on a spinning 'running' icon even though
    training had finished.
    """
    import solara

    solara.settings.main.allow_global_context = True
    from gui.tile.train_tile import _update_job, train_jobs

    train_jobs.set(
        [{"id": "job1", "status": "running", "deviance": None, "n_samples": None}]
    )
    old_snapshot = train_jobs.value

    fires = []
    unsub = train_jobs.subscribe_change(lambda *a: fires.append(a))
    try:
        _update_job("job1", status="completed", deviance=12.5, n_samples=9000)

        # The subscriber MUST be notified — this is what stops the spinner.
        assert len(fires) == 1, "set() did not fire listeners (spinner stays stuck)"

        updated = next(j for j in train_jobs.value if j["id"] == "job1")
        assert updated["status"] == "completed"
        assert updated["deviance"] == 12.5
        assert updated["n_samples"] == 9000

        # The previously-published list must NOT be mutated (proves immutability).
        assert old_snapshot[0]["status"] == "running"
    finally:
        if callable(unsub):
            unsub()
        train_jobs.set([])


def test_job_status_update_skips_cancelled_job():
    """A finishing job must not overwrite a status the user already cancelled."""
    import solara

    solara.settings.main.allow_global_context = True
    from gui.tile.train_tile import _update_job, train_jobs

    train_jobs.set([{"id": "job1", "status": "cancelled", "deviance": None}])
    try:
        _update_job("job1", status="completed", deviance=12.5)
        assert train_jobs.value[0]["status"] == "cancelled"
    finally:
        train_jobs.set([])


def test_spawn_in_context_runs_target_without_active_context():
    """spawn_in_context falls back to a plain thread when no Solara context
    is active (e.g. unit tests) and still executes the target."""
    import threading
    from gui.scripts.solara_threads import spawn_in_context

    done = threading.Event()
    received = []

    def _target(a, b):
        received.append((a, b))
        done.set()

    thread = spawn_in_context(_target, ("x", 42))
    assert done.wait(timeout=5), "target did not run"
    thread.join(timeout=5)
    assert received == [("x", 42)]

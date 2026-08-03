"""build_fit_kwargs, train-job state updates, and model naming/overwrite rules."""

import types
from pathlib import Path

from gui.tile.train_tile import build_fit_kwargs


def _dataset():
    target = types.SimpleNamespace(name="forest_loss_2015_2020")
    return types.SimpleNamespace(name="calibration", target=target)


def _project():
    return types.SimpleNamespace(
        folders=types.SimpleNamespace(
            rmj_mw=Path("/tmp/rmj_mw"), rmj_bm=Path("/tmp/rmj_bm")
        )
    )


def test_ml_models_get_empty_fit_kwargs():
    """ML families fit on the attached dataset — no fit() kwargs."""
    assert build_fit_kwargs("glm", _dataset(), _project()) == {}
    assert build_fit_kwargs("rf", _dataset(), _project()) == {}
    assert build_fit_kwargs("icar", _dataset(), _project()) == {}


def test_mw_gets_time_interval_and_folder():
    """MW derives its time interval from the target name and gets a folder."""
    kw = build_fit_kwargs("mw", _dataset(), _project())
    assert kw["time_interval"] == 5
    assert kw["folder"] == Path("/tmp/rmj_mw")


def test_jnr_gets_folder_only():
    """The JNR benchmark gets only a folder."""
    kw = build_fit_kwargs("jnr", _dataset(), _project())
    assert kw["folder"] == Path("/tmp/rmj_bm")
    assert "time_interval" not in kw  # JNR.fit() has no time_interval arg


def test_job_status_update_notifies_subscribers():
    """Regression: marking a job completed must fire reactive subscribers.

    That firing is what makes the UI re-render and the spinner stop.
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


def test_training_publishes_project_for_rerender(monkeypatch):
    """Regression: a finished run must publish the mutated project on the reactive.

    Otherwise dependent tiles (Step 7 — Inference) never re-render to list
    the new model: register() mutates project.models in place; without
    project.set(p.model_copy()) the identity-equality reactive never fires,
    so the Inference model dropdown stays empty until an unrelated project
    edit forces a re-render.
    """
    import copy

    import solara

    solara.settings.main.allow_global_context = True
    from gui.tile import train_tile

    class _DummyModel:
        def __init__(self, **kwargs):
            self.deviance = 1.0
            self.n_samples = 7
            self.project = None
            self.name = None
            self._key = None

        def fit(self, **kwargs):
            return self

        def register(self, project, key=None, auto_save=True):
            self._key = key or "glm"
            project.models[self._key] = self

        def _model_key(self):
            return self._key or "glm"

    class _FakeProject:
        def __init__(self):
            self.models = {}
            self.project_name = "t"

        def model_copy(self):
            return copy.copy(self)

    monkeypatch.setitem(
        train_tile.MODEL_REGISTRY,
        "glm",
        {**train_tile.MODEL_REGISTRY["glm"], "class": _DummyModel},
    )

    fake = _FakeProject()
    project_reactive = solara.reactive(fake, equals=lambda a, b: a is b)
    train_tile.train_jobs.set(
        [{"id": "jobA", "status": "running", "deviance": None, "n_samples": None}]
    )

    fires = []
    unsub = project_reactive.subscribe_change(lambda *a: fires.append(a))
    try:
        train_tile._run_training(
            "jobA", "glm", {}, None, None, fake, project_reactive, "v1"
        )

        # The reactive MUST fire so the Inference tile re-renders.
        assert len(fires) == 1, "project.set() never fired — Step 7 won't re-render"
        # The published project carries the newly registered model under its
        # name-derived key.
        assert "glm_v1" in project_reactive.value.models
        assert project_reactive.value is not fake, "must publish a fresh copy"
    finally:
        if callable(unsub):
            unsub()
        train_tile.train_jobs.set([])


def test_sanitize_name_is_path_safe():
    """User-typed names must be normalised to a path/key-safe slug."""
    from gui.tile.train_tile import _sanitize_name

    assert _sanitize_name("My Model 1") == "My_Model_1"
    assert _sanitize_name("  glm/v2  ") == "glm_v2"
    assert _sanitize_name("a..b!!c") == "a_b_c"
    assert _sanitize_name("keep-this_one") == "keep-this_one"
    assert _sanitize_name("***") == ""  # nothing salvageable → empty
    assert _sanitize_name("") == ""


def test_storage_key_matches_base_formula():
    """The tile's key must mirror BaseRiskModel's {model_type}_{name} formula."""
    from gui.tile.train_tile import _storage_key

    assert _storage_key("glm", "v1") == "glm_v1"
    assert _storage_key("rf", "") == "rf"  # no name → bare model type


def _name_test_harness(monkeypatch):
    """Shared dummy model + project for the naming/overwrite tests."""
    import copy

    import solara

    solara.settings.main.allow_global_context = True
    from gui.tile import train_tile

    class _DummyModel:
        def __init__(self, **kwargs):
            self.deviance = 1.0
            self.n_samples = 7
            self.project = None
            self.name = None
            self._key = None

        def fit(self, **kwargs):
            # name MUST be set before fit() so the pickle filename includes it.
            assert self.name is not None, "model.name not set before fit()"
            return self

        def register(self, project, key=None, auto_save=True):
            self._key = key
            project.models[key] = self

        def _model_key(self):
            return self._key

    class _FakeProject:
        def __init__(self):
            self.models = {}
            self.project_name = "t"
            self.deleted = []

        def delete_model(self, key, auto_save=False):
            self.deleted.append(key)
            self.models.pop(key, None)

        def model_copy(self):
            return copy.copy(self)

    monkeypatch.setitem(
        train_tile.MODEL_REGISTRY,
        "glm",
        {**train_tile.MODEL_REGISTRY["glm"], "class": _DummyModel},
    )
    return train_tile, _FakeProject


def test_distinct_names_do_not_overwrite(monkeypatch):
    """Two models trained under different names coexist in the registry."""
    train_tile, _FakeProject = _name_test_harness(monkeypatch)

    fake = _FakeProject()
    train_tile.train_jobs.set(
        [
            {"id": "j1", "status": "running"},
            {"id": "j2", "status": "running"},
        ]
    )
    try:
        train_tile._run_training("j1", "glm", {}, None, None, fake, None, "v1")
        train_tile._run_training("j2", "glm", {}, None, None, fake, None, "v2")

        assert set(fake.models) == {"glm_v1", "glm_v2"}
        assert fake.deleted == []  # nothing was overwritten
    finally:
        train_tile.train_jobs.set([])


def test_same_name_overwrites_and_cleans_old_files(monkeypatch):
    """Re-training under an existing name replaces the old model.

    The old model is deleted first so its on-disk files are cleaned up,
    not orphaned.
    """
    train_tile, _FakeProject = _name_test_harness(monkeypatch)

    fake = _FakeProject()
    train_tile.train_jobs.set(
        [
            {"id": "j1", "status": "running"},
            {"id": "j2", "status": "running"},
        ]
    )
    try:
        train_tile._run_training("j1", "glm", {}, None, None, fake, None, "v1")
        first = fake.models["glm_v1"]
        train_tile._run_training("j2", "glm", {}, None, None, fake, None, "v1")

        assert fake.deleted == ["glm_v1"]  # old model removed first
        assert set(fake.models) == {"glm_v1"}  # single entry, replaced
        assert fake.models["glm_v1"] is not first  # by the new model
    finally:
        train_tile.train_jobs.set([])


def test_spawn_in_context_runs_target_without_active_context():
    """spawn_in_context falls back to a plain thread without a Solara context.

    No context is active in unit tests; the target must still execute.
    """
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


def test_prepare_samples_records_dataset_name():
    """Regression: ML-family models must record which dataset they trained on.

    Only MW/JNR set dataset_name in their own fit(), so the models list
    showed "—" for every GLM/RF/iCAR model.
    """
    from spatialrisk.mlmodels import GLMModel

    m = GLMModel()
    m.dataset = types.SimpleNamespace(
        name="calibration",
        target=types.SimpleNamespace(name="tgt"),
        features=[types.SimpleNamespace(name="x")],
        year=None,
        extract_at_points=lambda pts: "df",
    )
    m.sample = types.SimpleNamespace(load_points=lambda: None)
    df, formula = m._prepare_samples(formula="tgt ~ x")
    assert m.dataset_name == "calibration"


def test_registry_keys_match_the_core_model_type():
    """The GUI family token must equal the core model_type everywhere.

    Storage keys are '{registry key}_{name}' while predictions and core
    registration use '{model_type}_{name}'; a mismatch (the old 'benchmark'
    vs 'jnr') makes trained models undispatchable.
    """
    from gui.scripts.model_registry import MODEL_REGISTRY

    for key, entry in MODEL_REGISTRY.items():
        assert entry["class"].model_fields["model_type"].default == key

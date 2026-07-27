"""Wiring: Inference tab is list-first; the run form lives in a dialog."""

import inspect
import types


def test_naming_helpers_are_aliases():
    """The tile's private naming helpers stay bound to artifact_names."""
    from gui.scripts import artifact_names as an
    from gui.tile import inference_tile as it

    assert it._sanitize_pred_name is an.sanitize_key
    assert it._default_pred_name is an.default_pred_name
    assert it._prediction_name_exists is an.prediction_name_exists


def test_inference_tile_is_list_first_with_dialog():
    """The tile renders a list plus one dialog — no inline form left."""
    from gui.tile.inference_tile import InferenceTile

    src = inspect.getsource(InferenceTile)
    assert "PredictionFormDialog" in src
    assert "tiles.inference.new_button" in src
    # single entry point: the import modal and its button are gone
    assert "PredictionImportModal" not in src
    assert "import_modal_open" not in src
    # old inline form + overwrite dialog stay gone
    assert "pred_name_label" not in src
    assert "confirm_overwrite_title" not in src


def test_prediction_form_dialog_contract():
    """The dialog keeps the shared frame, the name field and both sources."""
    import gui.widget.prediction_form_dialog as mod

    src = inspect.getsource(mod)
    assert "CreationDialog" in src and "ArtifactNameField" in src
    assert "use_artifact_name" in src and "default_pred_name" in src
    assert "prediction_name_exists" in src
    # unified dialog: source kind slot + import mode
    assert "tiles.inference.source_label" in src
    assert "FileInputComponent" in src
    assert "sepal_client" in src
    # import previews the resolved (suffixed) key, it never replaces
    assert "resolve_import_key" in src


def test_worker_threads_the_forest_feature_to_the_runner(monkeypatch):
    """The dialog's choice has to survive the hop onto the worker thread.

    ``_run_inference`` swallows exceptions into the job row, so a dropped
    argument would surface only as a failed prediction at runtime.
    """
    from gui.scripts import inference_runner
    from gui.tile import inference_tile as it

    calls = []
    monkeypatch.setattr(
        inference_runner, "run_inference", lambda *a, **kw: calls.append((a, kw))
    )
    previous_jobs = it.inference_jobs.value
    try:
        it.inference_jobs.set([{"id": "j1", "status": "running", "error": None}])
        it._run_inference(
            "j1",
            "glm_glm_v1",
            "calibration",
            types.SimpleNamespace(project_name="p"),
            name="run_a",
            forest_feature="forest_gfc_tc75",
        )

        assert calls, "run_inference was never called"
        _args, kwargs = calls[0]
        assert kwargs["forest_feature"] == "forest_gfc_tc75"
        assert kwargs["name"] == "run_a"
        assert it.inference_jobs.value[0]["status"] == "completed"
    finally:
        it.inference_jobs.set(previous_jobs)


def test_tile_forwards_the_entrys_forest_feature():
    """on_submit carries the dialog's key through to the worker, absent or not."""
    from gui.tile.inference_tile import InferenceTile

    src = inspect.getsource(InferenceTile)
    assert 'entry.get("forest_feature")' in src

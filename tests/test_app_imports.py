def test_workflow_tabs_includes_process():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "ProcessTile" in src
    assert 'rv.Tab(children=["Process"]' in src


def test_workflow_tabs_includes_sampling():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "SamplingTile" in src
    assert 'rv.Tab(children=["Sampling"], disabled=not has_datasets)' in src


def test_workflow_tabs_gate_downstream_steps():
    """Train is now gated on sample sets; Inference/Evaluation unchanged."""
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert 'rv.Tab(children=["Train"], disabled=not has_samples)' in src
    assert 'rv.Tab(children=["Inference"], disabled=not has_models)' in src
    assert 'rv.Tab(children=["Evaluation"], disabled=not has_predictions)' in src


def test_train_tile_selects_sample_set():
    import inspect
    import gui.tile.train_tile as t
    src = inspect.getsource(t.TrainTile)
    assert "p.samples" in src


def test_page_restores_job_lists_on_load():
    """Loading a project rebuilds the Train/Inference session job lists so saved
    models/predictions appear in the GUI (regression for empty lists on load)."""
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.Page)
    assert "restore_jobs_on_load" in src
    assert "build_train_jobs" in src
    assert "build_inference_jobs" in src
    # Reconstruction must be driven by the on-switch signal.
    assert "solara.use_effect(restore_jobs_on_load, [project_loaded_signal])" in src


def test_page_clears_map_overlays_on_switch():
    """Switching projects clears the previous project's overlay layers and the
    per-tile on-map tracking so they don't leak onto the shared map."""
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.Page)
    assert "clear_project_overlays(sepal_map)" in src
    assert "vars_on_map.set(set())" in src
    assert "samples_on_map.set(set())" in src
    assert "preds_on_map.set(set())" in src
    assert "solara.use_effect(render_map_on_switch, [project_loaded_signal])" in src

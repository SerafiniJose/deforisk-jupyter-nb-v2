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

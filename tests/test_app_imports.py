def test_workflow_tabs_includes_process():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "ProcessTile" in src
    assert 'rv.Tab(children=["Process"]' in src


def test_workflow_tabs_gate_downstream_steps():
    """Train/Inference/Evaluation tabs are gated on their prerequisites."""
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert 'rv.Tab(children=["Train"], disabled=not has_datasets)' in src
    assert 'rv.Tab(children=["Inference"], disabled=not has_models)' in src
    assert 'rv.Tab(children=["Evaluation"], disabled=not has_predictions)' in src

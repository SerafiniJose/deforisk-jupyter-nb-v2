def test_workflow_tabs_includes_process():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "ProcessTile" in src
    assert 'rv.Tab(children=["Process"]' in src

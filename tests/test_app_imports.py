def test_workflow_tabs_uses_pipeline_header():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "PipelineHeader(" in src
    assert "on_navigate=set_active_tab" in src
    # The old strip and its hand-maintained gating are gone.
    assert "rv.Tab(" not in src
    assert "disabled_flags" not in src


def test_workflow_tabs_hosts_all_tiles_in_registry_order():
    """One rv.TabItem per registry step, tiles in canonical order — the
    registry is the single source of truth for step order."""
    import inspect
    import gui.solara_app as app
    from gui.store.workflow_steps import STEPS

    src = inspect.getsource(app.WorkflowTabs)
    assert src.count("with rv.TabItem():") == len(STEPS)
    tiles = ["AoiTile", "VariablesTile", "ProcessTile", "PostProcessTile",
             "DatasetTile", "SamplingTile", "TrainTile", "InferenceTile",
             "EvaluationTile"]
    positions = [src.index(t) for t in tiles]
    assert positions == sorted(positions), "tiles out of registry order"


def test_app_state_has_no_stale_current_step():
    from gui.store.state_manager import AppState
    assert not hasattr(AppState(), "current_step")


def test_train_tile_selects_dataset_and_sample():
    import inspect
    from gui.tile import train_tile
    src = inspect.getsource(train_tile)
    assert "selected_dataset" in src
    assert "has_sampling" in src
    assert "p.datasets" in src and "p.samples" in src
    # dataset no longer derived from the sample
    assert "sample_set.dataset_name" not in src


def test_page_resets_job_lists_on_load():
    """Switching projects clears the session job lists; product rows derive
    from the registries at render time (no job_restore facade)."""
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.Page)
    assert "reset_jobs_on_load" in src
    assert "build_train_jobs" not in src
    assert "build_inference_jobs" not in src
    assert "solara.use_effect(reset_jobs_on_load, [project_loaded_signal])" in src


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


def test_solara_app_imports_summary_tile():
    import gui.solara_app as app
    assert hasattr(app, "ProjectSummaryTile")


def test_page_wires_project_summary_step():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.Page)
    assert 't("app.step_project_summary")' in src
    assert "ProjectSummaryTile(" in src
    # Left-drawer step opens as a modal dialog.
    assert '"display": "dialog"' in src


def test_workflow_tabs_wires_aoi_asset():
    import inspect
    import gui.solara_app as solara_app
    src = inspect.getsource(solara_app.WorkflowTabs)
    assert "aoi_asset=app_state.aoi_asset" in src
    assert "on_selection=app_state.aoi_asset.set" in src
    assert "restore_signal=app_state.project_loaded_signal.value" in src


def test_aoi_tile_imports_vendored_view():
    import inspect
    import gui.tile.aoi_tile as aoi_tile
    assert "gui.widget.aoi_view" in inspect.getsource(aoi_tile)


def test_solara_app_installs_log_console_handler():
    import inspect
    import gui.solara_app as app
    assert "install_log_console_handler()" in inspect.getsource(app)


def test_page_renders_log_console():
    import inspect
    import gui.solara_app as app
    assert "LogConsole()" in inspect.getsource(app.Page)


def test_page_clears_log_on_switch():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.Page)
    assert "clear_log_records()" in src
    assert "solara.use_effect(reset_log_on_switch, [project_loaded_signal])" in src


def test_notification_compute_is_registry_driven():
    """No more hand-maintained `if tab == N` ladder — the step key comes from
    the STEPS registry, so reordering steps can't desync notifications."""
    import inspect
    from gui.widget import notification_area

    src = inspect.getsource(notification_area._compute)
    assert "tab ==" not in src
    assert "STEPS[" in src
    # Count/"run X first" messages moved into the pipeline header.
    assert "dataset_count" not in src
    assert "train_no_dataset" not in src

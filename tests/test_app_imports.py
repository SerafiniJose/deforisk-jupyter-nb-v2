def test_workflow_tabs_includes_process():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "ProcessTile" in src
    assert 'rv.Tab(children=[t("workflow.tab_process")]' in src


def test_workflow_tabs_includes_sampling():
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert "SamplingTile" in src
    assert 'rv.Tab(children=[t("workflow.tab_sampling")], disabled=not has_processed_raster)' in src


def test_workflow_tabs_gate_downstream_steps():
    """Train is now gated on datasets; Inference/Evaluation unchanged."""
    import inspect
    import gui.solara_app as app
    src = inspect.getsource(app.WorkflowTabs)
    assert 'rv.Tab(children=[t("workflow.tab_train")], disabled=not has_datasets)' in src
    assert 'rv.Tab(children=[t("workflow.tab_inference")], disabled=not has_models)' in src
    assert 'rv.Tab(children=[t("workflow.tab_evaluation")], disabled=not has_predictions)' in src


def test_tab_gating_sampling_on_raster_train_on_datasets():
    import inspect
    from gui import solara_app
    src = inspect.getsource(solara_app.WorkflowTabs)
    assert "has_processed_raster" in src
    # Sampling no longer gated on datasets; Train gated on datasets
    assert 'rv.Tab(children=[t("workflow.tab_sampling")], disabled=not has_processed_raster)' in src
    assert 'rv.Tab(children=[t("workflow.tab_train")], disabled=not has_datasets)' in src


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


def test_workflow_tabs_includes_postprocess():
    import inspect
    import gui.solara_app as app

    src = inspect.getsource(app.WorkflowTabs)
    assert "PostProcessTile" in src
    assert 'rv.Tab(children=[t("workflow.tab_postprocess")], disabled=not has_processed)' in src


def test_notification_ladder_renumbered_for_postprocess():
    import inspect
    from gui.widget import notification_area

    src = inspect.getsource(notification_area._compute)
    assert "elif tab == 3:  # Post-process" in src
    assert "elif tab == 4:  # Dataset" in src
    assert "elif tab == 8:  # Evaluation" in src

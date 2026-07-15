"""Wiring checks for the decoupled sampling tile."""
import inspect


def test_sampling_tile_uses_raster_and_mask():
    from gui.tile import sampling_tile
    src = inspect.getsource(sampling_tile)
    assert "raster_var_name" in src and "mask_var_name" in src
    assert "add_sample(" in src
    assert "allocation" in src
    # old dataset-driven flow is gone
    assert "dataset_name=" not in src
    assert "add_sample_set(" not in src          # old registry call gone
    assert "from spatialrisk.sampleset" not in src  # old SampleSet model import gone


def test_sample_set_list_widget_importable():
    import gui.widget.sample_set_list as w
    assert hasattr(w, "SampleSetList")


def test_sampling_tile_has_distance_mode():
    from gui.tile import sampling_tile
    src = inspect.getsource(sampling_tile)
    # systematic-only spacing mode wired into the form
    assert "spacing_m" in src
    assert "tiles.sampling.systematic_mode_spacing" in src
    assert 'strategy == "systematic"' in src
    # _run_sampling carries spacing_m between n_samples and seed (catches
    # positional-arg reordering vs the spawn_in_context call site)
    params = list(inspect.signature(sampling_tile._run_sampling).parameters)
    assert params.index("spacing_m") == params.index("n_samples") + 1
    assert params.index("spacing_m") == params.index("seed") - 1


def test_toggle_is_offloaded_and_idempotent():
    import inspect
    from gui.tile import sampling_tile as st

    assert hasattr(st, "samples_pending")
    src = inspect.getsource(st.SamplingTile)
    # toggle dispatches to a background worker and guards against double-clicks
    assert "spawn_in_context" in src
    assert "samples_pending" in src

    worker = inspect.getsource(st._toggle_sample_on_map)
    # prefers PMTiles, falls back to GeoJSON
    assert "pmtiles_path" in worker
    assert "add_sample_pmtiles_on_map" in worker
    assert "add_sample_points_on_map" in worker

    remover = inspect.getsource(st._remove_sample_layers)
    assert "remove_sample_pmtiles_from_map" in remover
    assert "remove_sample_points_from_map" in remover


def test_sampling_tile_design_first_and_autoname():
    import inspect
    from gui.tile import sampling_tile as st

    # pure helper is used by the component
    assert hasattr(st, "_suggest_name")

    src = inspect.getsource(st.SamplingTile)
    # name-dirty tracking exists
    assert "name_dirty" in src
    # existing-name set includes in-flight jobs, not just persisted samples
    assert "sampling_jobs" in src and "_suggest_name" in src
    # contextual raster label keys are referenced
    assert "raster_variable_label_strata" in src
    assert "raster_variable_label_area" in src

    # design (strategy) select is rendered before the raster select.
    # (Note: "raster_variable_label_" only appears in the hoisted raster_label
    # computation above on_generate, not at the render call site — the render
    # references the `raster_label` variable, so we anchor on that instead.)
    assert src.index("strategy_label") < src.index("label=raster_label")

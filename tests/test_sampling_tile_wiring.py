"""Wiring checks for the decoupled sampling tile."""
import inspect


def test_sampling_tile_uses_raster_and_mask():
    """Sampling tile wires raster/mask vars into Sample generation, not a dataset."""
    from gui.tile import sampling_tile

    src = inspect.getsource(sampling_tile)
    assert "raster_var_name" in src and "mask_var_name" in src
    assert "add_sample(" in src
    assert "allocation" in src
    # old dataset-driven flow is gone
    assert "dataset_name=" not in src
    assert "add_sample_set(" not in src  # old registry call gone
    assert "from spatialrisk.sampleset" not in src  # old SampleSet model import gone


def test_sample_set_list_widget_importable():
    """The list widget module exists and exposes SampleSetList."""
    import gui.widget.sample_set_list as w

    assert hasattr(w, "SampleSetList")


def test_sampling_tile_has_distance_mode():
    """Systematic spacing mode threads through the tile and its form dialog."""
    import gui.widget.sample_form_dialog as dlg
    from gui.tile import sampling_tile

    src = inspect.getsource(sampling_tile)
    # systematic-only spacing mode still threads through the tile's job spawn
    assert "spacing_m" in src

    # the mode select and its systematic-only guard now live in the form dialog
    dlg_src = inspect.getsource(dlg)
    assert "tiles.sampling.systematic_mode_spacing" in dlg_src
    assert 'strategy == "systematic"' in dlg_src

    # _run_sampling carries spacing_m between n_samples and seed (catches
    # positional-arg reordering vs the spawn_in_context call site)
    params = list(inspect.signature(sampling_tile._run_sampling).parameters)
    assert params.index("spacing_m") == params.index("n_samples") + 1
    assert params.index("spacing_m") == params.index("seed") - 1


def test_toggle_is_offloaded_and_idempotent():
    """Map toggle runs off-thread, guards double-clicks, and drops both layer kinds."""
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


def test_sampling_form_dialog_design_first_and_autoname():
    """The tile is list-first (New button + dialog) and the dialog is design-first."""
    import inspect

    import gui.widget.sample_form_dialog as dlg
    from gui.tile import sampling_tile as st

    # the tile still exposes the suggestion helper (compat alias)
    assert callable(st._suggest_name)
    tile_src = inspect.getsource(st.SamplingTile)
    # list-first: tile has the New button + dialog, not the form fields
    assert "SampleFormDialog" in tile_src
    assert "tiles.sampling.new_button" in tile_src
    assert "n_samples_label" not in tile_src

    src = inspect.getsource(dlg)
    # shared naming (suggested-until-edited) and the shared frame
    assert "use_artifact_name" in src and "CreationDialog" in src
    # contextual raster label preserved
    assert "raster_variable_label_strata" in src
    assert "raster_variable_label_area" in src
    # design (strategy) select renders before the raster select
    assert src.index("strategy_label") < src.index("raster_variable_label_")
    # seed is progressive-disclosed under Advanced
    assert "ExpansionPanel" in src and "seed_label" in src


def test_sampling_tile_opens_the_details_dialog():
    """Eye action → tile state → read-only dialog, mirroring TrainTile."""
    import inspect

    from gui.tile import sampling_tile as st

    src = inspect.getsource(st.SamplingTile)
    assert "SampleDetailsDialog" in src
    assert "on_open=set_details_key" in src
    assert "details_key, set_details_key = solara.use_state(None)" in src
    # Hooks must precede the tile's early returns (p is None / no raster keys),
    # or the hook count changes between renders.
    assert src.index("set_details_key = solara.use_state") < src.index("if p is None:")

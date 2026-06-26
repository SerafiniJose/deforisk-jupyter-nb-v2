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

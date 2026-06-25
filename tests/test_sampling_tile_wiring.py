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

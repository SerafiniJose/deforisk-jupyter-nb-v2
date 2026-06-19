"""Smoke/wiring tests for the Sampling tile and list widget (no render)."""

import inspect


def test_sampling_tile_module_exposes_component_and_reactives():
    import gui.tile.sampling_tile as st
    assert hasattr(st, "SamplingTile")
    assert hasattr(st, "sampling_jobs")
    assert hasattr(st, "samples_on_map")
    # The runner builds a SampleSet and calls generate().
    src = inspect.getsource(st._run_sampling)
    assert "SampleSet" in src
    assert ".generate()" in src
    assert "add_sample_set" in src


def test_sample_set_list_widget_importable():
    import gui.widget.sample_set_list as w
    assert hasattr(w, "SampleSetList")

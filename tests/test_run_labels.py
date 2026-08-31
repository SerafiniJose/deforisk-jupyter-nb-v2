"""Picker labels stay unique across MW models and named runs."""

import types

from spatialrisk.evaluation import run_label_for


def _pred(model_key, window=None, name=None, dataset="validation_2015_2020"):
    return types.SimpleNamespace(
        model_key=model_key, window=window, name=name, dataset_name=dataset
    )


def test_run_label_prefers_the_run_name():
    """A named run's label uses the run name, not the model key."""
    pred = _pred("mw_calib_a", window=5, name="val_2020")
    assert run_label_for(pred) == "MW_w5 · val_2020"


def test_run_label_falls_back_to_the_model_key():
    """An unnamed run's label falls back to the model key."""
    assert run_label_for(_pred("mw_calib_b", window=5)) == "MW_w5 · mw_calib_b"
    assert run_label_for(_pred("glm_baseline")) == "GLM · glm_baseline"


def test_map_items_are_unique_for_two_mw_models():
    """Two MW models predicting the same dataset get distinct picker labels."""
    from gui.tile.evaluation_helpers import map_items

    project = types.SimpleNamespace(
        predictions={
            "a__validation_w5": _pred("mw_calib_a", window=5),
            "b__validation_w5": _pred("mw_calib_b", window=5),
        }
    )
    texts = [i["text"] for i in map_items(project)]
    assert len(set(texts)) == 2
    assert "MW_w5 · mw_calib_a — validation_2015_2020" in texts


def test_allocation_run_source_uses_the_snapshot_name():
    """The allocation run-source label reflects the snapshot's saved name."""
    from gui.scripts.allocation_runner import _run_source

    record = types.SimpleNamespace(
        prediction_snapshot={
            "model_key": "mw_calib_a",
            "dataset_name": "validation_2015_2020",
            "window": 5,
            "name": "val_2020",
        }
    )
    assert _run_source(record) == "MW_w5 · val_2020 — validation_2015_2020"

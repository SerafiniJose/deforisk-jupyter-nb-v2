"""run_inference: the family-aware adapter from (model, dataset) to apply()."""

import types
from pathlib import Path

import pytest

from gui.scripts.inference_runner import run_inference


class _RecordingModel:
    def __init__(self):
        self.apply_calls = []

    def apply(self, *args, **kwargs):
        self.apply_calls.append((args, kwargs))
        return Path("/tmp/out.tif")


class _RecordingMW(_RecordingModel):
    def apply(self, *args, **kwargs):
        self.apply_calls.append((args, kwargs))
        return {5: Path("/tmp/mw_5.tif"), 11: Path("/tmp/mw_11.tif")}


def _project(model, model_key, with_forest=True):
    target = types.SimpleNamespace(
        name="forest_loss_2015_2020", path=Path("/tmp/d.tif")
    )
    feats = (
        [types.SimpleNamespace(name="forest_gfc", path=Path("/tmp/f.tif"))]
        if with_forest
        else []
    )
    dataset = types.SimpleNamespace(name="calibration", target=target, features=feats)
    folders = types.SimpleNamespace(
        glm_model=Path("/tmp/far_glm"),
        rf_model=Path("/tmp/far_rf"),
        icar_model=Path("/tmp/far_icar"),
        rmj_bm=Path("/tmp/rmj_bm"),
        rmj_mw=Path("/tmp/rmj_mw"),
    )
    return types.SimpleNamespace(
        models={model_key: model},
        get_dataset=lambda n: dataset,
        folders=folders,
    )


def test_ml_model_apply_gets_mask_and_output(tmp_path):
    """An ML family gets apply(output_file, dataset, mask, mask_value)."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1")
    run_inference(proj, "glm_glm_v1", "calibration")
    (args, kwargs) = m.apply_calls[0]
    # apply(output_file, dataset, mask, mask_value)
    assert str(args[0]).endswith("calibration.tif")
    assert args[1] is proj.get_dataset("calibration")  # dataset positional arg
    assert args[2] == Path("/tmp/f.tif")  # forest_gfc mask
    assert args[3] == 0  # mask_value


def test_ml_model_with_none_name_falls_back_to_model_key(tmp_path):
    """A model whose ``name`` is None falls back to model_key, not a crash.

    Real models (BaseRiskModel) default ``name`` to None. The old code used
    ``getattr(model, "name", model_key)``, whose default only applies when the
    attribute is *missing* — so an existing-but-None name returned None and
    ``Path(...) / None`` raised TypeError.
    """
    m = _RecordingModel()
    m.name = None
    proj = _project(m, "glm")
    run_inference(proj, "glm", "calibration")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/far_glm/glm")  # model_key is the fallback
    assert out_path.name == "calibration.tif"


def test_jnr_model_apply_gets_time_interval(tmp_path):
    """The JNR family gets its time interval derived from the target name."""
    m = _RecordingModel()
    proj = _project(m, "jnr_calibration_jnr")
    run_inference(proj, "jnr_calibration_jnr", "calibration")
    (args, kwargs) = m.apply_calls[0]
    assert str(args[0]).endswith(
        "prob_bm_calibration.tif"
    )  # output path positional arg
    assert args[1] is proj.get_dataset("calibration")  # dataset positional arg
    assert kwargs["time_interval"] == 5
    assert kwargs["deforate_model"] is None


def test_mw_model_apply_returns_multiple_and_uses_output_folder(tmp_path):
    """The MW family writes one raster per window into an output folder."""
    m = _RecordingMW()
    proj = _project(m, "mw_calibration_mw")
    run_inference(proj, "mw_calibration_mw", "calibration")
    (args, kwargs) = m.apply_calls[0]
    assert kwargs["time_interval"] == 5
    assert kwargs["output_folder"] == Path("/tmp/rmj_mw")


def test_ml_model_missing_forest_feature_raises():
    """An ML run without a forest feature fails its precondition, loudly."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", with_forest=False)
    with pytest.raises(ValueError, match="forest_gfc"):
        run_inference(proj, "glm_glm_v1", "calibration")


def test_named_run_uses_name_subfolder_and_sets_pending_name():
    """A named ML run gets its own subfolder and hands the name to the model.

    _register_prediction keys the prediction by that name.
    """
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1")
    run_inference(proj, "glm_glm_v1", "calibration", name="run_a")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/far_glm/run_a")  # name is the subfolder
    assert out_path.name == "calibration.tif"
    assert m._pending_pred_name == "run_a"


def test_named_jnr_run_uses_name_subfolder():
    """A named JNR run writes into a per-name subfolder too."""
    m = _RecordingModel()
    proj = _project(m, "jnr_calibration_jnr")
    run_inference(proj, "jnr_calibration_jnr", "calibration", name="bench1")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/rmj_bm/bench1")
    assert out_path.name == "prob_bm_calibration.tif"


def test_named_mw_run_uses_name_output_folder():
    """A named MW run nests its output folder under the run name."""
    m = _RecordingMW()
    proj = _project(m, "mw_calibration_mw")
    run_inference(proj, "mw_calibration_mw", "calibration", name="mwrun")
    (_args, kwargs) = m.apply_calls[0]
    assert kwargs["output_folder"] == Path("/tmp/rmj_mw/mwrun")
    assert m._pending_pred_name == "mwrun"

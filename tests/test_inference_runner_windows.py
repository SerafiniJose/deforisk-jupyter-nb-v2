"""run_inference forwards the Predict dialog's window subset to MW apply()."""

import types

from gui.scripts.inference_runner import (
    is_mw_family,
    mw_window_options,
    run_inference,
)


class _MWModel:
    def __init__(self):
        self.ldefrate_files = {"5": "a.tif", "11": "b.tif"}
        self.win_size_list = [5, 11]
        self.applied = None
        self.project = None
        self.dataset = None
        self._pending_pred_name = None

    def apply(self, dataset, time_interval, output_folder, windows=None):
        self.applied = {"time_interval": time_interval, "windows": windows}
        return {}


def _project(tmp_path, model):
    dataset = types.SimpleNamespace(
        name="validation",
        target=types.SimpleNamespace(name="forest_loss_2015_2020"),
    )
    return types.SimpleNamespace(
        models={"mw_calibration_mw": model},
        get_dataset=lambda key: dataset if key == "validation" else None,
        processed_variables={},
        folders=types.SimpleNamespace(rmj_mw=str(tmp_path)),
    )


def test_is_mw_family():
    """The key's family token distinguishes MW models from other families."""
    assert is_mw_family("mw_calibration_mw")
    assert not is_mw_family("glm_glm_v1")
    assert not is_mw_family(None)


def test_mw_window_options_prefers_trained_windows(tmp_path):
    """Trained windows win when present; else fall back to win_size_list."""
    model = _MWModel()
    project = _project(tmp_path, model)
    assert mw_window_options(project, "mw_calibration_mw") == [5, 11]
    model.ldefrate_files = {}
    model.win_size_list = [21, 5]
    assert mw_window_options(project, "mw_calibration_mw") == [5, 21]
    assert mw_window_options(project, "glm_glm_v1") == []


def test_run_inference_forwards_windows(tmp_path):
    """The MW branch forwards the caller's windows subset into apply()."""
    model = _MWModel()
    run_inference(
        _project(tmp_path, model),
        "mw_calibration_mw",
        "validation",
        name="val_2020",
        windows=[11],
    )
    assert model.applied == {"time_interval": 5, "windows": [11]}

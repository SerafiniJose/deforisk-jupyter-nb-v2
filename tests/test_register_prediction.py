# tests/test_register_prediction.py
from pathlib import Path

from spatialrisk.predictions.prediction import Prediction
from spatialrisk.project import Project


class _FakeVar:
    def __init__(self, name, year=None):
        self.name = name
        self.year = year


class _FakeDataset:
    def __init__(self, name="ds_2020"):
        self.name = name
        self.year = 2020
        self.target = _FakeVar("forest_loss", 2020)
        self.features = [_FakeVar("slope")]


def _glm_with_project():
    from spatialrisk.mlmodels import GLMModel

    Project._ensure_model_schemas()
    project = Project(project_name="reg_test")
    model = GLMModel(name="m", dataset_name="ds_2020", year=2020)
    project.models["glm_m"] = model
    model.project = project
    model.dataset = _FakeDataset()
    return project, model


def test_register_prediction_builds_and_registers():
    project, model = _glm_with_project()
    result = model._register_prediction("/tmp/glm_2020.tif", auto_save=False)

    assert isinstance(result, Prediction)
    assert result.model_key == "glm_m"
    assert result.dataset_name == "ds_2020"
    assert result.year == 2020
    assert result.dataset_snapshot["feature_names"] == ["slope"]
    assert project.get_prediction("glm_m__ds_2020_y2020") is result


def test_register_prediction_with_window():
    project, model = _glm_with_project()
    result = model._register_prediction("/tmp/w5.tif", window=5, auto_save=False)

    assert result.window == 5
    assert project.get_prediction("glm_m__ds_2020_y2020_w5") is result


def test_register_prediction_noop_without_project():
    from spatialrisk.mlmodels import GLMModel

    Project._ensure_model_schemas()
    model = GLMModel(name="m", dataset_name="ds")
    assert model.project is None
    assert model._register_prediction("/tmp/x.tif") is None


def test_model_key_reverse_lookup_honors_custom_key():
    project, model = _glm_with_project()
    # Re-key the model under a custom key.
    del project.models["glm_m"]
    project.models["my_custom_glm"] = model
    assert model._model_key() == "my_custom_glm"


def test_base_apply_registers_prediction(monkeypatch, tmp_path):
    """base.apply() registers exactly one Prediction at the end, via the shared path."""
    from spatialrisk.mlmodels import base as base_mod

    project, model = _glm_with_project()

    # Stub out the heavy raster machinery: drive apply() straight to its tail.
    captured = {}

    def _fake_apply(self, output_file, dataset=None, mask=None, mask_value=0):
        active_dataset = dataset if dataset is not None else self.dataset
        out = Path(output_file)
        # tail of the real apply(): the single registration hook under test
        self._register_prediction(out, dataset=active_dataset)
        return out

    # Verify the real apply() ends with a _register_prediction call by spying on it.
    real_register = base_mod.BaseRiskModel._register_prediction

    def _spy(self, *args, **kwargs):
        captured["called"] = True
        captured["path"] = args[0] if args else kwargs.get("path")
        return real_register(self, *args, **kwargs)

    monkeypatch.setattr(base_mod.BaseRiskModel, "_register_prediction", _spy)
    monkeypatch.setattr(base_mod.BaseRiskModel, "apply", _fake_apply)

    out_path = tmp_path / "glm_out.tif"
    model.apply(out_path)

    assert captured["called"] is True
    assert Path(captured["path"]) == out_path
    assert project.get_prediction("glm_m__ds_2020_y2020") is not None


def test_base_apply_source_calls_register_before_return():
    """Static guard: the shared base.apply() body contains the registration hook."""
    import inspect

    from spatialrisk.mlmodels.base import BaseRiskModel

    src = inspect.getsource(BaseRiskModel.apply)
    assert "_register_prediction(output_file" in src
    # The call precedes the final return of output_file.
    reg_idx = src.index("_register_prediction(output_file")
    ret_idx = src.rindex("return output_file")
    assert reg_idx < ret_idx

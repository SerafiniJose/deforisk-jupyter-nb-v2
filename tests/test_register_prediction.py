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

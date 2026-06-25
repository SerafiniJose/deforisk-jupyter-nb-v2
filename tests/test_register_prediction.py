from pathlib import Path

from spatialrisk import Project
from spatialrisk.mlmodels import GLMModel


class _FakeVar:
    def __init__(self, name, year=None):
        self.name = name
        self.year = year


class _FakeDataset:
    def __init__(self, name="ds_2020", year=2020):
        self.name = name
        self.year = year
        self.target = _FakeVar("forest_loss", year)
        self.features = [_FakeVar("slope")]


def test_register_prediction_builds_and_registers(monkeypatch):
    project = Project(project_name="reg_test")
    model = GLMModel(name="m1", model_type="glm", year=2020)
    project.add_model(model, auto_save=False)

    ds = _FakeDataset()
    pred = model._register_prediction(Path("/tmp/glm.tif"), dataset=ds, auto_save=False)

    assert pred is not None
    assert pred.model_key == "glm_m1"            # reverse-looked-up from project.models
    assert pred.dataset_name == "ds_2020"
    assert pred.year == 2020
    assert pred.window is None
    assert pred.model_snapshot["model_type"] == "glm"   # full model config copy
    assert pred.dataset_snapshot["feature_names"] == ["slope"]
    assert project.get_prediction("glm_m1__ds_2020_y2020") is pred


def test_register_prediction_with_window(monkeypatch):
    project = Project(project_name="reg_test_w")
    model = GLMModel(name="mw1", model_type="mw", year=2020)
    project.add_model(model, key="mw_custom", auto_save=False)

    pred = model._register_prediction(Path("/tmp/mw_5.tif"), dataset=_FakeDataset(), window=5, auto_save=False)
    assert pred.model_key == "mw_custom"   # honors custom registry key
    assert pred.window == 5
    assert project.get_prediction("mw_custom__ds_2020_y2020_w5") is pred


def test_register_prediction_noop_without_project():
    model = GLMModel(name="orphan", model_type="glm")
    result = model._register_prediction(Path("/tmp/x.tif"), dataset=_FakeDataset(), auto_save=False)
    assert result is None


def test_pending_name_overrides_prediction_key_and_label():
    """A pending prediction name keys/labels the output by the user's choice
    instead of the provenance-derived key — so distinct runs don't collide."""
    project = Project(project_name="named_pred")
    model = GLMModel(name="m1", model_type="glm", year=2020)
    project.add_model(model, auto_save=False)
    model._pending_pred_name = "run_a"

    pred = model._register_prediction(Path("/tmp/glm.tif"), dataset=_FakeDataset(), auto_save=False)

    assert pred.name == "run_a"
    assert pred.model_key == "glm_m1"          # provenance field kept for evaluation
    assert project.get_prediction("run_a") is pred
    assert project.get_prediction("glm_m1__ds_2020_y2020") is None   # NOT the old key


def test_pending_name_keeps_window_suffix_distinct():
    """Multi-output runs (MW windows) stay distinct under one pending name."""
    project = Project(project_name="named_pred_w")
    model = GLMModel(name="mw1", model_type="mw", year=2020)
    project.add_model(model, key="mw_mw1", auto_save=False)
    model._pending_pred_name = "run_b"

    p5 = model._register_prediction(Path("/tmp/mw_5.tif"), dataset=_FakeDataset(), window=5, auto_save=False)
    p11 = model._register_prediction(Path("/tmp/mw_11.tif"), dataset=_FakeDataset(), window=11, auto_save=False)

    assert project.get_prediction("run_b_w5") is p5
    assert project.get_prediction("run_b_w11") is p11
    assert p5.name == p11.name == "run_b"
    # filter by name groups both windows of the run together (drives the tile's
    # per-job map toggle).
    assert set(project.filter_predictions(name="run_b")) == {"run_b_w5", "run_b_w11"}

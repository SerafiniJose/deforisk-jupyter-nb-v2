# tests/test_project_predictions.py
from pathlib import Path

from spatialrisk.predictions.prediction import Prediction
from spatialrisk.project import Project


def _make_project():
    Project._ensure_model_schemas()
    return Project(project_name="pred_test")


def test_add_get_list_predictions():
    project = _make_project()
    pred = Prediction(path=Path("/tmp/glm_2020.tif"), model_key="glm_m", dataset_name="ds")
    project.add_prediction(pred, auto_save=False)

    assert project.list_predictions() == ["glm_m__ds"]
    assert project.get_prediction("glm_m__ds") is pred
    assert pred.project is project


def test_window_predictions_get_distinct_keys():
    project = _make_project()
    p5 = Prediction(path=Path("/tmp/w5.tif"), model_key="mw_m", dataset_name="ds", window=5)
    p10 = Prediction(path=Path("/tmp/w10.tif"), model_key="mw_m", dataset_name="ds", window=10)
    project.add_prediction(p5, auto_save=False)
    project.add_prediction(p10, auto_save=False)

    assert set(project.list_predictions()) == {"mw_m__ds_w5", "mw_m__ds_w10"}


def test_explicit_key_override():
    project = _make_project()
    pred = Prediction(path=Path("/tmp/a.tif"), model_key="glm_m", dataset_name="ds")
    project.add_prediction(pred, key="custom_key", auto_save=False)

    assert project.get_prediction("custom_key") is pred


def test_filter_predictions():
    project = _make_project()
    a = Prediction(path=Path("/tmp/a.tif"), model_key="glm_m", dataset_name="ds1", year=2020)
    b = Prediction(path=Path("/tmp/b.tif"), model_key="rf_m", dataset_name="ds1", year=2020)
    c = Prediction(path=Path("/tmp/c.tif"), model_key="glm_m", dataset_name="ds2", year=2018)
    for p in (a, b, c):
        project.add_prediction(p, auto_save=False)

    assert set(project.filter_predictions(model_key="glm_m").values()) == {a, c}
    assert set(project.filter_predictions(dataset_name="ds1").values()) == {a, b}
    assert set(project.filter_predictions(year=2020).values()) == {a, b}
    assert set(project.filter_predictions(model_key="glm_m", year=2020).values()) == {a}

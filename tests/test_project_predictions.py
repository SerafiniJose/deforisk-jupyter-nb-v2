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


def test_predictions_round_trip_save_load(tmp_path, monkeypatch):
    import spatialrisk.project as project_mod

    monkeypatch.setattr(
        project_mod.Project, "save", project_mod.Project.save, raising=True
    )
    project = _make_project()
    # Redirect the project folder to a temp dir.
    monkeypatch.setattr(
        type(project), "save",
        lambda self, filename=None: _save_to(self, tmp_path, filename),
        raising=False,
    )

    pred = Prediction(
        path=Path("/tmp/glm_2020.tif"),
        model_key="glm_m",
        dataset_name="ds",
        year=2020,
        model_snapshot={"model_type": "glm", "formula": "y ~ x"},
        dataset_snapshot={"name": "ds", "feature_names": ["slope"]},
    )
    project.add_prediction(pred, auto_save=True)

    loaded = project_mod.Project.load("pred_test", filename=str(tmp_path / "pred_test_project.json"))
    assert loaded.list_predictions() == ["glm_m__ds_y2020"]
    restored = loaded.get_prediction("glm_m__ds_y2020")
    assert restored.path == Path("/tmp/glm_2020.tif")
    assert restored.model_key == "glm_m"
    assert restored.year == 2020
    assert restored.model_snapshot == {"model_type": "glm", "formula": "y ~ x"}
    assert restored.dataset_snapshot == {"name": "ds", "feature_names": ["slope"]}
    assert restored.project is loaded


def _save_to(project, tmp_path, filename):
    """Minimal save() shim writing into tmp_path, exercising the real serializer."""
    import json

    data = {
        "project_name": project.project_name,
        "predictions": {
            key: pred.model_dump(mode="json")
            for key, pred in project.predictions.items()
        },
    }
    out = tmp_path / "pred_test_project.json"
    out.write_text(json.dumps(data, indent=4, default=str), encoding="utf-8")
    return out

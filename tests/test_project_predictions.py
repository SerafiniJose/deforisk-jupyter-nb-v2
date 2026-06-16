from pathlib import Path

from spatialrisk import Project
from spatialrisk.predictions.prediction import Prediction


def _make_project(tmp_path):
    return Project(project_name="test_predictions")


def test_add_get_list_predictions(tmp_path):
    project = _make_project(tmp_path)
    pred = Prediction(path=Path("/tmp/glm.tif"), model_key="glm_m", dataset_name="ds_2020", year=2020)
    project.add_prediction(pred, auto_save=False)

    key = "glm_m__ds_2020_y2020"
    assert project.list_predictions() == [key]
    assert project.get_prediction(key) is pred
    assert pred.project is project  # back-reference set


def test_window_predictions_get_distinct_keys(tmp_path):
    project = _make_project(tmp_path)
    for w in (5, 11):
        Prediction(
            path=Path(f"/tmp/mw_{w}.tif"),
            model_key="mw_bench",
            dataset_name="ds_2020",
            window=w,
        ).add_to_project(project, auto_save=False)
    assert project.list_predictions() == ["mw_bench__ds_2020_w5", "mw_bench__ds_2020_w11"]


def test_explicit_key_override(tmp_path):
    project = _make_project(tmp_path)
    pred = Prediction(path=Path("/tmp/x.tif"), model_key="rf_m", dataset_name="ds")
    project.add_prediction(pred, key="custom_key", auto_save=False)
    assert project.get_prediction("custom_key") is pred


def test_filter_predictions(tmp_path):
    project = _make_project(tmp_path)
    Prediction(path=Path("/tmp/a.tif"), model_key="glm_m", dataset_name="ds_2020", year=2020).add_to_project(project, auto_save=False)
    Prediction(path=Path("/tmp/b.tif"), model_key="rf_m", dataset_name="ds_2020", year=2020).add_to_project(project, auto_save=False)
    Prediction(path=Path("/tmp/c.tif"), model_key="glm_m", dataset_name="ds_2010", year=2010).add_to_project(project, auto_save=False)

    by_model = project.filter_predictions(model_key="glm_m")
    assert set(by_model.keys()) == {"glm_m__ds_2020_y2020", "glm_m__ds_2010_y2010"}

    by_dataset = project.filter_predictions(dataset_name="ds_2020")
    assert set(by_dataset.keys()) == {"glm_m__ds_2020_y2020", "rf_m__ds_2020_y2020"}

    by_attr = project.filter_predictions(year=2010)
    assert set(by_attr.keys()) == {"glm_m__ds_2010_y2010"}


def test_predictions_round_trip_save_load(tmp_path, monkeypatch):
    project = Project(project_name="rt_predictions")
    pred = Prediction(
        path=Path("/tmp/glm_2020.tif"),
        model_key="glm_m",
        dataset_name="ds_2020",
        year=2020,
        model_snapshot={"model_type": "glm", "name": "m", "deviance": 1.23},
        dataset_snapshot={"name": "ds_2020", "year": 2020, "feature_names": ["slope"]},
    )
    project.add_prediction(pred, auto_save=False)

    # Serialize predictions exactly as Project.save() does.
    dumped = {k: p.model_dump(mode="json") for k, p in project.predictions.items()}
    assert dumped["glm_m__ds_2020_y2020"]["path"] == "/tmp/glm_2020.tif"
    assert dumped["glm_m__ds_2020_y2020"]["model_snapshot"]["deviance"] == 1.23

    # Reconstruct exactly as Project.load() does.
    rebuilt = Project(project_name="rt_predictions")
    for key, pdata in dumped.items():
        if pdata.get("path"):
            pdata["path"] = Path(pdata["path"])
        restored = Prediction(**pdata)
        restored.project = rebuilt
        rebuilt.predictions[key] = restored

    got = rebuilt.get_prediction("glm_m__ds_2020_y2020")
    assert got.path == Path("/tmp/glm_2020.tif")
    assert got.model_snapshot["deviance"] == 1.23
    assert got.dataset_snapshot["feature_names"] == ["slope"]


def test_predictions_survive_real_save_load(tmp_path, monkeypatch):
    # save() writes to <downloads_folder>/<project_name>/ and load() reads from
    # the same module-level `downloads_folder`. Redirect it to tmp_path so the
    # real save()/load() round-trip is exercised without touching the user folder.
    import spatialrisk.project as project_module

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    project = Project(project_name="rt_disk_predictions")
    pred = Prediction(
        path=Path("/tmp/glm_2020.tif"),
        model_key="glm_m",
        dataset_name="ds_2020",
        year=2020,
        model_snapshot={"model_type": "glm", "name": "m", "deviance": 1.23},
        dataset_snapshot={"name": "ds_2020", "year": 2020, "feature_names": ["slope"]},
    )
    project.add_prediction(pred, auto_save=False)

    project.save()

    loaded = Project.load("rt_disk_predictions")
    got = loaded.get_prediction("glm_m__ds_2020_y2020")
    assert got is not None
    assert got.path == Path("/tmp/glm_2020.tif")
    assert got.model_key == "glm_m"
    assert got.model_snapshot["deviance"] == 1.23
    assert got.dataset_snapshot["feature_names"] == ["slope"]
    assert got.project is loaded

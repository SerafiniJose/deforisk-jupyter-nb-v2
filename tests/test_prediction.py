from pathlib import Path

from spatialrisk.predictions.prediction import Prediction, build_dataset_snapshot


def test_prediction_minimal_fields():
    pred = Prediction(
        path=Path("/tmp/glm_2020.tif"),
        model_key="glm_my_model",
        dataset_name="ds_2020",
        year=2020,
    )
    assert pred.path == Path("/tmp/glm_2020.tif")
    assert pred.model_key == "glm_my_model"
    assert pred.dataset_name == "ds_2020"
    assert pred.year == 2020
    assert pred.window is None
    assert pred.active is True
    assert pred.tags == []
    assert pred.model_snapshot == {}
    assert pred.dataset_snapshot == {}
    assert pred.metrics == {}


def test_prediction_round_trips_path_as_string():
    pred = Prediction(path=Path("/tmp/out.tif"), model_key="rf_m", dataset_name="ds")
    dumped = pred.model_dump(mode="json")
    assert dumped["path"] == "/tmp/out.tif"
    assert "project" not in dumped  # live ref excluded
    restored = Prediction(**{**dumped, "path": Path(dumped["path"])})
    assert restored.path == Path("/tmp/out.tif")
    assert restored.model_key == "rf_m"


def test_prediction_storage_key_disambiguates_year_and_window():
    base = Prediction(path=Path("/tmp/a.tif"), model_key="mw_m", dataset_name="ds")
    assert base.storage_key() == "mw_m__ds"
    y = Prediction(path=Path("/tmp/a.tif"), model_key="mw_m", dataset_name="ds", year=2020)
    assert y.storage_key() == "mw_m__ds_y2020"
    yw = Prediction(
        path=Path("/tmp/a.tif"), model_key="mw_m", dataset_name="ds", year=2020, window=5
    )
    assert yw.storage_key() == "mw_m__ds_y2020_w5"


class _FakeVar:
    def __init__(self, name, year=None):
        self.name = name
        self.year = year


class _FakeDataset:
    def __init__(self):
        self.name = "calibration_2020"
        self.year = 2020
        self.target = _FakeVar("forest_loss", 2020)
        self.features = [_FakeVar("slope"), _FakeVar("dist_road")]


def test_build_dataset_snapshot_compact_and_no_project_recursion():
    snap = build_dataset_snapshot(_FakeDataset())
    assert snap == {
        "name": "calibration_2020",
        "year": 2020,
        "target_name": "forest_loss",
        "target_year": 2020,
        "feature_names": ["slope", "dist_road"],
    }


def test_build_dataset_snapshot_handles_none():
    assert build_dataset_snapshot(None) == {}

from pathlib import Path
from types import SimpleNamespace

from spatialrisk import Project
from spatialrisk.predictions.prediction import Prediction
from spatialrisk.evaluations import EvaluationRecord
from spatialrisk.sampleset import SampleSet
from gui.scripts.summary_helpers import (
    project_overview,
    raw_variable_rows,
    processed_variable_rows,
    dataset_rows,
    sample_rows,
    model_rows,
    prediction_rows,
    evaluation_rows,
)


def _raster(name, year=None, rtype="continuous"):
    return SimpleNamespace(
        name=name, data_type="raster", raster_type=rtype, year=year, active=True
    )


def _vector(name, year=None):
    return SimpleNamespace(
        name=name, data_type="vector", raster_type=None, year=year, active=True
    )


# --- Task 1: overview + variable rows --------------------------------------

def test_project_overview_empty():
    p = Project(project_name="empty")
    ov = project_overview(p)
    assert ov["project_name"] == "empty"
    assert ov["aoi_name"] is None
    assert ov["years"] == []
    assert ov["counts"] == {
        "raw": 0, "processed": 0, "datasets": 0, "samples": 0,
        "models": 0, "predictions": 0, "evaluations": 0,
    }


def test_project_overview_counts_aoi_years():
    p = Project(project_name="demo")
    p.aoi = {"name": "San Marino"}
    p.raw_variables["altitude"] = _raster("altitude")
    p.raw_variables["forest_2015"] = _raster("forest", year=2015)
    ov = project_overview(p, last_saved="2026-06-23T10:00:00", dirty=True)
    assert ov["aoi_name"] == "San Marino"
    assert ov["counts"]["raw"] == 2
    assert ov["years"] == [2015]
    assert ov["dirty"] is True
    assert ov["last_saved"] == "2026-06-23T10:00:00"


def test_raw_variable_rows_fields_and_base_badge():
    p = Project(project_name="demo")
    alt = _raster("altitude")
    p.raw_variables["altitude"] = alt
    p.raw_variables["towns"] = _vector("towns", year=2020)
    p.base_raster = alt
    stats, rows = raw_variable_rows(p)
    assert stats == {"total": 2, "vector": 1, "raster": 1}
    by_name = {r["name"]: r for r in rows}
    assert by_name["altitude"]["is_base"] is True
    assert by_name["altitude"]["data_type"] == "raster"
    assert by_name["altitude"]["year"] == "—"
    assert by_name["towns"]["is_base"] is False
    assert by_name["towns"]["data_type"] == "vector"
    assert by_name["towns"]["year"] == 2020


def test_processed_variable_rows_source_and_empty():
    p = Project(project_name="demo")
    assert processed_variable_rows(p) == ({"total": 0, "vector": 0, "raster": 0}, [])
    p.raw_variables["altitude"] = _raster("altitude")
    p.processed_variables["altitude"] = _raster("altitude")
    stats, rows = processed_variable_rows(p)
    assert stats["total"] == 1
    assert rows[0]["source"] == "altitude"


# --- Task 2: dataset / sample / model / prediction / evaluation rows --------

def test_dataset_rows():
    p = Project(project_name="demo")
    assert dataset_rows(p) == ({"total": 0}, [])
    p.datasets["calib_2020"] = SimpleNamespace(
        name="calib_2020",
        year=2020,
        target=SimpleNamespace(name="floss"),
        features=[SimpleNamespace(name="slope"), SimpleNamespace(name="alt")],
    )
    stats, rows = dataset_rows(p)
    assert stats == {"total": 1}
    assert rows[0]["target_name"] == "floss"
    assert rows[0]["feature_count"] == 2
    assert rows[0]["year"] == 2020


def test_sample_rows():
    p = Project(project_name="demo")
    p.samples["s1"] = SampleSet(
        name="s1", dataset_name="calib_2020", strategy="random",
        n_total=100, n_event=30, n_forest=70, seed=42,
    )
    stats, rows = sample_rows(p)
    assert stats == {"total": 1, "points": 100}
    assert rows[0]["n_event"] == 30
    assert rows[0]["seed"] == 42
    assert rows[0]["dataset_name"] == "calib_2020"


def test_model_rows_trained_and_missing_attr_safe():
    p = Project(project_name="demo")
    p.models["glm_glm_v1"] = SimpleNamespace(
        name="glm_v1", model_type="glm", year=2015, trained=True,
        trained_at="2026-06-20T09:00:00", n_samples=5000, deviance=1.23456,
        parameters={"solver": "lbfgs", "max_iter": 1000},
    )
    p.models["bare"] = SimpleNamespace(model_type="rf")  # missing optional fields
    stats, rows = model_rows(p)
    assert stats == {"total": 2, "trained": 1}
    by_key = {r["key"]: r for r in rows}
    assert by_key["glm_glm_v1"]["deviance"] == 1.235
    assert "solver=lbfgs" in by_key["glm_glm_v1"]["params"]
    assert by_key["bare"]["trained"] is False
    assert by_key["bare"]["params"] == "—"
    assert by_key["bare"]["name"] == "bare"


def test_prediction_rows():
    p = Project(project_name="demo")
    Prediction(
        path=Path("/tmp/glm.tif"), model_key="glm_v1", dataset_name="ds_2020", year=2020,
    ).add_to_project(p, auto_save=False)
    stats, rows = prediction_rows(p)
    assert stats == {"total": 1, "active": 1}
    assert rows[0]["model_key"] == "glm_v1"
    assert rows[0]["year"] == 2020
    assert rows[0]["active"] is True
    assert rows[0]["window"] == "—"


def test_evaluation_rows():
    p = Project(project_name="demo")
    p.add_evaluation(
        EvaluationRecord(
            truth_tag="gfc", truth_defor="1", truth_forest="0", time_interval=5,
            prediction_keys=["a", "b"], csizes=[10, 20], metrics=["wRMSE"],
            created_at="2026-06-20T09:00:00", run_id="r1",
        ),
        auto_save=False,
    )
    stats, rows = evaluation_rows(p)
    assert stats == {"total": 1}
    assert rows[0]["name"] == "gfc"        # name falls back to truth_tag when unset
    assert rows[0]["truth_tag"] == "gfc"
    assert rows[0]["n_predictions"] == 2
    assert rows[0]["csizes"] == "10, 20"
    assert rows[0]["metrics"] == "wRMSE"

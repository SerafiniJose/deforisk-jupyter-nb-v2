"""Rebuilding the Train/Inference job lists from a loaded project.

Regression test for: loading a saved project did not show its trained models
or predictions in the GUI, because the Train/Inference tiles render from
module-level in-session job reactives that were never reconstructed on load.
"""

from pathlib import Path

from spatialrisk import Project
from spatialrisk.mlmodels import GLMModel
from spatialrisk.predictions.prediction import Prediction

from gui.scripts.job_restore import build_train_jobs, build_inference_jobs


def test_build_train_jobs_from_loaded_models():
    project = Project(project_name="jr_models")
    model = GLMModel(
        name="v1", model_type="glm", dataset_name="ds_2020",
        deviance=12.5, n_samples=1000,
    )
    project.add_model(model, auto_save=False)
    [key] = project.list_models()

    jobs = build_train_jobs(
        project, model_labels={"glm": "GLM (Logistic Regression)"}
    )

    assert len(jobs) == 1
    job = jobs[0]
    assert job["status"] == "completed"
    assert job["model_type"] == "glm"
    assert job["model_label"] == "GLM (Logistic Regression)"
    assert job["dataset_name"] == "ds_2020"
    assert job["deviance"] == 12.5
    assert job["n_samples"] == 1000
    # id + model_storage_key must equal the registry key so the list's remove
    # action deletes the right model (train_tile._do_remove uses these).
    assert job["id"] == key
    assert job["model_storage_key"] == key


def test_build_train_jobs_falls_back_to_model_type_without_labels():
    project = Project(project_name="jr_models2")
    project.add_model(GLMModel(name="v1", model_type="glm"), auto_save=False)

    [job] = build_train_jobs(project)

    assert job["model_label"] == "glm"


def test_build_train_jobs_empty_when_no_models():
    assert build_train_jobs(Project(project_name="empty")) == []
    assert build_train_jobs(None) == []


def test_build_inference_jobs_groups_predictions_by_model_and_dataset():
    project = Project(project_name="jr_preds")
    # Two windows of the same run share (model_key, dataset_name) → one job,
    # matching InferenceTile._matching_predictions' grouping.
    for w in (5, 11):
        Prediction(
            path=Path(f"/tmp/mw_{w}.tif"), model_key="mw_b",
            dataset_name="ds_2020", window=w,
        ).add_to_project(project, auto_save=False)
    # A different model → a separate job.
    Prediction(
        path=Path("/tmp/glm.tif"), model_key="glm_v1",
        dataset_name="ds_2020", year=2020,
    ).add_to_project(project, auto_save=False)

    jobs = build_inference_jobs(project)

    combos = {(j["model_key"], j["dataset_name"]) for j in jobs}
    assert combos == {("mw_b", "ds_2020"), ("glm_v1", "ds_2020")}
    assert all(j["status"] == "completed" for j in jobs)
    assert len({j["id"] for j in jobs}) == len(jobs)  # ids unique


def test_build_inference_jobs_empty_when_no_predictions():
    assert build_inference_jobs(Project(project_name="empty")) == []
    assert build_inference_jobs(None) == []

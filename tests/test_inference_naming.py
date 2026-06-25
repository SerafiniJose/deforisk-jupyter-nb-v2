# tests/test_inference_naming.py
"""Prediction-naming helpers used by the Inference tile (Step 7)."""

from pathlib import Path

from spatialrisk import Project
from spatialrisk.predictions.prediction import Prediction
from gui.tile.inference_tile import (
    _default_pred_name,
    _prediction_name_exists,
    _sanitize_pred_name,
)


def test_sanitize_pred_name_keeps_default_token_intact():
    # The default "model__dataset" token (double underscore) must survive.
    assert _sanitize_pred_name("glm_v1__calibration") == "glm_v1__calibration"
    assert _sanitize_pred_name("  my run 1 ") == "my_run_1"
    assert _sanitize_pred_name("a/b:c") == "a_b_c"
    assert _sanitize_pred_name("***") == ""


def test_default_pred_name_combines_model_and_dataset():
    assert _default_pred_name("glm_v1", "calibration") == "glm_v1__calibration"
    assert _default_pred_name("", "calibration") == ""   # incomplete selection
    assert _default_pred_name("glm_v1", "") == ""


def test_prediction_name_exists_detects_key_and_name():
    project = Project(project_name="exists_test")
    # A name-keyed prediction (the new path).
    project.add_prediction(
        Prediction(name="run_a", path=Path("/tmp/a.tif"),
                   model_key="glm_v1", dataset_name="calibration"),
        key="run_a", auto_save=False,
    )

    assert _prediction_name_exists(project, "run_a") is True   # matches key + name
    assert _prediction_name_exists(project, "run_b") is False  # absent
    assert _prediction_name_exists(project, "") is False       # empty never collides
    assert _prediction_name_exists(None, "run_a") is False     # no project

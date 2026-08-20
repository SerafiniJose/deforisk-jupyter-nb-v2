"""Unit tests for the shared artifact-naming helpers."""
from pathlib import Path

from gui.scripts.artifact_names import (
    default_pred_name,
    name_field_messages,
    prediction_name_exists,
    sanitize_key,
    suggest_name,
    suggest_version,
)
from spatialrisk import Project
from spatialrisk.predictions.prediction import Prediction


def test_suggest_name_first_free_slot():
    assert suggest_name("dataset", set()) == "dataset_1"
    assert suggest_name("dataset", {"dataset_1"}) == "dataset_2"
    assert suggest_name("dataset", {"dataset_1", "dataset_3"}) == "dataset_2"
    assert suggest_name("random", {"stratified_1"}) == "random_1"


def test_suggest_version_scopes_to_model_key():
    assert suggest_version("glm", set()) == "v1"
    assert suggest_version("glm", {"glm_v1", "glm_v2"}) == "v3"
    # another model's versions don't block
    assert suggest_version("rf", {"glm_v1"}) == "v1"


def test_sanitize_key_matches_existing_tile_behaviour():
    # Same regex as train_tile._sanitize_name / inference_tile._sanitize_pred_name.
    assert sanitize_key("glm_v1__calibration") == "glm_v1__calibration"
    assert sanitize_key("  my run 1 ") == "my_run_1"
    assert sanitize_key("a/b:c") == "a_b_c"
    assert sanitize_key("***") == ""
    assert sanitize_key(None) == ""


def test_default_pred_name():
    assert default_pred_name("glm_v1", "calibration") == "glm_v1__calibration"
    assert default_pred_name("", "calibration") == ""
    assert default_pred_name("glm_v1", "") == ""


def test_prediction_name_exists_detects_key_and_name():
    """A prediction name collides on either its registry key or its name."""
    project = Project(project_name="exists_test")
    # A name-keyed prediction (the new path).
    project.add_prediction(
        Prediction(name="run_a", path=Path("/tmp/a.tif"),
                   model_key="glm_v1", dataset_name="calibration"),
        key="run_a", auto_save=False,
    )

    assert prediction_name_exists(project, "run_a") is True   # matches key + name
    assert prediction_name_exists(project, "run_b") is False  # absent
    assert prediction_name_exists(project, "") is False       # empty never collides
    assert prediction_name_exists(None, "run_a") is False     # no project


def test_name_field_messages_states():
    # empty name: required; only an error once a submit was attempted
    assert name_field_messages("", False, False) == ("widgets.artifact_name.required", False)
    assert name_field_messages("", False, True) == ("widgets.artifact_name.required", True)
    # taken key: warning message, never a hard error
    assert name_field_messages("x", True, True) == ("widgets.artifact_name.exists_warning", False)
    # free key: saved-as preview
    assert name_field_messages("x", False, False) == ("widgets.artifact_name.saved_as", False)

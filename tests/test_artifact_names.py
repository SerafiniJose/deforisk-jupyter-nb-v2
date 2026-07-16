"""Unit tests for the shared artifact-naming helpers."""
from gui.scripts.artifact_names import (
    default_pred_name,
    name_field_messages,
    sanitize_key,
    suggest_name,
    suggest_version,
)


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


def test_name_field_messages_states():
    # empty name: required; only an error once a submit was attempted
    assert name_field_messages("", False, False) == ("widgets.artifact_name.required", False)
    assert name_field_messages("", False, True) == ("widgets.artifact_name.required", True)
    # taken key: warning message, never a hard error
    assert name_field_messages("x", True, True) == ("widgets.artifact_name.exists_warning", False)
    # free key: saved-as preview
    assert name_field_messages("x", False, False) == ("widgets.artifact_name.saved_as", False)

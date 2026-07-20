# tests/test_evaluation_record.py
from spatialrisk.evaluations import EvaluationPlotArtifact, EvaluationRecord


def _artifact(**over):
    base = dict(
        prediction_key="glm_m__ds_2020",
        model="GLM",
        period="ds_2020",
        csize_px=300,
        points_csv="/p/evaluation/tag/abcd1234/pred_obs_GLM_ds_2020_300.csv",
        png_path="/p/evaluation/tag/abcd1234/pred_obs_GLM_ds_2020_300.png",
    )
    base.update(over)
    return EvaluationPlotArtifact(**base)


def _record(**over):
    base = dict(
        truth_tag="forest_loss_2015_2020",
        truth_defor="forest_loss_2015_2020",
        truth_forest="forest_gfc",
        time_interval=5,
        prediction_keys=["glm_m__ds_2020"],
        csizes=[300],
        created_at="2026-06-22T14:05:33",
        indices=[{"model": "GLM", "MedAE": 12.3}],
        csv_path="/tmp/indices_all.csv",
        run_id="abcd1234",
    )
    base.update(over)
    return EvaluationRecord(**base)


def test_storage_key_is_truth_time_and_run():
    rec = _record()
    assert rec.storage_key() == "forest_loss_2015_2020__20260622140533_abcd1234"


def test_storage_key_unique_per_run():
    a = _record(run_id="aaaa1111", created_at="2026-06-22T14:05:33")
    b = _record(run_id="bbbb2222", created_at="2026-06-22T14:05:33")
    assert a.storage_key() != b.storage_key()


def test_model_dump_round_trip_preserves_indices():
    rec = _record()
    dumped = rec.model_dump(mode="json")
    rebuilt = EvaluationRecord(**dumped)
    assert rebuilt.indices == [{"model": "GLM", "MedAE": 12.3}]
    assert rebuilt.prediction_keys == ["glm_m__ds_2020"]


def test_metrics_defaults_empty_for_legacy_records():
    rec = _record()
    assert rec.metrics == []


def test_metrics_round_trips():
    rec = _record(metrics=["MedAE", "R2"])
    rebuilt = EvaluationRecord(**rec.model_dump(mode="json"))
    assert rebuilt.metrics == ["MedAE", "R2"]


# --- run-scoped plot artifacts (Task 4) --------------------------------------

def test_artifacts_default_empty_for_legacy_records():
    """Records saved before run-scoping carry no ``artifacts`` key at all."""
    assert _record().artifacts == []


def test_plot_artifact_carries_map_identity_and_both_paths():
    art = _artifact()
    assert art.prediction_key == "glm_m__ds_2020"
    assert art.model == "GLM"
    assert art.period == "ds_2020"
    assert art.csize_px == 300
    assert art.points_csv.endswith("pred_obs_GLM_ds_2020_300.csv")
    assert art.png_path.endswith("pred_obs_GLM_ds_2020_300.png")


def test_artifacts_round_trip_through_model_dump():
    rec = _record(artifacts=[_artifact(), _artifact(csize_px=100)])
    rebuilt = EvaluationRecord(**rec.model_dump(mode="json"))
    assert [a.csize_px for a in rebuilt.artifacts] == [300, 100]
    assert rebuilt.artifacts[0].points_csv == _artifact().points_csv
    assert isinstance(rebuilt.artifacts[0], EvaluationPlotArtifact)


def test_artifacts_accept_plain_dicts_from_a_loaded_manifest():
    rec = EvaluationRecord(**{**_record().model_dump(mode="json"),
                              "artifacts": [_artifact().model_dump(mode="json")]})
    assert rec.artifacts[0].model == "GLM"

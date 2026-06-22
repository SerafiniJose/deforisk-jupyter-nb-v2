# tests/test_evaluation_record.py
from spatialrisk.evaluations import EvaluationRecord


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

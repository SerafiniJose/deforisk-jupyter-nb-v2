import json
from datetime import datetime
from pathlib import Path

from gui.scripts.project_io import ProjectInfo, list_project_infos


def _write_project(
    data_dir: Path, name: str, raw=0, processed=0, models=0, trained=0, predictions=0
):
    folder = data_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_name": name,
        "raw_variables": {f"r{i}": {} for i in range(raw)},
        "processed_variables": {f"p{i}": {} for i in range(processed)},
        # The first ``trained`` of the ``models`` are flagged trained.
        "models": {f"m{i}": {"trained": i < trained} for i in range(models)},
        "predictions": {f"pred{i}": {} for i in range(predictions)},
    }
    (folder / f"{name}_project.json").write_text(json.dumps(payload), encoding="utf-8")
    return folder


def test_lists_projects_with_counts_and_mtime(tmp_path):
    _write_project(tmp_path, "alpha", raw=3, processed=2, models=1)
    infos = list_project_infos(tmp_path)
    assert len(infos) == 1
    info = infos[0]
    assert info.name == "alpha"
    assert (info.raw_count, info.processed_count, info.model_count) == (3, 2, 1)
    assert info.readable is True
    assert isinstance(info.modified, datetime)


def test_counts_trained_models_and_predictions(tmp_path):
    _write_project(tmp_path, "mtq", models=2, trained=1, predictions=2)
    info = list_project_infos(tmp_path)[0]
    assert info.model_count == 2
    assert info.trained_model_count == 1
    assert info.prediction_count == 2


def test_no_predictions_or_trained_models_is_zero(tmp_path):
    _write_project(tmp_path, "fresh", raw=1, models=1)  # one untrained model
    info = list_project_infos(tmp_path)[0]
    assert info.trained_model_count == 0
    assert info.prediction_count == 0


def test_sorted_by_name(tmp_path):
    _write_project(tmp_path, "beta")
    _write_project(tmp_path, "alpha")
    assert [i.name for i in list_project_infos(tmp_path)] == ["alpha", "beta"]


def test_dir_without_project_json_is_skipped(tmp_path):
    (tmp_path / "not_a_project").mkdir()
    assert list_project_infos(tmp_path) == []


def test_corrupt_json_marked_unreadable(tmp_path):
    folder = tmp_path / "broken"
    folder.mkdir()
    (folder / "broken_project.json").write_text("{ not json", encoding="utf-8")
    infos = list_project_infos(tmp_path)
    assert len(infos) == 1
    assert infos[0].name == "broken"
    assert infos[0].readable is False
    assert infos[0].error  # non-empty reason
    assert infos[0].modified is None
    assert infos[0].trained_model_count == 0
    assert infos[0].prediction_count == 0


def test_missing_data_dir_returns_empty(tmp_path):
    assert list_project_infos(tmp_path / "does_not_exist") == []

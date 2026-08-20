"""run_output_dir must treat truth_tag/run_id as identifiers, never as paths."""

import types

import pytest

from spatialrisk.evaluation import evaluate_against_truth, run_output_dir


def _project(tmp_path):
    return types.SimpleNamespace(
        folders=types.SimpleNamespace(project_folder=str(tmp_path)),
        predictions={},
    )


def test_ordinary_tags_and_run_ids_resolve_unchanged(tmp_path):
    p = _project(tmp_path)
    assert (run_output_dir(p, "loss_2010")
            == tmp_path / "evaluation" / "loss_2010")
    assert (run_output_dir(p, "loss_2010", "run0001")
            == tmp_path / "evaluation" / "loss_2010" / "run0001")


@pytest.mark.parametrize("tag", ["/etc", "..", ".", "", "a/b", "a\\b"])
def test_path_like_truth_tags_are_rejected(tmp_path, tag):
    with pytest.raises(ValueError):
        run_output_dir(_project(tmp_path), tag)


@pytest.mark.parametrize("run_id", ["/tmp/x", "..", ".", "", "a/b", "a\\b"])
def test_path_like_run_ids_are_rejected(tmp_path, run_id):
    with pytest.raises(ValueError):
        run_output_dir(_project(tmp_path), "loss_2010", run_id)


def test_no_directory_is_created_before_validation_succeeds(tmp_path):
    """evaluate_against_truth with a path-like tag must fail before ANY mkdir."""
    with pytest.raises(ValueError):
        evaluate_against_truth(
            _project(tmp_path), [], defor_file="d.tif", forest_file="f.tif",
            time_interval=5, truth_tag="../escape", auto_save=False)
    assert not (tmp_path / "evaluation").exists()
    assert not (tmp_path.parent / "escape").exists()

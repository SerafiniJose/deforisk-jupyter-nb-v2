import types
import types as _types
from pathlib import Path

import pandas as pd
import pytest

from gui.tile import evaluation_helpers as h
from gui.tile.evaluation_helpers import build_evaluation_record


def _var(name, year=None, path=None):
    return _types.SimpleNamespace(name=name, year=year,
                                  path=path or f"/data/{name}.tif")


def _project():
    pv = {
        "forest_loss_2015_2020": _var("forest_loss_2015_2020"),
        "forest_gfc": _var("forest_gfc"),
    }
    preds = {
        "glm__cal": _types.SimpleNamespace(model_key="glm_glm_v1", window=None,
                                           dataset_name="calibration"),
        "mw__val": _types.SimpleNamespace(model_key="mw_calibration_mw", window=11,
                                          dataset_name="validation"),
    }
    return _types.SimpleNamespace(processed_variables=pv, predictions=preds)


def test_variable_items_lists_instances():
    items = h.variable_items(_project())
    values = {i["value"] for i in items}
    assert values == {"forest_loss_2015_2020", "forest_gfc"}


def test_variable_items_labels_year_when_present():
    proj = _types.SimpleNamespace(
        processed_variables={"forest_loss_2015": _var("forest_loss", year=2015)},
        predictions={})
    items = h.variable_items(proj)
    assert items[0]["text"] == "forest_loss (2015)"
    assert items[0]["value"] == "forest_loss_2015"


def test_map_items_labels_model_and_period():
    items = h.map_items(_project())
    texts = {i["text"] for i in items}
    assert texts == {"GLM — calibration", "MW_w11 — validation"}


def test_default_forest_key_finds_forest_gfc():
    assert h.default_forest_key(_project()) == "forest_gfc"


def test_default_forest_key_none_when_absent():
    proj = _types.SimpleNamespace(
        processed_variables={"altitude": _var("altitude")}, predictions={})
    assert h.default_forest_key(proj) is None


def test_parse_interval_from_truth_name():
    assert h.parse_interval(_project(), "forest_loss_2015_2020") == 5


def test_parse_interval_none_for_unparseable():
    proj = _types.SimpleNamespace(
        processed_variables={"forest_gfc": _var("forest_gfc")}, predictions={})
    assert h.parse_interval(proj, "forest_gfc") is None


def test_parse_interval_none_when_project_has_no_processed_variables():
    assert h.parse_interval(_types.SimpleNamespace(), "anything") is None


def test_build_truth_spec_resolves_paths():
    spec, err = h.build_truth_spec(
        _project(), "forest_loss_2015_2020", "forest_gfc", "5")
    assert err is None
    assert spec["defor_file"] == "/data/forest_loss_2015_2020.tif"
    assert spec["forest_file"] == "/data/forest_gfc.tif"
    assert spec["time_interval"] == 5
    assert spec["truth_tag"] == "forest_loss_2015_2020"


def test_build_truth_spec_tags_year_when_present():
    proj = _types.SimpleNamespace(
        processed_variables={"floss_2015": _var("forest_loss", year=2015),
                             "forest_gfc": _var("forest_gfc")},
        predictions={})
    spec, err = h.build_truth_spec(proj, "floss_2015", "forest_gfc", "1")
    assert err is None and spec["truth_tag"] == "forest_loss_2015"


def test_build_truth_spec_requires_truth():
    spec, err = h.build_truth_spec(_project(), "", "forest_gfc", "5")
    assert spec is None and "truth" in err.lower()


def test_build_truth_spec_requires_forest():
    spec, err = h.build_truth_spec(_project(), "forest_loss_2015_2020", "", "5")
    assert spec is None and "forest" in err.lower()


def test_build_truth_spec_rejects_nonpositive_interval():
    spec, err = h.build_truth_spec(
        _project(), "forest_loss_2015_2020", "forest_gfc", "0")
    assert spec is None and "positive" in err.lower()


def test_build_truth_spec_rejects_nonnumeric_interval():
    spec, err = h.build_truth_spec(
        _project(), "forest_loss_2015_2020", "forest_gfc", "abc")
    assert spec is None and "whole number" in err.lower()


def _fake_project(tmp):
    return types.SimpleNamespace(
        folders=types.SimpleNamespace(project_folder=Path(tmp))
    )


def test_build_evaluation_record_maps_df_and_paths(tmp_path):
    df = pd.DataFrame([
        {"model": "GLM", "period": "ds_A", "MedAE": 12.3, "R2": 0.81},
        {"model": "RF", "period": "ds_A", "MedAE": 9.1, "R2": 0.88},
    ])
    spec = {
        "defor_file": "/x/forest_loss_2015_2020.tif",
        "forest_file": "/x/forest_gfc.tif",
        "time_interval": 5,
        "truth_tag": "forest_loss_2015_2020",
    }
    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec,
        resolved_keys=["glm__ds_A", "rf__ds_A"],
        run_id="abcd1234", created_at="2026-06-22T14:05:33",
    )
    assert rec.truth_tag == "forest_loss_2015_2020"
    assert rec.name == "forest_loss_2015_2020"
    assert rec.time_interval == 5
    assert rec.prediction_keys == ["glm__ds_A", "rf__ds_A"]
    assert rec.indices[0]["MedAE"] == 12.3       # JSON-native float, not str
    assert rec.indices[1]["model"] == "RF"
    # run-scoped: Path(csv_path).parent is this run's OWN figure directory
    assert rec.csv_path.endswith(
        "evaluation/forest_loss_2015_2020/abcd1234/indices_all.csv")
    assert rec.run_id == "abcd1234"
    assert rec.artifacts == []                   # df carried none


def test_build_evaluation_record_maps_artifacts_from_the_run(tmp_path):
    from spatialrisk.evaluations import EvaluationPlotArtifact

    df = pd.DataFrame([{"model": "GLM", "period": "ds_A", "MedAE": 12.3}])
    df.attrs["artifacts"] = [EvaluationPlotArtifact(
        prediction_key="glm__ds_A", model="GLM", period="ds_A", csize_px=300,
        points_csv="/p/evaluation/t/abcd1234/pred_obs_GLM_ds_A_300.csv",
        png_path="/p/evaluation/t/abcd1234/pred_obs_GLM_ds_A_300.png")]
    spec = {"defor_file": "/x/d.tif", "forest_file": "/x/f.tif",
            "time_interval": 5, "truth_tag": "t"}

    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec, resolved_keys=["glm__ds_A"],
        run_id="abcd1234", created_at="2026-06-22T14:05:33")
    assert len(rec.artifacts) == 1
    assert rec.artifacts[0].prediction_key == "glm__ds_A"
    assert rec.artifacts[0].csize_px == 300


def test_build_evaluation_record_prefers_the_dataframes_own_run_id(tmp_path):
    """csv_path and artifacts must never disagree, even given a stale run_id.

    ``evaluate_against_truth`` stamps ``df.attrs["run_id"]`` with the id it
    actually used to choose the artifact directory. If a caller passes a
    different ``run_id`` (e.g. a stale value), the record must still be built
    from the DataFrame's own id rather than silently producing a ``csv_path``
    that points at a directory the artifacts do not live in.
    """
    from spatialrisk.evaluations import EvaluationPlotArtifact

    df = pd.DataFrame([{"model": "GLM", "period": "ds_A", "MedAE": 12.3}])
    df.attrs["run_id"] = "actual_run"
    df.attrs["artifacts"] = [EvaluationPlotArtifact(
        prediction_key="glm__ds_A", model="GLM", period="ds_A", csize_px=300,
        points_csv="/p/evaluation/t/actual_run/pred_obs_GLM_ds_A_300.csv",
        png_path="/p/evaluation/t/actual_run/pred_obs_GLM_ds_A_300.png")]
    spec = {"defor_file": "/x/d.tif", "forest_file": "/x/f.tif",
            "time_interval": 5, "truth_tag": "t"}

    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec, resolved_keys=["glm__ds_A"],
        run_id="stale_run", created_at="2026-06-22T14:05:33")

    assert rec.run_id == "actual_run"
    assert rec.csv_path.endswith("evaluation/t/actual_run/indices_all.csv")
    # csv_path is scoped to the SAME run id the artifacts were written under,
    # never the stale "run_id" argument
    assert "stale_run" not in rec.csv_path
    assert "/t/actual_run/" in rec.artifacts[0].points_csv


def test_a_dataframe_from_an_unscoped_run_is_rejected_not_mis_scoped(tmp_path):
    """attrs["run_id"] = None means "shared folder", not "no information".

    ``evaluate_against_truth`` stamps the attr unconditionally, ``None``
    included, and a ``None`` run id puts every artifact straight into
    ``evaluation/<truth_tag>/``. Testing the attr with ``is None`` instead of a
    presence sentinel let the ``run_id`` argument win here and produced a
    ``csv_path`` inside a run directory that was never created — the exact
    csv_path/artifacts disagreement the function's contract rules out. The frame
    describes a layout ``EvaluationRecord`` cannot represent (``run_id`` is a
    required string), so it is refused with a message naming the mismatch rather
    than resolved by guessing.
    """
    df = pd.DataFrame([{"model": "GLM", "period": "ds_A", "MedAE": 12.3}])
    df.attrs["run_id"] = None          # unscoped run: shared truth folder
    df.attrs["artifacts"] = []
    spec = {"defor_file": "/x/d.tif", "forest_file": "/x/f.tif",
            "time_interval": 5, "truth_tag": "t"}

    with pytest.raises(ValueError, match="WITHOUT a run id"):
        build_evaluation_record(
            _fake_project(tmp_path), df, spec, resolved_keys=["glm__ds_A"],
            run_id="abcd1234", created_at="2026-06-22T14:05:33")


def test_a_dataframe_with_no_run_id_attr_falls_back_to_the_argument(tmp_path):
    """The fallback still exists for frames that never went through evaluate."""
    df = pd.DataFrame([{"model": "GLM", "period": "ds_A", "MedAE": 12.3}])
    spec = {"defor_file": "/x/d.tif", "forest_file": "/x/f.tif",
            "time_interval": 5, "truth_tag": "t"}

    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec, resolved_keys=["glm__ds_A"],
        run_id="abcd1234", created_at="2026-06-22T14:05:33")

    assert rec.run_id == "abcd1234"
    assert rec.csv_path.endswith("evaluation/t/abcd1234/indices_all.csv")


# --- run artifact directory + deletion ordering ------------------------------

def _record_for(run_id="abcd1234", truth_tag="t"):
    from spatialrisk.evaluations import EvaluationRecord

    return EvaluationRecord(
        truth_tag=truth_tag, truth_defor="d", truth_forest="f", time_interval=5,
        prediction_keys=["k"], csizes=[300], created_at="2026-06-22T14:05:33",
        indices=[], csv_path=None, run_id=run_id)


def _project_with_run_dir(tmp_path, run_id="abcd1234", truth_tag="t"):
    run_dir = tmp_path / "evaluation" / truth_tag / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "pred_obs.png").write_bytes(b"x")
    shared = tmp_path / "evaluation" / truth_tag / "pred_obs.png"
    shared.write_bytes(b"x")           # legacy dual-published copy
    return _fake_project(tmp_path), run_dir, shared


def test_run_artifact_dir_resolves_the_runs_own_folder(tmp_path):
    project, run_dir, _ = _project_with_run_dir(tmp_path)
    assert h.run_artifact_dir(project, _record_for()) == run_dir


def test_run_artifact_dir_is_none_for_legacy_records(tmp_path):
    """A pre-Task-4 record has no run directory on disk — nothing to delete."""
    project = _fake_project(tmp_path)
    (tmp_path / "evaluation" / "t").mkdir(parents=True)
    assert h.run_artifact_dir(project, _record_for()) is None


def test_delete_run_artifacts_removes_only_that_run(tmp_path):
    project, run_dir, shared = _project_with_run_dir(tmp_path)
    other = tmp_path / "evaluation" / "t" / "zzzz9999"
    other.mkdir()
    (other / "pred_obs.png").write_bytes(b"y")

    assert h.delete_run_artifacts(project, _record_for()) is True
    assert not run_dir.exists()
    assert other.exists()               # a sibling run is untouched
    assert shared.exists()              # legacy shared files stay recoverable


def test_delete_run_artifacts_refuses_outside_the_evaluation_folder(tmp_path):
    project = _fake_project(tmp_path)
    # The evaluation/ folder must exist for "evaluation/../secrets" to even
    # resolve to an existing directory (is_dir() fails on a missing "evaluation"
    # component regardless of what ".." points at) — without this, run_artifact_dir
    # returns None before the escape guard is ever reached, and the assertions
    # below would pass vacuously.
    (tmp_path / "evaluation").mkdir()
    escaped = tmp_path / "secrets"
    escaped.mkdir()
    (escaped / "keep.txt").write_text("keep")
    rec = _record_for(run_id="secrets", truth_tag="..")

    assert h.delete_run_artifacts(project, rec) is False
    assert (escaped / "keep.txt").exists()


def test_delete_evaluation_run_removes_artifacts_after_a_successful_commit(tmp_path):
    project, run_dir, _ = _project_with_run_dir(tmp_path)
    rec = _record_for()
    saved = {"n": 0, "dir_at_save": None}
    project.evaluations = {"key": rec}
    project.get_evaluation = lambda k: project.evaluations.get(k)

    def delete_evaluation(key, auto_save=False):
        return project.evaluations.pop(key, None) is not None

    def save():
        # the artifact directory MUST still exist when the manifest is committed
        saved["dir_at_save"] = run_dir.exists()
        saved["n"] += 1

    project.delete_evaluation = delete_evaluation
    project.save = save

    assert h.delete_evaluation_run(project, "key") == (True, None)
    assert saved["n"] == 1
    assert saved["dir_at_save"] is True    # ordering: commit, THEN delete
    assert not run_dir.exists()
    assert project.evaluations == {}


def test_delete_evaluation_run_keeps_artifacts_when_the_commit_fails(tmp_path):
    """A failed save must never lose data — files stay AND the registry rolls back."""
    project, run_dir, _ = _project_with_run_dir(tmp_path)
    rec = _record_for()
    project.evaluations = {"key": rec}
    project.get_evaluation = lambda k: project.evaluations.get(k)
    project.delete_evaluation = (
        lambda key, auto_save=False: project.evaluations.pop(key, None) is not None)

    def boom():
        raise OSError("disk full")

    project.save = boom

    deleted, error = h.delete_evaluation_run(project, "key")
    assert deleted is False and "disk full" in error
    assert run_dir.exists()                    # artifacts preserved
    assert project.evaluations == {"key": rec}  # registry entry restored


def test_delete_evaluation_run_restores_registry_order_and_survives_a_later_save(tmp_path):
    """The rollback restores the COMPLETE snapshot (order included), so a later
    successful save cannot silently persist the failed deletion."""
    project, run_dir, _ = _project_with_run_dir(tmp_path)
    rec = _record_for()
    other = _record_for(run_id="zzzz9999")
    project.evaluations = {"first": other, "key": rec}
    project.get_evaluation = lambda k: project.evaluations.get(k)
    project.delete_evaluation = (
        lambda key, auto_save=False: project.evaluations.pop(key, None) is not None)
    calls = {"n": 0}

    def save():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk full")

    project.save = save

    deleted, error = h.delete_evaluation_run(project, "key")
    assert deleted is False and "disk full" in error
    assert list(project.evaluations) == ["first", "key"]  # order preserved
    project.save()                              # a later save succeeds...
    assert "key" in project.evaluations         # ...and the run is still there


def test_delete_evaluation_run_unknown_key_is_a_no_op(tmp_path):
    project = _fake_project(tmp_path)
    project.get_evaluation = lambda k: None
    assert h.delete_evaluation_run(project, "missing") == (False, None)


# --- cell-size parsing -------------------------------------------------------

def test_parse_csizes_single_value():
    sizes, err = h.parse_csizes("300")
    assert err is None and sizes == [300]


def test_parse_csizes_comma_separated():
    sizes, err = h.parse_csizes("100, 300, 1000")
    assert err is None and sizes == [100, 300, 1000]


def test_parse_csizes_space_separated():
    sizes, err = h.parse_csizes("100 300")
    assert err is None and sizes == [100, 300]


def test_parse_csizes_dedupes_preserving_order():
    sizes, err = h.parse_csizes("300, 100, 300")
    assert err is None and sizes == [300, 100]


def test_parse_csizes_rejects_empty():
    sizes, err = h.parse_csizes("   ")
    assert sizes is None and "cell size" in err.lower()


def test_parse_csizes_rejects_noninteger():
    sizes, err = h.parse_csizes("100, abc")
    assert sizes is None and "whole number" in err.lower()


def test_parse_csizes_rejects_nonpositive():
    sizes, err = h.parse_csizes("100, 0")
    assert sizes is None and "positive" in err.lower()


# --- metric options + display filtering --------------------------------------

def test_metric_items_lists_four_indices():
    items = h.metric_items()
    assert [i["value"] for i in items] == ["MedAE", "R2", "RMSE", "wRMSE"]
    # display label uses the squared glyph for R2
    assert next(i["text"] for i in items if i["value"] == "R2") == "R²"


def test_displayed_indices_drops_unselected_metric_columns():
    indices = [
        {"model": "GLM", "period": "ds_A", "MedAE": 12.3, "R2": 0.81,
         "RMSE": 5.0, "wRMSE": 4.0},
    ]
    out = h.displayed_indices(indices, ["MedAE", "R2"])
    assert out == [{"model": "GLM", "period": "ds_A", "MedAE": 12.3, "R2": 0.81}]


def test_displayed_indices_empty_metrics_keeps_all_columns():
    indices = [{"model": "GLM", "MedAE": 12.3, "RMSE": 5.0}]
    assert h.displayed_indices(indices, []) == indices


def test_displayed_indices_keeps_context_columns():
    indices = [{"model": "GLM", "period": "ds_A", "ncell": 42,
                "csize_coarse_grid_ha": 81.0, "MedAE": 12.3, "RMSE": 5.0}]
    out = h.displayed_indices(indices, ["MedAE"])
    assert out == [{"model": "GLM", "period": "ds_A", "ncell": 42,
                    "csize_coarse_grid_ha": 81.0, "MedAE": 12.3}]


def test_rows_for_record_handles_record_without_metrics_attr():
    # Records created before the 'metrics' field existed lack the attribute
    # entirely (stale in-memory instance after hot-reload). Must not crash.
    legacy = _types.SimpleNamespace(
        indices=[{"model": "GLM", "MedAE": 12.3, "R2": 0.81, "RMSE": 5.0}])
    assert not hasattr(legacy, "metrics")
    assert h.rows_for_record(legacy) == legacy.indices  # all columns kept


def test_rows_for_record_applies_selected_metrics():
    rec = _types.SimpleNamespace(
        indices=[{"model": "GLM", "MedAE": 12.3, "R2": 0.81, "RMSE": 5.0}],
        metrics=["MedAE"])
    assert h.rows_for_record(rec) == [{"model": "GLM", "MedAE": 12.3}]


def test_build_evaluation_record_stores_metrics_and_csizes(tmp_path):
    df = pd.DataFrame([{"model": "GLM", "period": "ds_A", "MedAE": 12.3}])
    spec = {
        "defor_file": "/x/d.tif", "forest_file": "/x/f.tif",
        "time_interval": 5, "truth_tag": "forest_loss_2015_2020",
    }
    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec, resolved_keys=["glm__ds_A"],
        run_id="abcd1234", created_at="2026-06-22T14:05:33",
        csizes=(100, 300), metrics=["MedAE", "R2"],
    )
    assert rec.csizes == [100, 300]
    assert rec.metrics == ["MedAE", "R2"]

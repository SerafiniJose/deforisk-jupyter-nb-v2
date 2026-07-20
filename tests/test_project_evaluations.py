from spatialrisk import Project
from spatialrisk.evaluations import EvaluationRecord


def _record(run_id="abcd1234", created_at="2026-06-22T14:05:33"):
    return EvaluationRecord(
        truth_tag="forest_loss_2015_2020",
        truth_defor="forest_loss_2015_2020",
        truth_forest="forest_gfc",
        time_interval=5,
        prediction_keys=["glm_m__ds_2020"],
        csizes=[300],
        created_at=created_at,
        indices=[{"model": "GLM", "MedAE": 12.3, "R2": 0.81}],
        csv_path="/tmp/indices_all.csv",
        run_id=run_id,
    )


def test_add_get_list_evaluations():
    project = Project(project_name="ev_reg")
    rec = _record()
    project.add_evaluation(rec, auto_save=False)
    key = rec.storage_key()
    assert project.list_evaluations() == [key]
    assert project.get_evaluation(key) is rec


def test_history_keeps_multiple_runs_same_truth():
    project = Project(project_name="ev_hist")
    project.add_evaluation(_record(run_id="aaaa1111"), auto_save=False)
    project.add_evaluation(_record(run_id="bbbb2222"), auto_save=False)
    assert len(project.list_evaluations()) == 2


def test_delete_evaluation():
    project = Project(project_name="ev_del")
    rec = _record()
    project.add_evaluation(rec, auto_save=False)
    assert project.delete_evaluation(rec.storage_key()) is True
    assert project.list_evaluations() == []
    assert project.delete_evaluation("missing") is False


def test_evaluations_survive_real_save_load(tmp_path, monkeypatch):
    import spatialrisk.project as project_module
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    project = Project(project_name="ev_disk")
    rec = _record()
    project.add_evaluation(rec, auto_save=False)
    project.save()

    loaded = Project.load("ev_disk")
    got = loaded.get_evaluation(rec.storage_key())
    assert got is not None
    assert got.indices == [{"model": "GLM", "MedAE": 12.3, "R2": 0.81}]
    assert got.prediction_keys == ["glm_m__ds_2020"]


def test_load_without_evaluations_key_is_empty(tmp_path, monkeypatch):
    import spatialrisk.project as project_module
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    Project(project_name="ev_legacy").save()  # no evaluations written
    loaded = Project.load("ev_legacy")
    assert loaded.list_evaluations() == []


# --- run-scoped plot artifacts (Task 4) --------------------------------------


def test_artifacts_survive_real_save_load(tmp_path, monkeypatch):
    """``artifacts`` must round-trip through Project.save()/load()."""
    import spatialrisk.project as project_module
    from spatialrisk.evaluations import EvaluationPlotArtifact
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    rec = _record()
    rec.artifacts = [EvaluationPlotArtifact(
        prediction_key="glm_m__ds_2020", model="GLM", period="ds_2020",
        csize_px=300,
        points_csv="/p/evaluation/t/abcd1234/pred_obs_GLM_ds_2020_300.csv",
        png_path="/p/evaluation/t/abcd1234/pred_obs_GLM_ds_2020_300.png")]
    project = Project(project_name="ev_art")
    project.add_evaluation(rec, auto_save=False)
    project.save()

    got = Project.load("ev_art").get_evaluation(rec.storage_key())
    assert len(got.artifacts) == 1
    assert got.artifacts[0].csize_px == 300
    assert got.artifacts[0].png_path.endswith("pred_obs_GLM_ds_2020_300.png")
    assert got.artifacts[0].prediction_key == "glm_m__ds_2020"


def test_legacy_manifest_without_artifacts_still_loads(tmp_path, monkeypatch):
    """A manifest written before this change has no ``artifacts`` key at all."""
    import json

    import spatialrisk.project as project_module
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    rec = _record()
    project = Project(project_name="ev_pre")
    project.add_evaluation(rec, auto_save=False)
    save_path = project.save()

    data = json.loads(save_path.read_text(encoding="utf-8"))
    for ev_data in data["evaluations"].values():
        ev_data.pop("artifacts")          # simulate a pre-Task-4 manifest
    save_path.write_text(json.dumps(data, indent=4), encoding="utf-8")

    got = Project.load("ev_pre").get_evaluation(rec.storage_key())
    assert got is not None
    assert got.artifacts == []
    assert got.csv_path == "/tmp/indices_all.csv"   # legacy shared path preserved

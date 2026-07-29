"""Allocation job runner, row builder and transactional delete."""

from pathlib import Path

import pytest

from gui.scripts.allocation_runner import (
    AllocationForm,
    allocation_rows,
    delete_allocation_run,
    run_allocation,
    validate_form,
)
from spatialrisk.allocations import AllocationRun
from spatialrisk.project import Project


def _form(**kw):
    """Build an AllocationForm with a complete, runnable set of inputs."""
    base = dict(
        name="reserve_north",
        prediction_key="icar_run",
        external_riskmap=None,
        user_defrate_path=None,
        borders_file="/data/borders.gpkg",
        mask_file=None,
        defor_juris_ha=20000.0,
        years_forecast=4,
        density_map=False,
    )
    base.update(kw)
    return AllocationForm(**base)


def _project(monkeypatch, tmp_path, name):
    """Project rooted at *tmp_path* (the repo's downloads_folder convention)."""
    import spatialrisk.project as project_module

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)
    project = Project(project_name=name)
    project.initialize_folders()
    return project


def _record(run_dir, key="reserve_abc123"):
    """A saved AllocationRun pointing at *run_dir*."""
    return AllocationRun(
        name="reserve",
        run_id="abc123",
        borders_file="/b.gpkg",
        defor_juris_ha=1.0,
        years_forecast=4,
        annual_ha=2.0,
        total_ha=8.0,
        out_dir=str(run_dir),
        csv_path=str(Path(run_dir) / "defor_project.csv"),
    )


def test_validate_form_rejects_empty_name():
    """A run without a name cannot be saved or foldered."""
    assert "name" in validate_form(None, _form(name="  ")).lower()


def test_validate_form_rejects_missing_risk_map():
    """Neither a prediction nor an external file means nothing to allocate over."""
    msg = validate_form(None, _form(prediction_key=None, external_riskmap=None))
    assert "risk map" in msg.lower()


def test_validate_form_rejects_non_positive_years():
    """A forecast period of zero years would divide by zero downstream."""
    assert "year" in validate_form(None, _form(years_forecast=0)).lower()


def test_validate_form_rejects_negative_hectares():
    """Negative expected deforestation is meaningless."""
    assert "hectare" in validate_form(None, _form(defor_juris_ha=-1)).lower()


def test_validate_form_accepts_a_complete_form(tmp_path):
    """A fully specified form whose files exist passes validation."""
    borders = tmp_path / "borders.gpkg"
    borders.write_text("")
    assert validate_form(None, _form(borders_file=str(borders))) is None


def test_validate_form_rejects_a_borders_file_that_is_not_there(tmp_path):
    """Paths are checked before the run starts, not deep inside GDAL."""
    msg = validate_form(None, _form(borders_file=str(tmp_path / "gone.gpkg")))
    assert "does not exist" in msg


def test_run_allocation_registers_a_record(tmp_path, monkeypatch):
    """A successful run lands in project.allocations with its provenance."""
    import gui.scripts.allocation_runner as runner
    from spatialrisk.allocation import AllocationResult
    from spatialrisk.predictions.prediction import Prediction

    project = _project(monkeypatch, tmp_path, "alloc_run")
    # A real Prediction, not a stub: run_allocation autosaves, and save()
    # serializes every registered prediction via model_dump().
    project.predictions["icar_run"] = Prediction(
        path=tmp_path / "prob.tif",
        model_key="icar",
        dataset_name="forecast",
    )

    monkeypatch.setattr(
        runner,
        "resolve_defrate_table",
        lambda *a, **k: runner.DefrateSource(
            path=tmp_path / "r.csv", provenance="computed"
        ),
    )

    def fake_allocate(**kwargs):
        out = Path(kwargs["out_dir"])
        out.mkdir(parents=True, exist_ok=True)
        return AllocationResult(
            annual_ha=312.4,
            total_ha=1249.6,
            out_dir=out,
            csv_path=out / "defor_project.csv",
            defrate_path=out / "defrate.csv",
            cropped_riskmap_path=out / "project_riskmap.tif",
        )

    monkeypatch.setattr(runner, "_allocate", fake_allocate)

    record = run_allocation(project, _form(), job_id="job1")

    assert isinstance(record, AllocationRun)
    assert record.storage_key() in project.allocations
    assert record.annual_ha == pytest.approx(312.4)
    assert record.prediction_key == "icar_run"
    assert record.prediction_snapshot["model_key"] == "icar"
    assert Path(record.out_dir).parent.name == "allocation"


def test_run_allocation_uses_the_external_map_when_given(tmp_path, monkeypatch):
    """An external risk map bypasses the prediction registry entirely."""
    import gui.scripts.allocation_runner as runner
    from spatialrisk.allocation import AllocationResult

    project = _project(monkeypatch, tmp_path, "alloc_ext")
    seen = {}

    def fake_allocate(**kwargs):
        seen.update(kwargs)
        out = Path(kwargs["out_dir"])
        out.mkdir(parents=True, exist_ok=True)
        return AllocationResult(
            annual_ha=1.0,
            total_ha=4.0,
            out_dir=out,
            csv_path=out / "c.csv",
            defrate_path=out / "d.csv",
            cropped_riskmap_path=out / "r.tif",
        )

    monkeypatch.setattr(runner, "_allocate", fake_allocate)
    form = _form(
        prediction_key=None,
        external_riskmap=str(tmp_path / "ext.tif"),
        user_defrate_path=str(tmp_path / "ext.csv"),
    )

    record = run_allocation(project, form, job_id="job2")

    assert str(seen["riskmap_file"]) == str(tmp_path / "ext.tif")
    assert record.external_riskmap == str(tmp_path / "ext.tif")
    assert record.prediction_key is None


def test_allocation_rows_merge_records_and_jobs(tmp_path, monkeypatch):
    """In-flight jobs sort ahead of saved records in the list."""
    project = _project(monkeypatch, tmp_path, "alloc_rows")
    project.allocations["reserve_abc123"] = _record("/o")
    jobs = [{"id": "j1", "name": "pending_run", "status": "running"}]

    rows = allocation_rows(project, jobs)

    assert [r["kind"] for r in rows] == ["job", "record"]
    assert rows[0]["status"] == "running"
    assert rows[1]["annual_ha"] == pytest.approx(2.0)
    assert rows[1]["key"] == "reserve_abc123"


def test_delete_allocation_run_removes_record_and_folder(tmp_path, monkeypatch):
    """Deleting drops the registry entry and the run's output folder."""
    project = _project(monkeypatch, tmp_path, "alloc_del")
    run_dir = Path(project.folders.project_folder) / "allocation" / "reserve_abc123"
    run_dir.mkdir(parents=True)
    (run_dir / "defor_project.csv").write_text("x")
    project.allocations["reserve_abc123"] = _record(run_dir)

    assert delete_allocation_run(project, "reserve_abc123") is True
    assert "reserve_abc123" not in project.allocations
    assert not run_dir.exists()


def test_delete_allocation_run_restores_record_when_save_fails(tmp_path, monkeypatch):
    """A failed manifest save must not leave a record pointing at deleted files."""
    project = _project(monkeypatch, tmp_path, "alloc_del_fail")
    run_dir = Path(project.folders.project_folder) / "allocation" / "reserve_abc123"
    run_dir.mkdir(parents=True)
    project.allocations["reserve_abc123"] = _record(run_dir)

    def boom(self, *args, **kwargs):
        raise OSError("disk full")

    # Patch the class: pydantic models reject setting non-field attributes.
    monkeypatch.setattr(Project, "save", boom)

    with pytest.raises(OSError):
        delete_allocation_run(project, "reserve_abc123")

    assert "reserve_abc123" in project.allocations
    assert run_dir.exists()


def test_delete_allocation_run_refuses_paths_outside_the_project(tmp_path, monkeypatch):
    """A record whose out_dir escapes <project>/allocation never gets removed."""
    project = _project(monkeypatch, tmp_path, "alloc_del_evil")
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    project.allocations["evil_abc123"] = AllocationRun(
        name="evil",
        run_id="abc123",
        borders_file="/b.gpkg",
        defor_juris_ha=1.0,
        years_forecast=4,
        annual_ha=1.0,
        total_ha=1.0,
        out_dir=str(outside),
        csv_path=str(outside / "c.csv"),
    )

    delete_allocation_run(project, "evil_abc123")

    assert outside.exists()  # never removed: outside <project>/allocation

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
    """No prediction selected means nothing to allocate over.

    External risk maps are NOT an allocation-side input: they enter the
    project through the inference tab and arrive here as predictions.
    """
    msg = validate_form(None, _form(prediction_key=None))
    assert "prediction" in msg.lower()


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


def test_form_has_no_external_riskmap_channel():
    """External risk maps are imported on the inference tab, never here."""
    import dataclasses

    fields = [f.name for f in dataclasses.fields(AllocationForm)]
    assert "external_riskmap" not in fields


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


# --- form-time helpers: rate-table preview and mask choices -------------


def _preview_pred(**kw):
    from types import SimpleNamespace

    base = dict(
        path=Path("/data/p/inference/forecast/prob.tif"),
        model_key="icar",
        dataset_name="forecast",
        window=None,
        defrate_path=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _preview_project(predictions=None, processed_variables=None):
    from types import SimpleNamespace

    return SimpleNamespace(
        predictions=predictions or {},
        processed_variables=processed_variables or {},
    )


def test_preview_shows_a_persisted_table_without_computing(tmp_path):
    """A JNR/MW run's own table is previewed by name, with the JNR caveat."""
    from gui.scripts.allocation_runner import preview_defrate_source

    csv = tmp_path / "defrate_cat_bm_forecast.csv"
    csv.write_text("cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n")
    project = _preview_project(
        {"jnr_run": _preview_pred(model_key="benchmark", defrate_path=csv)}
    )

    src = preview_defrate_source(project, "jnr_run")

    assert src.path == csv
    assert src.provenance == "persisted"
    assert src.caveat is not None


def test_preview_of_a_far_prediction_names_the_future_table(tmp_path):
    """FAR families preview the to-be-computed path; nothing is computed."""
    from gui.scripts.allocation_runner import preview_defrate_source

    pred = _preview_pred(path=tmp_path / "prob.tif")
    project = _preview_project({"icar_run": pred})

    src = preview_defrate_source(project, "icar_run")

    assert src.provenance == "computed"
    assert src.path == tmp_path / "defrate_cat_icar_forecast.csv"
    assert not src.path.exists()  # preview must not compute it


def test_preview_reports_an_unresolvable_table_instead_of_raising(tmp_path):
    """An MW run with no sibling table previews as unavailable, with the reason."""
    from gui.scripts.allocation_runner import preview_defrate_source

    prob = tmp_path / "prob_mw_11_forecast.tif"
    prob.write_bytes(b"")
    project = _preview_project(
        {"mw_run": _preview_pred(model_key="mw", window=11, path=prob)}
    )

    src = preview_defrate_source(project, "mw_run")

    assert src.provenance == "unavailable"
    assert src.path is None
    assert "rate table" in (src.caveat or "")


def test_preview_honours_the_user_override(tmp_path):
    """An explicit table short-circuits the preview like it does the run."""
    from gui.scripts.allocation_runner import preview_defrate_source

    csv = tmp_path / "mine.csv"
    csv.write_text("cat\n1\n")

    src = preview_defrate_source(_preview_project(), "nope", user_path=csv)

    assert src.provenance == "user"
    assert src.path == csv


def test_resolve_returns_an_existing_computed_table_even_with_compute_off(tmp_path):
    """compute=False forbids computing, not reusing a table already on disk."""
    from gui.scripts.allocation_runner import resolve_defrate_table

    pred = _preview_pred(path=tmp_path / "prob.tif")
    existing = tmp_path / "defrate_cat_icar_forecast.csv"
    existing.write_text("cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n")
    project = _preview_project({"icar_run": pred})

    src = resolve_defrate_table(project, "icar_run", compute=False)

    assert src.path == existing
    assert src.provenance == "computed"


def test_mask_items_lists_processed_raster_variables(tmp_path):
    """The mask choices are the project's processed rasters, not free files."""
    from types import SimpleNamespace

    from gui.scripts.allocation_runner import mask_items

    project = _preview_project(
        processed_variables={
            "forest_gfc_tc30": SimpleNamespace(path=tmp_path / "forest.tif"),
            "roads": SimpleNamespace(path=tmp_path / "roads.gpkg"),  # vector: out
            "elevation": SimpleNamespace(path=tmp_path / "elev.vrt"),
        }
    )

    items = mask_items(project)

    assert [i["text"] for i in items] == ["elevation", "forest_gfc_tc30"]
    assert items[1]["value"] == str(tmp_path / "forest.tif")


def test_mask_items_of_an_empty_project_is_empty():
    """No processed variables (or no project) means no mask choices."""
    from gui.scripts.allocation_runner import mask_items

    assert mask_items(None) == []
    assert mask_items(_preview_project()) == []


def test_allocation_rows_carry_the_run_source(tmp_path, monkeypatch):
    """Record rows say what the run was computed from (model — dataset)."""
    project = _project(monkeypatch, tmp_path, "alloc_rows_src")
    record = _record(tmp_path / "run")
    record.prediction_snapshot = {
        "model_key": "icar",
        "dataset_name": "forecast",
        "window": None,
        "year": None,
        "path": "/p.tif",
    }
    project.allocations["reserve_abc123"] = record

    rows = allocation_rows(project)

    assert rows[0]["source"] == "ICAR — forecast"


def test_allocation_rows_source_is_none_for_external_maps(tmp_path, monkeypatch):
    """An external-map run has no prediction to name; the widget labels it."""
    project = _project(monkeypatch, tmp_path, "alloc_rows_ext")
    project.allocations["reserve_abc123"] = _record(tmp_path / "run")

    rows = allocation_rows(project)

    assert rows[0]["source"] is None

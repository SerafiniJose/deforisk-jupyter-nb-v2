"""AllocationRun record + Project.allocations persistence."""

from pathlib import Path

import pytest

from spatialrisk.allocations import AllocationRun
from spatialrisk.project import Project


def _run(name="reserve_north", run_id="abc12345", **kw):
    """Build an AllocationRun with sensible defaults for the tests."""
    defaults = dict(
        name=name,
        run_id=run_id,
        created_at="2026-07-29T10:00:00",
        prediction_key="icar_run1",
        prediction_snapshot={"model_key": "icar", "dataset_name": "forecast"},
        borders_file="/data/borders.gpkg",
        defor_juris_ha=20000.0,
        years_forecast=4,
        annual_ha=312.4,
        total_ha=1249.6,
        out_dir="/data/p/allocation/reserve_north_abc12345",
        csv_path="/data/p/allocation/reserve_north_abc12345/defor_project.csv",
    )
    defaults.update(kw)
    return AllocationRun(**defaults)


def test_storage_key_combines_sanitized_name_and_run_id():
    """The storage key is a path-safe name joined to the run id."""
    assert _run(name="Reserve North!").storage_key() == "Reserve_North_abc12345"


def test_same_name_twice_yields_distinct_keys():
    """Two runs sharing a name never collide: the run id disambiguates."""
    a, b = _run(run_id="aaa11111"), _run(run_id="bbb22222")
    assert a.storage_key() != b.storage_key()


def _use_tmp_downloads(monkeypatch, tmp_path):
    """Point the project module's downloads folder at *tmp_path* (repo convention)."""
    import spatialrisk.project as project_module

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)


def test_add_and_delete_allocation():
    """Adding registers under the storage key; deleting is idempotent-safe."""
    project = Project(project_name="alloc_reg")
    key = project.add_allocation(_run(), auto_save=False)
    assert key in project.allocations
    assert project.delete_allocation(key) is True
    assert key not in project.allocations
    assert project.delete_allocation(key) is False


def test_allocations_survive_save_and_load(tmp_path, monkeypatch):
    """Records round-trip through the project manifest with their provenance."""
    _use_tmp_downloads(monkeypatch, tmp_path)

    project = Project(project_name="alloc_disk")
    key = project.add_allocation(_run(), auto_save=False)
    project.save()

    reloaded = Project.load("alloc_disk")
    assert key in reloaded.allocations
    run = reloaded.allocations[key]
    assert run.annual_ha == pytest.approx(312.4)
    assert run.prediction_key == "icar_run1"
    assert run.prediction_snapshot["model_key"] == "icar"


def test_prediction_defrate_path_round_trips(tmp_path, monkeypatch):
    """Prediction.defrate_path survives save/load as a Path."""
    from spatialrisk.predictions.prediction import Prediction

    _use_tmp_downloads(monkeypatch, tmp_path)

    project = Project(project_name="alloc_pred")
    pred = Prediction(
        path=tmp_path / "prob.tif",
        model_key="mw",
        dataset_name="forecast",
        window=11,
        defrate_path=tmp_path / "defrate_cat_mw_11_forecast.csv",
    )
    pred.add_to_project(project, key="mw_run_w11", auto_save=False)
    project.save()

    reloaded = Project.load("alloc_pred")
    assert reloaded.predictions["mw_run_w11"].defrate_path == Path(
        tmp_path / "defrate_cat_mw_11_forecast.csv"
    )

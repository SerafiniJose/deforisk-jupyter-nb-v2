"""Project registry + save/load for the location-only Sample model."""
import json

import pytest

import spatialrisk.project as project_mod
from spatialrisk.project import Project
from spatialrisk.sample import Sample


def _sample(**kw):
    base = dict(name="calib", raster_var_name="target", strategy="random",
                n_samples=10, n_total=10, class_counts={"0": 6, "1": 4})
    base.update(kw)
    return Sample(**base)


def test_add_get_delete_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_samples_io")
    s = _sample()
    p.add_sample(s, auto_save=False)
    assert p.get_sample("calib") is s
    assert s.project is p           # back-ref wired
    p.delete_sample("calib", auto_save=False)
    assert p.get_sample("calib") is None


def test_save_load_round_trips_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_samples_rt")
    s = _sample(name="calib", mask_var_name="forest", strategy="stratified",
                n_samples=100, allocation="equal", n_total=100,
                class_counts={"0": 50, "1": 50},
                points_path=tmp_path / "calib.gpkg")
    p.add_sample(s, auto_save=False)
    p.save()

    loaded = Project.load("t_samples_rt")
    rs = loaded.get_sample("calib")
    assert rs is not None
    assert rs.raster_var_name == "target"
    assert rs.allocation == "equal"
    assert rs.class_counts == {"0": 50, "1": 50}
    assert rs.project is loaded     # back-ref relinked on load


def test_load_skips_old_format_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_samples_old")
    p.save()
    # Inject an old-schema samples entry (no raster_var_name).
    path = p.folders.project_folder / "t_samples_old_project.json"
    data = json.loads(path.read_text())
    data["samples"] = {"old": {"name": "old", "dataset_name": "ds",
                               "table_path": "x.csv", "strategy": "random"}}
    path.write_text(json.dumps(data))

    loaded = Project.load("t_samples_old")
    assert loaded.get_sample("old") is None    # skipped, project still loads


def test_save_load_round_trips_spacing_m(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_samples_spacing")
    s = _sample(name="grid", strategy="systematic", n_samples=None,
                spacing_m=250.0, n_total=16, class_counts={"0": 16},
                points_path=tmp_path / "grid.gpkg")
    p.add_sample(s, auto_save=False)
    p.save()

    loaded = Project.load("t_samples_spacing")
    rs = loaded.get_sample("grid")
    assert rs is not None
    assert rs.spacing_m == 250.0


def test_pmtiles_path_round_trips(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_pmtiles_rt")
    pm = tmp_path / "calib.pmtiles"; pm.write_bytes(b"PMTILES")
    s = _sample(name="calib", points_path=tmp_path / "calib.gpkg", pmtiles_path=pm)
    p.add_sample(s, auto_save=False)
    p.save()

    rs = Project.load("t_pmtiles_rt").get_sample("calib")
    assert rs is not None
    assert str(rs.pmtiles_path) == str(pm)


def test_pmtiles_path_none_round_trips(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_pmtiles_none")
    s = _sample(name="calib", points_path=tmp_path / "calib.gpkg")  # no pmtiles_path
    p.add_sample(s, auto_save=False)
    p.save()

    assert Project.load("t_pmtiles_none").get_sample("calib").pmtiles_path is None


def test_delete_sample_removes_pmtiles(tmp_path, monkeypatch):
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    p = Project(project_name="t_pmtiles_del")
    gpkg = tmp_path / "calib.gpkg"; gpkg.write_bytes(b"GPKG")
    pm = tmp_path / "calib.pmtiles"; pm.write_bytes(b"PMTILES")
    s = _sample(name="calib", points_path=gpkg, pmtiles_path=pm)
    p.add_sample(s, auto_save=False)

    p.delete_sample("calib", auto_save=False)
    assert not gpkg.exists()
    assert not pm.exists()

"""Project-level tests for the samples registry, persistence and relink."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from spatialrisk.dataset import Dataset
from spatialrisk.project import Project
from spatialrisk.sampleset import SampleSet


def _write_raster(path, array):
    transform = from_origin(0, 10, 1, 1)
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="float32", crs="EPSG:3857", transform=transform, nodata=-9999,
    ) as dst:
        dst.write(array.astype("float32"), 1)


def _project_with_sample(tmp_path, monkeypatch):
    # Point the project's downloads folder at tmp so save()/folders write there.
    import spatialrisk.project as project_mod
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path, raising=False)

    class _Var:
        def __init__(self, name, path):
            self.name, self.path, self.year = name, path, None

    p = Project(project_name="proj")
    target = np.zeros((10, 10), dtype="float32")
    target[:5, :] = 1.0
    feat = np.arange(100, dtype="float32").reshape(10, 10)
    tpath = tmp_path / "target.tif"
    fpath = tmp_path / "feat.tif"
    _write_raster(tpath, target)
    _write_raster(fpath, feat)

    ds = Dataset(project=p, name="ds")
    ds.target = _Var("forest_loss", tpath)
    ds.features = [_Var("altitude", fpath)]
    p.datasets["ds"] = ds

    ss = SampleSet(
        project=p, name="s1", dataset_name="ds", strategy="random",
        n_samples=20, seed=1,
        table_path=p.folders.samples_folder / "s1.csv",
        points_path=p.folders.samples_folder / "s1.gpkg",
    ).generate()
    return p, ss


def test_add_and_get_sample_set(tmp_path, monkeypatch):
    p, ss = _project_with_sample(tmp_path, monkeypatch)
    p.add_sample_set(ss, auto_save=False)
    assert p.get_sample_set("s1") is ss
    assert p.list_sample_sets() == ["s1"]


def test_relink_backrefs_repoints_samples_and_datasets(tmp_path, monkeypatch):
    p, ss = _project_with_sample(tmp_path, monkeypatch)
    p.add_sample_set(ss, auto_save=False)
    copied = p.model_copy()
    assert copied.samples["s1"].project is copied
    assert copied.datasets["ds"].project is copied


def test_save_load_round_trips_samples(tmp_path, monkeypatch):
    p, ss = _project_with_sample(tmp_path, monkeypatch)
    p.add_sample_set(ss, auto_save=False)
    p.save()

    loaded = Project.load("proj")
    assert "s1" in loaded.samples
    restored = loaded.samples["s1"]
    assert restored.dataset_name == "ds"
    assert restored.n_total == ss.n_total
    assert restored.n_event == ss.n_event
    assert restored.project is loaded
    # Materialized table still loads (no regeneration).
    assert len(restored.load_table()) == ss.n_total


def test_delete_sample_set_removes_entry_and_files(tmp_path, monkeypatch):
    p, ss = _project_with_sample(tmp_path, monkeypatch)
    p.add_sample_set(ss, auto_save=False)
    table, points = ss.table_path, ss.points_path
    p.delete_sample_set("s1", auto_save=False)
    assert "s1" not in p.samples
    assert not table.exists()
    assert not points.exists()

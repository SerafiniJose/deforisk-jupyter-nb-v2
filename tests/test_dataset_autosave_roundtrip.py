"""Regression: the dataset tile must persist datasets to disk immediately.

The GUI dataset tile used to mutate ``project.datasets`` and only call
``project.set(...)`` (marking the project dirty) without writing the manifest.
A dataset therefore vanished on reload unless the user manually hit Save —
unlike every other workflow artifact (samples/models/predictions all
``auto_save=True``). These tests lock in that a registered dataset survives a
real ``save()``/``load()`` round-trip and that a removal is persisted too, which
is exactly the behaviour the tile now relies on (``add_dataset(auto_save=True)``
on register, ``project.save()`` on remove).
"""

from pathlib import Path

import spatialrisk.project as project_module
from spatialrisk import Project
from spatialrisk.variables import LocalRasterVar
from spatialrisk.variables.models import RasterType


def _project_with_processed_vars(name: str) -> Project:
    """A project with two static processed raster variables (target + feature)."""
    Project._ensure_model_schemas()
    project = Project(project_name=name)
    for var_name in ("forest_loss", "slope"):
        var = LocalRasterVar(
            name=var_name,
            path=Path(f"/tmp/{var_name}.tif"),
            raster_type=RasterType.continuous,
        )
        var.project = project
        project.processed_variables[var_name] = var
    return project


def test_registered_dataset_survives_save_load(tmp_path, monkeypatch):
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    from spatialrisk.dataset import Dataset

    project = _project_with_processed_vars("ds_autosave")
    ds = Dataset(project=project, name="calib")
    ds.set_target("forest_loss")
    ds.set_features(["slope"])

    # This is the tile's register path: auto_save must write the manifest now.
    project.add_dataset(ds, key="calib", auto_save=True)

    manifest = tmp_path / "ds_autosave" / "ds_autosave_project.json"
    assert manifest.exists(), "add_dataset(auto_save=True) did not write the manifest"

    loaded = Project.load("ds_autosave")
    assert loaded.list_datasets() == ["calib"]
    restored = loaded.get_dataset("calib")
    assert restored.target.name == "forest_loss"
    assert [f.name for f in restored.features] == ["slope"]


def test_dataset_removal_is_persisted(tmp_path, monkeypatch):
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    from spatialrisk.dataset import Dataset

    project = _project_with_processed_vars("ds_remove")
    ds = Dataset(project=project, name="calib")
    ds.set_target("forest_loss")
    ds.set_features(["slope"])
    project.add_dataset(ds, key="calib", auto_save=True)

    # The tile's remove path: delete the key then save().
    del project.datasets["calib"]
    project.save()

    loaded = Project.load("ds_remove")
    assert loaded.list_datasets() == [], "removal was not persisted to disk"

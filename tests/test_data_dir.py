"""Tests for the canonical data-directory resolution."""

from pathlib import Path

import spatialrisk.project as pm


def test_resolve_data_dir_default(monkeypatch):
    """Default to the SEPAL-wide ~/module_results/<module> location."""
    monkeypatch.delenv("SPATIAL_RISK_DATA_DIR", raising=False)
    # SEPAL convention: all module outputs live under ~/module_results/<module>
    expected = (Path.home() / "module_results" / "spatial_risk_module").resolve()
    assert pm._resolve_data_dir() == expected


def test_resolve_data_dir_env_override(monkeypatch, tmp_path):
    """SPATIAL_RISK_DATA_DIR overrides the default location."""
    monkeypatch.setenv("SPATIAL_RISK_DATA_DIR", str(tmp_path))
    assert pm._resolve_data_dir() == tmp_path.resolve()


def test_data_dir_alias_is_downloads_folder():
    """The legacy downloads_folder alias tracks DATA_DIR."""
    assert pm.DATA_DIR == pm.downloads_folder


def test_save_list_load_share_one_dir(monkeypatch, tmp_path):
    """save(), list_project_infos(), and load() must agree on one directory."""
    import spatialrisk.project as project_module
    from gui.scripts.project_io import list_project_infos
    from spatialrisk import Project

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    Project(project_name="roundtrip").save()

    infos = list_project_infos(tmp_path)
    assert [i.name for i in infos] == ["roundtrip"]

    loaded = Project.load("roundtrip")
    assert loaded.project_name == "roundtrip"


def test_root_folder_is_not_cwd_relative(monkeypatch, tmp_path):
    """``folders["root_folder"]`` must not be derived from the process CWD.

    It used to be ``Path.cwd().parent``, which on SEPAL resolves inside the
    read-only shared module mount -- so anything that read this key as a place
    to write would land there. It now tracks the module's real output root.
    """
    import spatialrisk.project as project_module
    from spatialrisk import Project

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)
    monkeypatch.chdir(tmp_path)

    folders = Project(project_name="cwd_check").folders

    # The key stays published: it is reachable as project.folders.root_folder.
    assert "root_folder" in folders
    assert folders["root_folder"] == project_module.DATA_DIR
    assert folders["root_folder"].is_absolute()
    assert folders["root_folder"] != Path.cwd().parent


def test_saved_manifest_never_persists_folder_paths(monkeypatch, tmp_path):
    """No machine-local folder path reaches the on-disk project JSON.

    ``folders`` is a property, not a model field, and ``save()`` serialises an
    explicit whitelist -- so retargeting ``root_folder`` cannot change the shape
    of a saved manifest, and ``load()`` has nothing to read back. This pins that
    invariant, which is what makes the value safe to change.
    """
    import json

    import spatialrisk.project as project_module
    from spatialrisk import Project

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    save_path = Project(project_name="no_folders").save()
    data = json.loads(save_path.read_text(encoding="utf-8"))

    assert "folders" not in data
    assert "root_folder" not in save_path.read_text(encoding="utf-8")
    assert "folders" not in Project.model_fields

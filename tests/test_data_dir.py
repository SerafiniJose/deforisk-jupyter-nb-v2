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

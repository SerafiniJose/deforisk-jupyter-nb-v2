from pathlib import Path

import spatialrisk.project as pm


def test_resolve_data_dir_default(monkeypatch):
    monkeypatch.delenv("SPATIAL_RISK_DATA_DIR", raising=False)
    expected = (Path(pm.__file__).resolve().parents[1] / "data").resolve()
    assert pm._resolve_data_dir() == expected
    # parents[1] of spatialrisk/project.py is the module root
    assert expected.name == "data"
    assert expected.parent.name == "spatial-risk-module"


def test_resolve_data_dir_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("SPATIAL_RISK_DATA_DIR", str(tmp_path))
    assert pm._resolve_data_dir() == tmp_path.resolve()


def test_data_dir_alias_is_downloads_folder():
    assert pm.DATA_DIR == pm.downloads_folder


def test_save_list_load_share_one_dir(monkeypatch, tmp_path):
    """save(), list_project_infos(), and load() must agree on one directory."""
    import spatialrisk.project as project_module
    from spatialrisk import Project
    from gui.scripts.project_io import list_project_infos

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    Project(project_name="roundtrip").save()

    infos = list_project_infos(tmp_path)
    assert [i.name for i in infos] == ["roundtrip"]

    loaded = Project.load("roundtrip")
    assert loaded.project_name == "roundtrip"

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

import sys
from pathlib import Path

import pytest


def test_gee_var_vector_download_uses_helper_not_geemap(tmp_path, monkeypatch):
    """The vector download branch calls download_ee_vector, never importing geemap."""
    # Any attempt to import geemap must fail loudly.
    monkeypatch.setitem(sys.modules, "geemap", None)

    import spatialrisk.variables.gee_var as gee_var_mod
    from spatialrisk.variables.gee_var import GEEVar
    from spatialrisk.variables.models import DataType

    captured = {}

    def _fake_download_ee_vector(fc, filename, selectors=None):
        captured["fc"] = fc
        captured["filename"] = Path(filename)
        captured["selectors"] = selectors
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text("stub")  # make the existence check pass
        return Path(filename)

    monkeypatch.setattr(gee_var_mod, "download_ee_vector", _fake_download_ee_vector)

    # Fake project with a folders.data_raw_folder.
    class _Folders:
        data_raw_folder = tmp_path / "raw"

    class _Project:
        folders = _Folders()

    var = GEEVar.model_construct(
        name="aoi",
        year=None,
        gee_images=["FAKE_FC"],
        data_type=DataType.vector,
        project=_Project(),
        default_crs="EPSG:4326",
        default_scale=None,
    )

    paths = var._download(overwrite=True)

    assert captured["fc"] == "FAKE_FC"
    assert captured["selectors"] == ["gaul0_name", "iso3_code"]
    assert captured["filename"].suffix == ".shp"
    assert paths[0].exists()


def test_gee_var_module_has_no_geemap_reference():
    """Static guard: gee_var.py no longer references geemap."""
    src = Path("spatialrisk/variables/gee_var.py").read_text()
    assert "geemap" not in src
    assert "download_ee_vector" in src

import subprocess
import pytest

gpd = pytest.importorskip("geopandas")
from shapely.geometry import Point


def _tiny_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {"strata": [1, 0, 1]},
        geometry=[Point(i / 10, i / 10) for i in range(3)], crs="EPSG:4326")
    gdf.to_file(path, driver="GPKG")
    return path


def test_available_false_raises(tmp_path, monkeypatch):
    from spatialrisk import pmtiles_convert
    src = _tiny_gpkg(tmp_path / "a.gpkg")
    monkeypatch.setattr(pmtiles_convert.shutil, "which", lambda _: None)
    with pytest.raises(RuntimeError):
        pmtiles_convert.gpkg_to_pmtiles(src, tmp_path / "a.pmtiles")


def test_builds_expected_command(tmp_path, monkeypatch):
    from spatialrisk import pmtiles_convert
    src = _tiny_gpkg(tmp_path / "s.gpkg")
    out = tmp_path / "s.pmtiles"
    captured = {}

    def fake_run(cmd, **kw):
        captured["cmd"] = cmd
        out.write_bytes(b"PMTILES")          # pretend tippecanoe wrote it
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(pmtiles_convert.shutil, "which", lambda _: "/usr/bin/tippecanoe")
    monkeypatch.setattr(pmtiles_convert.subprocess, "run", fake_run)
    result = pmtiles_convert.gpkg_to_pmtiles(src, out, layer="points", max_zoom=12)

    assert result == out
    cmd = captured["cmd"]
    assert cmd[0] == "tippecanoe"
    assert "-o" in cmd and str(out) in cmd
    assert "-l" in cmd and "points" in cmd
    assert "-z" in cmd and "12" in cmd
    assert "-Z" in cmd and "0" in cmd
    assert "--drop-densest-as-needed" in cmd


@pytest.mark.skipif(
    __import__("shutil").which("tippecanoe") is None, reason="tippecanoe not installed")
def test_real_conversion_writes_pmtiles(tmp_path):
    from spatialrisk.pmtiles_convert import gpkg_to_pmtiles
    src = _tiny_gpkg(tmp_path / "s.gpkg")
    out = gpkg_to_pmtiles(src, tmp_path / "s.pmtiles")
    assert out.exists() and out.stat().st_size > 0

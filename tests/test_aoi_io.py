from types import SimpleNamespace

import geopandas as gpd
import pytest
from shapely.geometry import box

import spatialrisk.project as proj
from gui.scripts.aoi_io import AOI_GEOMETRY_FILENAME, load_aoi, write_aoi


def _gdf(bounds=(12.40, 43.89, 12.52, 43.99)):
    return gpd.GeoDataFrame(
        {"name": ["aoi"]}, geometry=[box(*bounds)], crs="EPSG:4326"
    )


def _aoi(method="DRAW", name="san_marino", gee=True, admin=None, gdf=None):
    return SimpleNamespace(method=method, name=name, gee=gee, admin=admin, gdf=gdf)


# --- write_aoi --------------------------------------------------------------

def test_vector_aoi_writes_sidecar_and_metadata(tmp_path):
    meta = write_aoi(tmp_path, _aoi(gdf=_gdf()))

    assert meta == {
        "method": "DRAW",
        "name": "san_marino",
        "gee": True,
        "admin": None,
        "geometry_file": AOI_GEOMETRY_FILENAME,
    }
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_geometryless_aoi_writes_metadata_only(tmp_path):
    meta = write_aoi(tmp_path, _aoi(method="ADMIN1", name="COL_x", admin="21758", gdf=None))

    assert meta == {"method": "ADMIN1", "name": "COL_x", "gee": True, "admin": "21758"}
    assert "geometry_file" not in meta
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_none_aoi_returns_none_and_removes_stale_sidecar(tmp_path):
    write_aoi(tmp_path, _aoi(gdf=_gdf()))
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists()

    assert write_aoi(tmp_path, None) is None
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_geometryless_resave_removes_stale_sidecar(tmp_path):
    write_aoi(tmp_path, _aoi(gdf=_gdf()))
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists()

    # Re-saving with a geometry-less AOI must not leave a dangling pointer.
    write_aoi(tmp_path, _aoi(method="ASSET", gdf=None))
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_non_wgs84_geometry_is_reprojected(tmp_path):
    # A box in Web Mercator metres near San Marino.
    gdf = gpd.GeoDataFrame(
        {"name": ["aoi"]},
        geometry=[box(1_380_000, 5_440_000, 1_395_000, 5_460_000)],
        crs="EPSG:3857",
    )
    write_aoi(tmp_path, _aoi(gdf=gdf))

    written = gpd.read_file(tmp_path / AOI_GEOMETRY_FILENAME)
    assert written.crs.to_epsg() == 4326
    minx, miny, maxx, maxy = written.total_bounds
    assert 10 < minx < 14 and 43 < miny < 45  # plausible lon/lat, not metres


# --- load_aoi ---------------------------------------------------------------

def test_load_roundtrips_geometry_and_metadata(tmp_path):
    meta = write_aoi(tmp_path, _aoi(gdf=_gdf()))
    restored = load_aoi(tmp_path, meta)

    assert restored.method == "DRAW"
    assert restored.name == "san_marino"
    assert restored.gee is True
    assert restored.gdf is not None
    assert restored.gdf.total_bounds == pytest.approx([12.40, 43.89, 12.52, 43.99], abs=1e-6)


def test_load_metadata_only_aoi_has_no_geometry(tmp_path):
    meta = write_aoi(tmp_path, _aoi(method="ADMIN1", admin="21758", gdf=None))
    restored = load_aoi(tmp_path, meta)

    assert restored is not None
    assert restored.method == "ADMIN1"
    assert restored.admin == "21758"
    assert restored.gdf is None


def test_load_gee_admin_rebuilds_feature_collection(tmp_path, monkeypatch):
    """A GEE admin AOI persists only its GAUL ``admin`` code (no sidecar).

    Loading must rebuild the lazy EE FeatureCollection that selection produced
    (``pygaul.Items(admin=...)``) so the restored AOI is usable downstream
    (``resolve_aoi_ee``) without the user re-selecting the area.
    """
    import pygaul

    sentinel = object()
    captured = {}

    def fake_items(admin):
        captured["admin"] = admin
        return sentinel

    monkeypatch.setattr(pygaul, "Items", fake_items)

    meta = write_aoi(tmp_path, _aoi(method="ADMIN0", name="GUY", admin="197", gdf=None))
    restored = load_aoi(tmp_path, meta)

    assert restored.gdf is None
    assert restored.feature_collection is sentinel
    assert captured["admin"] == "197"


def test_load_gee_admin_degrades_when_rebuild_fails(tmp_path, monkeypatch):
    """If the FeatureCollection rebuild fails (EE not ready, offline), loading
    degrades to a metadata-only AOI instead of raising."""
    import pygaul

    def boom(admin):
        raise RuntimeError("Earth Engine not initialized")

    monkeypatch.setattr(pygaul, "Items", boom)

    meta = write_aoi(tmp_path, _aoi(method="ADMIN0", name="GUY", admin="197", gdf=None))
    restored = load_aoi(tmp_path, meta)

    assert restored is not None
    assert restored.admin == "197"
    assert restored.feature_collection is None


def test_load_none_metadata_returns_none(tmp_path):
    assert load_aoi(tmp_path, None) is None


def test_load_tolerates_missing_sidecar(tmp_path):
    # Manifest references a sidecar that was deleted/moved — degrade, don't crash.
    meta = {"method": "DRAW", "name": "x", "gee": False, "geometry_file": AOI_GEOMETRY_FILENAME}
    restored = load_aoi(tmp_path, meta)

    assert restored is not None
    assert restored.gdf is None


# --- Project.aoi field round-trip ------------------------------------------

def test_project_persists_aoi_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)

    p = proj.Project(project_name="proj_with_aoi")
    p.aoi = {"method": "DRAW", "name": "x", "gee": True, "admin": None,
             "geometry_file": AOI_GEOMETRY_FILENAME}
    p.save()

    loaded = proj.Project.load("proj_with_aoi")
    assert loaded.aoi == p.aoi


def test_project_persists_asset_aoi_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)

    p = proj.Project(project_name="proj_asset_aoi")
    p.aoi = {"method": "ASSET", "name": "aoi", "gee": True, "admin": None,
             "asset": {"asset_id": "users/me/aoi", "type": "TABLE",
                       "column": "ALL", "value": None}}
    p.save()

    loaded = proj.Project.load("proj_asset_aoi")
    assert loaded.aoi == p.aoi


def test_project_without_aoi_loads_none(tmp_path, monkeypatch):
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)

    proj.Project(project_name="proj_no_aoi").save()

    loaded = proj.Project.load("proj_no_aoi")
    assert loaded.aoi is None


# --- ASSET AOI persist + rebuild --------------------------------------------

def test_write_includes_asset_for_asset_method(tmp_path):
    asset = {"asset_id": "users/me/aoi", "type": "TABLE", "column": "ALL", "value": None}
    meta = write_aoi(tmp_path, _aoi(method="ASSET", name="aoi", gdf=None), asset=asset)
    assert meta["asset"] == asset


def test_write_omits_asset_for_non_asset_method(tmp_path):
    asset = {"asset_id": "users/me/aoi", "type": "TABLE", "column": "ALL", "value": None}
    meta = write_aoi(tmp_path, _aoi(method="ADMIN0", name="GUY", admin="197", gdf=None), asset=asset)
    assert "asset" not in meta


def test_load_asset_rebuilds_feature_collection(tmp_path, monkeypatch):
    import gui.scripts.aoi_io as aoi_io
    import ee

    sentinel = object()
    captured = {}
    monkeypatch.setattr(ee, "FeatureCollection", lambda aid: captured.update({"aid": aid}) or sentinel)

    meta = {"method": "ASSET", "name": "aoi", "gee": True,
            "asset": {"asset_id": "users/me/aoi", "type": "TABLE", "column": "ALL", "value": None}}
    restored = aoi_io.load_aoi(tmp_path, meta)

    assert restored.gdf is None
    assert restored.feature_collection is sentinel
    assert captured["aid"] == "users/me/aoi"


def test_load_asset_degrades_when_rebuild_fails(tmp_path, monkeypatch):
    import gui.scripts.aoi_io as aoi_io
    import ee
    monkeypatch.setattr(ee, "FeatureCollection", lambda aid: (_ for _ in ()).throw(RuntimeError("no ee")))
    meta = {"method": "ASSET", "name": "aoi", "gee": True,
            "asset": {"asset_id": "users/me/aoi", "type": "TABLE", "column": "ALL", "value": None}}
    restored = aoi_io.load_aoi(tmp_path, meta)
    assert restored is not None
    assert restored.feature_collection is None

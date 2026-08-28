"""AOI persistence helpers."""

from types import SimpleNamespace

import geopandas as gpd
import pytest
from shapely.geometry import box

import spatialrisk.project as proj
from gui.scripts.aoi_io import (
    AOI_GEOMETRY_FILENAME,
    attach_aoi,
    load_aoi,
    persist_aoi,
    write_aoi,
)


def _gdf(bounds=(12.40, 43.89, 12.52, 43.99)):
    return gpd.GeoDataFrame({"name": ["aoi"]}, geometry=[box(*bounds)], crs="EPSG:4326")


def _aoi(method="DRAW", name="san_marino", gee=True, admin=None, gdf=None, asset=None):
    return SimpleNamespace(
        method=method, name=name, gee=gee, admin=admin, gdf=gdf, asset=asset
    )


# --- write_aoi --------------------------------------------------------------


def test_vector_aoi_writes_sidecar_and_metadata(tmp_path):
    """Vector AOI with geometry writes both sidecar and metadata."""
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
    """Geometry-less AOI writes metadata only, no sidecar."""
    meta = write_aoi(
        tmp_path, _aoi(method="ADMIN1", name="COL_x", admin="21758", gdf=None)
    )

    assert meta == {"method": "ADMIN1", "name": "COL_x", "gee": True, "admin": "21758"}
    assert "geometry_file" not in meta
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_none_aoi_returns_none_and_removes_stale_sidecar(tmp_path):
    """None AOI returns None and removes any stale sidecar."""
    write_aoi(tmp_path, _aoi(gdf=_gdf()))
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists()

    assert write_aoi(tmp_path, None) is None
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_geometryless_resave_removes_stale_sidecar(tmp_path):
    """Re-saving a geometry-less AOI removes any stale sidecar from before."""
    write_aoi(tmp_path, _aoi(gdf=_gdf()))
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists()

    # Re-saving with a geometry-less AOI must not leave a dangling pointer.
    write_aoi(tmp_path, _aoi(method="ASSET", gdf=None))
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


def test_non_wgs84_geometry_is_reprojected(tmp_path):
    """Non-WGS84 geometry is reprojected to EPSG:4326 for the sidecar."""
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
    """Writing and loading an AOI preserves geometry and metadata."""
    meta = write_aoi(tmp_path, _aoi(gdf=_gdf()))
    restored = load_aoi(tmp_path, meta)

    assert restored.method == "DRAW"
    assert restored.name == "san_marino"
    assert restored.gee is True
    assert restored.gdf is not None
    assert restored.gdf.total_bounds == pytest.approx(
        [12.40, 43.89, 12.52, 43.99], abs=1e-6
    )


def test_load_metadata_only_aoi_has_no_geometry(tmp_path):
    """Metadata-only AOI loads without a geometry GeoDataFrame."""
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
    """If the FeatureCollection rebuild fails, loading degrades gracefully.

    When the rebuild fails (EE not ready, offline), loading degrades to a
    metadata-only AOI instead of raising.
    """
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
    """Loading with None metadata returns None."""
    assert load_aoi(tmp_path, None) is None


def test_load_tolerates_missing_sidecar(tmp_path):
    """Loading tolerates a missing geometry sidecar referenced in metadata."""
    # Manifest references a sidecar that was deleted/moved — degrade, don't crash.
    meta = {
        "method": "DRAW",
        "name": "x",
        "gee": False,
        "geometry_file": AOI_GEOMETRY_FILENAME,
    }
    restored = load_aoi(tmp_path, meta)

    assert restored is not None
    assert restored.gdf is None


# --- Project.aoi field round-trip ------------------------------------------


def test_project_persists_aoi_metadata(tmp_path, monkeypatch):
    """Project.aoi metadata round-trips through save/load."""
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)

    p = proj.Project(project_name="proj_with_aoi")
    p.aoi = {
        "method": "DRAW",
        "name": "x",
        "gee": True,
        "admin": None,
        "geometry_file": AOI_GEOMETRY_FILENAME,
    }
    p.save()

    loaded = proj.Project.load("proj_with_aoi")
    assert loaded.aoi == p.aoi


def test_project_persists_asset_aoi_metadata(tmp_path, monkeypatch):
    """Project persists and restores ASSET AOI metadata."""
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)

    p = proj.Project(project_name="proj_asset_aoi")
    p.aoi = {
        "method": "ASSET",
        "name": "aoi",
        "gee": True,
        "admin": None,
        "asset": {
            "asset_id": "users/me/aoi",
            "type": "TABLE",
            "column": "ALL",
            "value": None,
        },
    }
    p.save()

    loaded = proj.Project.load("proj_asset_aoi")
    assert loaded.aoi == p.aoi


def test_project_without_aoi_loads_none(tmp_path, monkeypatch):
    """Project without saved AOI loads None."""
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)

    proj.Project(project_name="proj_no_aoi").save()

    loaded = proj.Project.load("proj_no_aoi")
    assert loaded.aoi is None


# --- ASSET AOI persist + rebuild --------------------------------------------


def test_write_includes_asset_for_asset_method(tmp_path):
    """Write includes asset dict for ASSET method AOIs."""
    # The picker inputs ride on AoiResult.asset (pysepal restore support).
    asset = {
        "asset_id": "users/me/aoi",
        "type": "TABLE",
        "column": "ALL",
        "value": None,
    }
    meta = write_aoi(tmp_path, _aoi(method="ASSET", name="aoi", gdf=None, asset=asset))
    assert meta["asset"] == asset


def test_write_omits_asset_for_non_asset_method(tmp_path):
    """Write omits asset dict for non-ASSET method AOIs."""
    asset = {
        "asset_id": "users/me/aoi",
        "type": "TABLE",
        "column": "ALL",
        "value": None,
    }
    meta = write_aoi(
        tmp_path, _aoi(method="ADMIN0", name="GUY", admin="197", gdf=None, asset=asset)
    )
    assert "asset" not in meta


def test_load_attaches_asset_to_result(tmp_path):
    """Load attaches asset dict to the restored AOI result."""
    asset = {
        "asset_id": "users/me/aoi",
        "type": "TABLE",
        "column": "ALL",
        "value": None,
    }
    meta = write_aoi(tmp_path, _aoi(method="ASSET", name="aoi", gdf=None, asset=asset))
    restored = load_aoi(tmp_path, meta)
    assert restored.asset == asset


def test_load_asset_rebuilds_feature_collection(tmp_path, monkeypatch):
    """Load rebuilds the feature collection from asset metadata."""
    import ee

    import gui.scripts.aoi_io as aoi_io

    sentinel = object()
    captured = {}
    monkeypatch.setattr(
        ee, "FeatureCollection", lambda aid: captured.update({"aid": aid}) or sentinel
    )

    meta = {
        "method": "ASSET",
        "name": "aoi",
        "gee": True,
        "asset": {
            "asset_id": "users/me/aoi",
            "type": "TABLE",
            "column": "ALL",
            "value": None,
        },
    }
    restored = aoi_io.load_aoi(tmp_path, meta)

    assert restored.gdf is None
    assert restored.feature_collection is sentinel
    assert captured["aid"] == "users/me/aoi"


def test_load_asset_degrades_when_rebuild_fails(tmp_path, monkeypatch):
    """Load degrades gracefully when asset feature collection rebuild fails."""
    import ee

    import gui.scripts.aoi_io as aoi_io

    monkeypatch.setattr(
        ee,
        "FeatureCollection",
        lambda aid: (_ for _ in ()).throw(RuntimeError("no ee")),
    )
    meta = {
        "method": "ASSET",
        "name": "aoi",
        "gee": True,
        "asset": {
            "asset_id": "users/me/aoi",
            "type": "TABLE",
            "column": "ALL",
            "value": None,
        },
    }
    restored = aoi_io.load_aoi(tmp_path, meta)
    assert restored is not None
    assert restored.feature_collection is None


# --- persist_aoi ------------------------------------------------------------


def test_persist_keeps_stored_aoi_when_session_state_is_empty(tmp_path):
    """Persist keeps stored AOI when session state is empty.

    A save must never turn a persisted AOI into nothing.

    Session state going empty while a project holds a saved AOI means something
    dropped it (a widget teardown, a failed restore) — not that the user asked
    for the AOI to be removed. Overwriting is silent, unrecoverable data loss,
    so the stored metadata and its sidecar are kept.
    """
    stored = write_aoi(tmp_path, _aoi(gdf=_gdf()))
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists()

    kept = persist_aoi(tmp_path, None, stored)

    assert kept == stored
    assert (tmp_path / AOI_GEOMETRY_FILENAME).exists(), "sidecar geometry was deleted"


def test_persist_writes_a_new_aoi_over_the_stored_one(tmp_path):
    """Persist allows writing a new AOI over the stored one.

    Guarding an empty AOI must not block a real change of area.
    """
    stored = write_aoi(tmp_path, _aoi(method="ADMIN0", name="GUY", admin="197"))

    meta = persist_aoi(tmp_path, _aoi(method="ADMIN0", name="BOL", admin="178"), stored)

    assert meta["name"] == "BOL"
    assert meta["admin"] == "178"


def test_persist_stores_nothing_when_there_is_nothing_stored(tmp_path):
    """No AOI and none saved is an ordinary empty project."""
    assert persist_aoi(tmp_path, None, None) is None


def test_persist_of_empty_state_leaves_no_stale_sidecar(tmp_path):
    """Persist guarding metadata doesn't resurrect geometry for metadata-only AOI."""
    stored = write_aoi(tmp_path, _aoi(method="ADMIN0", name="GUY", admin="197"))

    kept = persist_aoi(tmp_path, None, stored)

    assert kept == stored
    assert not (tmp_path / AOI_GEOMETRY_FILENAME).exists()


# --- build_asset_feature_collection (strict builder) --------------------------


def test_strict_builder_rejects_a_filter_with_no_value():
    """AssetSelectComponent publishes {column: X, value: None} mid-filter.

    Accepting it would silently allocate over the whole unfiltered collection.
    """
    from gui.scripts.aoi_io import build_asset_feature_collection

    asset = {
        "asset_id": "users/me/table",
        "type": "TABLE",
        "column": "adm1",
        "value": None,
    }

    with pytest.raises(ValueError, match="adm1"):
        build_asset_feature_collection(asset)


def test_strict_builder_rejects_an_unknown_asset_type():
    """An unrecognised type must not fall through to None."""
    from gui.scripts.aoi_io import build_asset_feature_collection

    with pytest.raises(ValueError, match="FOLDER"):
        build_asset_feature_collection({"asset_id": "users/me/x", "type": "FOLDER"})


def test_forgiving_builder_still_degrades_to_none():
    """load_aoi's contract is unchanged: a bad asset loads as metadata-only."""
    from gui.scripts.aoi_io import _rebuild_asset_feature_collection

    asset = {
        "asset_id": "users/me/table",
        "type": "TABLE",
        "column": "adm1",
        "value": None,
    }

    assert _rebuild_asset_feature_collection(asset) is None


# --- attach_aoi: selection-time persistence ---------------------------------
#
# Job-completion saves call ``project.save()`` directly, which serializes
# whatever ``project.aoi`` holds at that moment. Before attach_aoi existed the
# AOI was only attached inside the manual Save flow, so a project driven
# through the workflow but never manually saved reloaded with every artifact
# EXCEPT its AOI (silently: the manifest simply lacked the key).


def test_attach_writes_sidecar_and_sets_project_aoi(tmp_path, monkeypatch):
    """Selecting an AOI immediately persists geometry + attaches metadata."""
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)
    p = proj.Project(project_name="attach_fresh")

    assert attach_aoi(p, _aoi(gdf=_gdf()), data_dir=tmp_path) is True

    assert p.aoi["method"] == "DRAW"
    assert (tmp_path / "attach_fresh" / AOI_GEOMETRY_FILENAME).exists()


def test_attach_does_not_materialize_unsaved_project(tmp_path, monkeypatch):
    """A never-saved project must not gain a manifest from AOI selection."""
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)
    p = proj.Project(project_name="attach_unsaved")

    attach_aoi(p, _aoi(gdf=_gdf()), data_dir=tmp_path)

    assert not (tmp_path / "attach_unsaved" / "attach_unsaved_project.json").exists()


def test_attach_updates_existing_manifest_in_place(tmp_path, monkeypatch):
    """A previously saved project's manifest gains the AOI on selection."""
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)
    p = proj.Project(project_name="attach_saved")
    p.save()

    attach_aoi(p, _aoi(gdf=_gdf()), data_dir=tmp_path)

    loaded = proj.Project.load("attach_saved")
    assert loaded.aoi["method"] == "DRAW"
    assert load_aoi(tmp_path / "attach_saved", loaded.aoi).gdf is not None


def test_attach_survives_job_style_bare_save(tmp_path, monkeypatch):
    """A background job's bare save carries the AOI (the testag regression).

    Once an AOI is selected, project.save() must serialize it with no manual
    Save in between.
    """
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)
    p = proj.Project(project_name="attach_job")

    attach_aoi(p, _aoi(gdf=_gdf()), data_dir=tmp_path)
    p.save()  # what every job-completion site does

    loaded = proj.Project.load("attach_job")
    restored = load_aoi(tmp_path / "attach_job", loaded.aoi)
    assert restored is not None and restored.method == "DRAW"
    assert restored.gdf is not None


def test_attach_skips_rewrite_when_metadata_unchanged(tmp_path, monkeypatch):
    """Re-running with the same AOI (e.g. right after a load) writes nothing.

    Otherwise every project load would bump the manifest mtime.
    """
    monkeypatch.setattr(proj, "downloads_folder", tmp_path)
    p = proj.Project(project_name="attach_idem")
    p.save()
    attach_aoi(p, _aoi(gdf=_gdf()), data_dir=tmp_path)

    manifest = tmp_path / "attach_idem" / "attach_idem_project.json"
    sidecar = tmp_path / "attach_idem" / AOI_GEOMETRY_FILENAME
    before = (manifest.stat().st_mtime_ns, sidecar.stat().st_mtime_ns)

    assert attach_aoi(p, _aoi(gdf=_gdf()), data_dir=tmp_path) is False

    assert (manifest.stat().st_mtime_ns, sidecar.stat().st_mtime_ns) == before


def test_attach_noop_without_project_or_aoi(tmp_path):
    """No project or no AOI: nothing to attach, nothing written."""
    assert attach_aoi(None, _aoi(gdf=_gdf()), data_dir=tmp_path) is False
    assert (
        attach_aoi(proj.Project(project_name="attach_none"), None, data_dir=tmp_path)
        is False
    )
    assert not (tmp_path / "attach_none").exists()

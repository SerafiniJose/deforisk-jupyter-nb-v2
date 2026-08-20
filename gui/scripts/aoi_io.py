"""AOI persistence for the Spatial Risk GUI.

The AOI is session state (``app_state.aoi_result``), not part of the library
``Project`` model. We persist it as:

* a light, library-agnostic metadata dict stored on ``project.aoi`` (and thus
  inside ``<project>_project.json``); plus
* the geometry itself in a sidecar ``aoi.geojson`` (WGS84) in the project
  folder, so the manifest stays small and the geometry is QGIS-openable.

Only AOIs that already carry a resolved ``gdf`` (DRAW / SHAPE / POINTS, and
admin selections whose geometry has been fetched) get a geometry sidecar.
GEE-lazy AOIs (admin / asset under GEE expose ``gdf=None``) are persisted as
metadata only. For admin AOIs that metadata (the GAUL ``admin`` code) is enough
to rebuild the lazy EE ``feature_collection`` on load (see ``load_aoi``), so the
AOI stays usable downstream without re-selection; asset AOIs remain
geometry-less on load. See ``write_aoi`` for the boundary.

Kept free of Solara/ipyvuetify so it can be unit-tested without a render
harness; pysepal/geopandas are imported lazily inside the functions.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("spatial_risk")

AOI_GEOMETRY_FILENAME = "aoi.geojson"
_ADMIN_METHODS = ("ADMIN0", "ADMIN1", "ADMIN2")


def _rebuild_admin_feature_collection(admin_code: str) -> Optional[Any]:
    """Rebuild the lazy GEE FeatureCollection for a persisted admin AOI.

    Admin selections persist only their GAUL ``admin`` code, not geometry.
    This mirrors what pysepal's ``process_admin`` does on selection — the
    FeatureCollection is ``pygaul.Items(admin=<code>)`` — so a loaded admin AOI
    is usable downstream (``resolve_aoi_ee``) and can frame the map without the
    user re-selecting it.

    Earth Engine must already be initialized (the GUI does this when the AOI
    panel mounts). Returns None if the rebuild fails (EE not ready, offline,
    bad code) so loading degrades to a metadata-only AOI instead of erroring.
    """
    try:
        import pygaul

        return pygaul.Items(admin=admin_code)
    except Exception:  # pragma: no cover - exercised via degrade test (patched)
        logger.debug(
            "Could not rebuild AOI FeatureCollection for admin=%s; "
            "loading as metadata-only.",
            admin_code,
            exc_info=True,
        )
        return None


def build_asset_feature_collection(asset: Dict[str, Any]) -> Any:
    """Rebuild the EE object for an ASSET selection, raising on any problem.

    Mirrors pysepal's ``process_asset`` validation. The strict counterpart of
    :func:`_rebuild_asset_feature_collection`: an allocation run that cannot
    build its borders must fail loudly, where loading persisted AOI metadata
    degrades to None.

    Raises:
        ValueError: no asset id, a column filter with no value, or an asset
            type the AOI workflow does not support.
    """
    import ee

    aid = (asset or {}).get("asset_id")
    if not aid:
        raise ValueError("No Earth Engine asset selected.")

    atype = asset.get("type", "TABLE")
    column, value = asset.get("column", "ALL"), asset.get("value")
    if column not in (None, "ALL") and value is None:
        raise ValueError(f"The filter on column '{column}' has no value selected.")

    if atype == "TABLE":
        obj = ee.FeatureCollection(aid)
        if column not in (None, "ALL"):
            obj = obj.filter(ee.Filter.eq(column, value))
        return obj
    if atype == "IMAGE":
        return ee.Image(aid)
    if atype == "IMAGE_COLLECTION":
        return ee.ImageCollection(aid)
    raise ValueError(f"Unsupported asset type {atype!r}.")


def _rebuild_asset_feature_collection(asset: Dict[str, Any]) -> Optional[Any]:
    """Best-effort rebuild for ``load_aoi``: None instead of an exception.

    Returns None (metadata-only fallback) if EE isn't ready or the asset is
    unavailable, so loading a project never crashes.
    """
    try:
        return build_asset_feature_collection(asset)
    except Exception:  # pragma: no cover - exercised via degrade test (patched)
        logger.debug(
            "Could not rebuild ASSET FeatureCollection; metadata-only.", exc_info=True
        )
    return None


def _aoi_metadata(aoi: Any, geometry_file: Optional[str]) -> Dict[str, Any]:
    """Build the serializable metadata dict for an AoiResult-like object."""
    meta: Dict[str, Any] = {
        "method": getattr(aoi, "method", None),
        "name": getattr(aoi, "name", None),
        "gee": bool(getattr(aoi, "gee", False)),
        "admin": getattr(aoi, "admin", None),
    }
    if geometry_file is not None:
        meta["geometry_file"] = geometry_file
    return meta


def write_aoi(project_dir: Path, aoi: Any) -> Optional[Dict[str, Any]]:
    """Persist ``aoi`` for a project and return the metadata dict to store.

    Writes the geometry to ``<project_dir>/aoi.geojson`` (WGS84) when the AOI
    carries a ``gdf``; otherwise persists metadata only. Any stale sidecar from
    a previous save is removed when the current AOI has no geometry (or is None),
    so the manifest's ``geometry_file`` pointer never dangles.

    Args:
        project_dir: The project folder (created if missing).
        aoi: A pysepal ``AoiResult`` (or compatible) or None.

    Returns:
        The metadata dict to assign to ``project.aoi``, or None when there is no
        AOI to persist.
    """
    project_dir = Path(project_dir)
    sidecar = project_dir / AOI_GEOMETRY_FILENAME

    if aoi is None:
        sidecar.unlink(missing_ok=True)
        return None

    project_dir.mkdir(parents=True, exist_ok=True)

    gdf = getattr(aoi, "gdf", None)
    if gdf is None:
        # Metadata-only AOI (e.g. GEE admin/asset): drop any stale geometry.
        sidecar.unlink(missing_ok=True)
        meta = _aoi_metadata(aoi, geometry_file=None)
        asset = getattr(aoi, "asset", None)
        if asset and getattr(aoi, "method", None) == "ASSET":
            meta["asset"] = asset
        return meta

    # Normalize to WGS84 so the sidecar matches what zoom_bounds expects.
    if getattr(gdf, "crs", None) is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    gdf.to_file(sidecar, driver="GeoJSON")
    return _aoi_metadata(aoi, geometry_file=AOI_GEOMETRY_FILENAME)


def persist_aoi(
    project_dir: Path, aoi: Any, stored: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Decide what a save should store on ``project.aoi``.

    ``write_aoi`` faithfully persists whatever session state holds, including
    "nothing" — which is right when the project never had an AOI, and wrong
    when it had one a moment ago. An AOI vanishing from session state means
    something dropped it, not that the user asked for it to be removed: the
    picker is step 1 and gates the whole workflow, so there is no flow in which
    saving is how you clear it. Overwriting the stored metadata (and unlinking
    its sidecar) in that case is unrecoverable — the geometry only lives here.

    So an empty AOI over a stored one keeps the stored one, loudly. This is
    defence-in-depth: the wipe that motivated it (pysepal's AoiView nulling the
    caller's reactive when the widget unmounted) is fixed upstream, but the
    failure mode was silent and cost real data.

    Args:
        project_dir: The project folder.
        aoi: The AOI in session state (an ``AoiResult`` or None).
        stored: The metadata already on the project (``project.aoi``), if any.

    Returns:
        The metadata dict to assign to ``project.aoi``, or None.
    """
    if aoi is None and stored:
        logger.warning(
            "Refusing to overwrite the saved AOI (%s) with an empty one: the "
            "project has a stored AOI but session state is empty. Keeping the "
            "saved AOI; re-select the area if you meant to change it.",
            stored.get("name") or stored.get("method"),
        )
        return stored

    return write_aoi(project_dir, aoi)


def load_aoi(project_dir: Path, metadata: Optional[Dict[str, Any]]) -> Optional[Any]:
    """Reconstruct an ``AoiResult`` from persisted metadata + sidecar geometry.

    Args:
        project_dir: The project folder containing ``aoi.geojson``.
        metadata: The ``project.aoi`` dict (or None).

    Returns:
        A reconstructed ``AoiResult``, or None when there is nothing to restore.
        Vector AOIs carry their ``gdf`` (from the sidecar). GEE admin AOIs carry
        no sidecar — their lazy EE ``feature_collection`` is rebuilt from the
        persisted ``admin`` code (``gdf`` stays None). ``feature_collection`` is
        None only when neither applies or the rebuild fails.
    """
    if not metadata:
        return None

    import geopandas as gpd
    from pysepal.solara.components.aoi import AoiResult

    method = metadata.get("method")
    admin = metadata.get("admin")
    gee = bool(metadata.get("gee", False))

    gdf = None
    geometry_file = metadata.get("geometry_file")
    if geometry_file:
        path = Path(project_dir) / geometry_file
        if path.exists():
            gdf = gpd.read_file(path)

    # GEE admin selections (ADMIN0/1/2) carry no geometry sidecar — only the
    # GAUL ``admin`` code is persisted. Rebuild the lazy EE FeatureCollection so
    # the restored AOI has usable geometry; otherwise it loads "present" but
    # empty and the Variables step fails with "no usable geometry — re-select
    # the area", forcing the user to reselect just to continue.
    feature_collection = None
    if gdf is None and gee and admin and method in _ADMIN_METHODS:
        feature_collection = _rebuild_admin_feature_collection(admin)
    elif gee and method == "ASSET" and metadata.get("asset"):
        feature_collection = _rebuild_asset_feature_collection(metadata["asset"])

    return AoiResult(
        method=method,
        name=metadata.get("name"),
        gdf=gdf,
        feature_collection=feature_collection,
        admin=admin,
        gee=gee,
        # ASSET picker inputs round-trip on the result so AoiView can restore
        # the asset field on load.
        asset=metadata.get("asset"),
    )

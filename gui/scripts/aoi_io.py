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
metadata only: enough to mark the AOI present, but without geometry to redraw
or zoom to. See ``write_aoi`` for the boundary.

Kept free of Solara/ipyvuetify so it can be unit-tested without a render
harness; pysepal/geopandas are imported lazily inside the functions.
"""

from pathlib import Path
from typing import Any, Dict, Optional

AOI_GEOMETRY_FILENAME = "aoi.geojson"


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
        return _aoi_metadata(aoi, geometry_file=None)

    # Normalize to WGS84 so the sidecar matches what zoom_bounds expects.
    if getattr(gdf, "crs", None) is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    gdf.to_file(sidecar, driver="GeoJSON")
    return _aoi_metadata(aoi, geometry_file=AOI_GEOMETRY_FILENAME)


def load_aoi(project_dir: Path, metadata: Optional[Dict[str, Any]]) -> Optional[Any]:
    """Reconstruct an ``AoiResult`` from persisted metadata + sidecar geometry.

    Args:
        project_dir: The project folder containing ``aoi.geojson``.
        metadata: The ``project.aoi`` dict (or None).

    Returns:
        A reconstructed ``AoiResult`` (``feature_collection`` left None — the app
        derives EE features from the gdf), or None when there is nothing to
        restore. ``gdf`` is None for metadata-only AOIs.
    """
    if not metadata:
        return None

    import geopandas as gpd
    from pysepal.solara.components.aoi import AoiResult

    gdf = None
    geometry_file = metadata.get("geometry_file")
    if geometry_file:
        path = Path(project_dir) / geometry_file
        if path.exists():
            gdf = gpd.read_file(path)

    return AoiResult(
        method=metadata.get("method"),
        name=metadata.get("name"),
        gdf=gdf,
        feature_collection=None,
        admin=metadata.get("admin"),
        gee=bool(metadata.get("gee", False)),
    )

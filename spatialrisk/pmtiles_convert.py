"""Convert a points GPKG to PMTiles via tippecanoe (Solara-free domain helper).

tippecanoe reads GeoJSON, not GPKG, so we reproject to WGS84 and export a
temporary GeoJSON first. Kept thin and mockable so the eager-conversion step in
``Sample.generate`` can be tested without the binary.
"""
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from spatialrisk.gdal_env import scratch_dir

logger = logging.getLogger("spatial_risk")


def tippecanoe_available() -> bool:
    """True if the ``tippecanoe`` binary is on PATH."""
    return shutil.which("tippecanoe") is not None


def gpkg_to_pmtiles(
    gpkg_path, out_path, *, layer="points", min_zoom=0, max_zoom=14
) -> Path:
    """Convert ``gpkg_path`` to a ``.pmtiles`` archive at ``out_path``.

    ``layer`` is the tippecanoe layer name (the MapLibre ``source-layer``).
    Raises ``RuntimeError`` if tippecanoe is missing, ``CalledProcessError`` if
    it fails. ``-r1`` disables tippecanoe's default rate-based point dropping
    (2.5x per zoom below maxzoom) so every point renders at every zoom;
    ``--drop-densest-as-needed`` stays as a guard that only thins tiles
    exceeding the 500KB limit, so million-point samples still tile.
    """
    import geopandas as gpd

    if not tippecanoe_available():
        raise RuntimeError("tippecanoe not found on PATH")

    gpkg_path, out_path = Path(gpkg_path), Path(out_path)
    gdf = gpd.read_file(gpkg_path)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Pinned: tempfile's candidate list ends with the CWD, which on SEPAL is the
    # read-only module mount, so an unavailable system temp dir would fail here.
    with tempfile.TemporaryDirectory(dir=scratch_dir()) as td:
        geojson = Path(td) / "points.geojson"
        gdf.to_file(geojson, driver="GeoJSON")
        cmd = [
            "tippecanoe",
            "-o",
            str(out_path),
            "-l",
            layer,
            "-Z",
            str(min_zoom),
            "-z",
            str(max_zoom),
            "-r1",
            "--drop-densest-as-needed",
            "--force",
            str(geojson),
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    logger.info("PMTiles written: %s", out_path)
    return out_path

"""Build external raster overviews (pyramids) for fast low-zoom tiling.

Prediction rasters are written un-tiled with no overviews, so localtileserver
reads them at full resolution for every tile. ``ensure_overviews`` builds
overviews once, as **external** ``.ovr`` sidecars: opening the dataset read-only
makes GDAL write ``<path>.ovr`` and leaves the source GeoTIFF byte-identical
(predictions are registered project artifacts and must not be mutated).
rio-tiler / localtileserver read ``.ovr`` sidecars automatically.
"""

from osgeo import gdal


def ensure_overviews(path, resampling="average", levels=(2, 4, 8, 16, 32)) -> bool:
    """Build external overviews for ``path`` if it has none. Idempotent.

    Parameters
    ----------
    path : str | Path
        GeoTIFF to add overviews to.
    resampling : str
        GDAL resampling algorithm. ``average`` is nodata-aware because prediction
        rasters carry ``nodata=0``.
    levels : tuple[int, ...]
        Decimation factors for the overview pyramid.

    Returns
    -------
    bool
        ``True`` if overviews were built, ``False`` if they already existed.
    """
    path = str(path)
    ds = gdal.Open(path, gdal.GA_ReadOnly)
    if ds is None:
        raise FileNotFoundError(f"Cannot open raster for overviews: {path}")
    try:
        if ds.GetRasterBand(1).GetOverviewCount() > 0:
            return False
        # COMPRESS_OVERVIEW keeps the .ovr sidecar small; read-only open => external.
        gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")
        ds.BuildOverviews(resampling, list(levels))
        return True
    finally:
        if ds is not None:
            ds.FlushCache()
            ds = None  # explicit flush then close

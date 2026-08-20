"""Compute geometric distance-bin edges from a distance raster and threshold."""

from pathlib import Path
from typing import List, Union


def compute_dist_bins(
    forest_edge_file: Union[str, Path],
    dist_thresh: float,
) -> List[float]:
    """Compute geometric distance-bin edges for vulnerability stratification.

    Thin wrapper around ``riskmapjnr.benchmark.compute_dist_bins``.  Creates
    29 vulnerability classes using a geometric progression between the pixel
    resolution and ``dist_thresh``.

    Parameters
    ----------
    forest_edge_file : str or Path
        Distance-to-forest-edge raster.  Only the pixel resolution is read;
        raster values are not accessed.
    dist_thresh : float
        Distance threshold in metres (from ``dist_edge_threshold``).

    Returns
    -------
    list of float
        30 bin edges (29 classes + boundaries), increasing from
        ``pixel_resolution`` to ``dist_thresh``.

    Notes:
    -----
    Relative paths are resolved against the current working directory before
    being forwarded (see the comment at the call site).
    """
    import riskmapjnr as rmj

    # Absolutise the path before it crosses the riskmapjnr boundary, for the
    # same reason as in ``vulnerability_map``: riskmapjnr resolves relative
    # paths against the *process* CWD, which on SEPAL is the read-only shared
    # module mount. This particular call only reads (``gdal.Open`` for the
    # geotransform, no output file of its own), so the failure mode here is a
    # path silently pointing somewhere other than the caller meant rather than
    # a stray write -- but the boundary rule is worth holding uniformly.
    return rmj.benchmark.compute_dist_bins(
        str(Path(forest_edge_file).resolve()), dist_thresh
    )

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
    """
    import riskmapjnr as rmj

    return rmj.benchmark.compute_dist_bins(str(forest_edge_file), dist_thresh)

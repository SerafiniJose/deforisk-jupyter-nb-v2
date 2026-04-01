"""Compute the forest-edge distance threshold from a binary deforestation raster."""

from pathlib import Path
from typing import Optional, Union


def dist_edge_threshold(
    deforestation_file: Union[str, Path],
    forest_edge_file: Union[str, Path],
    defor_values=1,
    defor_threshold: float = 99.5,
    max_dist: int = 5000,
    blk_rows: int = 128,
    tab_file: Optional[Union[str, Path]] = None,
    fig_file: Optional[Union[str, Path]] = None,
    verbose: bool = False,
) -> dict:
    """Compute the distance-to-forest-edge threshold from a binary raster.

    Thin wrapper around ``riskmapjnr.dist_edge_threshold`` that:

    * Passes ``check_fcc=False`` so that a generic binary (0/1) deforestation
      raster is accepted instead of the library's three-date FCC stack.
    * Provides sensible defaults and a cleaner parameter surface.

    Parameters
    ----------
    deforestation_file : str or Path
        Binary deforestation raster.  Pixels equal to ``defor_values`` are
        treated as deforested.  NoData must be 0.  Raster must be projected.
    forest_edge_file : str or Path
        Distance-to-forest-edge raster (metres) for the initial year of the
        period.  Must be already computed (``dist_file_available=True``).
    defor_values : int or list of int
        Pixel value(s) in ``deforestation_file`` that represent deforestation
        (default: ``1``).
    defor_threshold : float
        Percentile of the deforested-pixel distance distribution used as the
        distance threshold (default: 99.5).
    max_dist : int
        Upper bound (m) of the distance bins arange (default: 5000).
    blk_rows : int
        Number of rows per processing block (default: 128).
    tab_file : str or Path, optional
        Output CSV with the distance distribution table.  Defaults to
        ``None`` (no file written).
    fig_file : str or Path, optional
        Output PNG with the cumulative-deforestation vs. distance plot.
        Defaults to ``None`` (no file written).
    verbose : bool
        Print progress messages (default: False).

    Returns
    -------
    dict
        ``{"dist_thresh": float, "perc_thresh": float, "tot_def": float}``
    """
    import numpy as np
    import riskmapjnr as rmj

    # Provide harmless default paths if the caller doesn't want output files
    _tab = str(tab_file) if tab_file is not None else "/dev/null"
    _fig = str(fig_file) if fig_file is not None else "/dev/null"

    return rmj.dist_edge_threshold(
        fcc_file=str(deforestation_file),
        defor_values=defor_values,
        defor_threshold=defor_threshold,
        dist_file=str(forest_edge_file),
        dist_bins=np.arange(0, max_dist, step=30),
        tab_file_dist=_tab,
        fig_file_dist=_fig,
        blk_rows=blk_rows,
        dist_file_available=True,
        check_fcc=False,    # accept binary raster — bypass FCC validation
        verbose=verbose,
    )

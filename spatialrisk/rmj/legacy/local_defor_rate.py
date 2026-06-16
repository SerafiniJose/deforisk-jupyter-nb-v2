"""LEGACY — superseded by ``spatialrisk.rmj.deforrate.local_defor_rate``.

Thin wrapper that feeds a binary raster to ``riskmapjnr`` as an ``fcc_file``;
the forest denominator is derived as ``(in_data > 0)``. Kept for reference only;
not used by any model. See the ``rmj.legacy`` docstring.

Compute local deforestation rates from a binary deforestation raster.
"""

from pathlib import Path
from typing import Union


def local_defor_rate(
    deforestation_file: Union[str, Path],
    ldefrate_file: Union[str, Path],
    win_size: int,
    time_interval: int,
    defor_value: int = 1,
    rescale_min_val: int = 2,
    rescale_max_val: int = 65535,
    blk_rows: int = 256,
    verbose: bool = False,
) -> None:
    """Compute the local deforestation rate within a moving window.

    Thin wrapper around ``riskmapjnr.local_defor_rate`` that accepts a
    binary (0/1) deforestation raster directly and provides sensible
    defaults and a cleaner parameter surface.

    Parameters
    ----------
    deforestation_file : str or Path
        Binary deforestation raster.  Pixels equal to ``defor_value`` are
        treated as deforested.  NoData must be 0.  Raster must be projected.
    ldefrate_file : str or Path
        Output GeoTIFF path for the local deforestation rate raster (uint16).
    win_size : int
        Moving window size in pixels (odd number, e.g. 5, 11, 21).
    time_interval : int
        Number of years covered by the period.
    defor_value : int
        Pixel value in ``deforestation_file`` that represents deforestation
        (default: ``1``).
    rescale_min_val : int
        Minimum value for rescaling the output raster (default: ``2``).
        Value ``1`` is reserved for no-deforestation-observed pixels.
    rescale_max_val : int
        Maximum value for rescaling the output raster (default: ``65535``
        to match the uint16 scale used by GLM/RF/iCAR models).
    blk_rows : int
        Number of rows per processing block (default: ``256``).
    verbose : bool
        Print progress messages (default: ``False``).
    """
    import riskmapjnr as rmj

    rmj.local_defor_rate(
        fcc_file=str(deforestation_file),
        defor_values=defor_value,
        ldefrate_file=str(ldefrate_file),
        win_size=win_size,
        time_interval=time_interval,
        rescale_min_val=rescale_min_val,
        rescale_max_val=rescale_max_val,
        blk_rows=blk_rows,
        verbose=verbose,
    )

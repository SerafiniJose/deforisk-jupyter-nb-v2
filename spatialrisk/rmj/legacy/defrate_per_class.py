"""LEGACY — superseded by ``spatialrisk.rmj.deforrate.defrate_per_class``.

Kept for reference only; not used by any model. The current version uses the
period-robust ``(forest == 1) | (defor == 1)`` denominator. See the
``rmj.legacy`` docstring.

Compute deforestation rates per vulnerability class from binary rasters.

Replaces ``riskmapjnr.benchmark.defrate_per_class`` with a generic
implementation that accepts separate binary forest and deforestation rasters
instead of a multi-period FCC stack.  The period-name logic and hard-coded
FCC pixel-value filters are removed entirely.
"""

from pathlib import Path
from typing import Optional, Union


def defrate_per_class(
    forest_file: Union[str, Path],
    deforestation_file: Union[str, Path],
    vulnerability_file: Union[str, Path],
    time_interval: int,
    tab_file_defrate: Union[str, Path],
    deforate_model: Optional[Union[str, Path]] = None,
    forest_value: int = 1,
    defor_value: int = 1,
    blk_rows: int = 128,
) -> None:
    """Compute deforestation rates per vulnerability class.

    Replaces the FCC-based logic of ``rmj.benchmark.defrate_per_class`` with
    direct binary-raster filtering:

    * "forest at initial year" = pixels where ``forest_file == forest_value``
    * "deforested during period" = pixels where ``deforestation_file == defor_value``

    No period names, no FCC pixel-value mapping.

    Parameters
    ----------
    forest_file : str or Path
        Binary forest raster at the initial year of the period.
        Pixels equal to ``forest_value`` are treated as "forest at start"
        (includes pixels that will be deforested during the period).
    deforestation_file : str or Path
        Binary deforestation raster for the period.
        Pixels equal to ``defor_value`` are treated as "deforested".
    vulnerability_file : str or Path
        Vulnerability map produced by ``vulnerability_map()``
        (UInt16, encoding ``vulnerability_class * 1000 + subj_id``).
    time_interval : int
        Number of years in the period.
    tab_file_defrate : str or Path
        Output CSV path for deforestation rates per vulnerability class.
    deforate_model : str or Path, optional
        CSV with rates from the model period (calibration or historical).
        When provided, the observed per-class rates are used only to compute
        a quantity-adjustment correction; predictions use the model rates.
        When ``None``, rates are computed entirely from the observed data.
    forest_value : int
        Pixel value in ``forest_file`` meaning "forest" (default: 1).
    defor_value : int
        Pixel value in ``deforestation_file`` meaning "deforested"
        (default: 1).
    blk_rows : int
        Number of raster rows per processing block (default: 128).

    Output CSV columns
    ------------------
    cat           Vulnerability class code (``class * 1000 + subj_id``)
    nfor          Forest-pixel count for this class
    ndefor        Deforested-pixel count for this class
    rate_obs      Annual observed deforestation rate
    rate_mod      Relative model rate (from deforate_model, or = ndefor/nfor)
    rate_abs      Absolute probability after quantity-adjustment correction
    time_interval Period length in years
    pixel_area    Pixel area in hectares
    defor_dens    Deforestation density (ha / pixel / year)
    """
    import pandas as pd
    import rasterio
    from rasterio.windows import Window

    # Maximum vulnerability category (30 vuln classes × up to ~1000 subj IDs)
    n_cat_max = 30999
    cat = list(range(1, n_cat_max + 1))

    data = {"cat": cat, "nfor": 0, "ndefor": 0}
    df = pd.DataFrame(data)

    with rasterio.open(forest_file) as f_src:
        pixel_area = (f_src.res[0] * f_src.res[1]) / 10000  # m² → ha
        height = f_src.height
        width = f_src.width

    # Block-by-block accumulation using a fixed block height (blk_rows)
    with (
        rasterio.open(forest_file) as f_src,
        rasterio.open(deforestation_file) as d_src,
        rasterio.open(vulnerability_file) as v_src,
    ):
        for row_start in range(0, height, blk_rows):
            row_count = min(blk_rows, height - row_start)
            window = Window(0, row_start, width, row_count)

            forest_data = f_src.read(1, window=window)
            defor_data = d_src.read(1, window=window)
            vuln_data = v_src.read(1, window=window)

            # Forest at initial year (the baseline population for rate computation)
            data_for = vuln_data[forest_data == forest_value]
            # Deforested during the period
            data_defor = vuln_data[defor_data == defor_value]

            cat_for = pd.Categorical(data_for.flatten(), categories=cat)
            df["nfor"] += cat_for.value_counts().values

            cat_defor = pd.Categorical(data_defor.flatten(), categories=cat)
            df["ndefor"] += cat_defor.value_counts().values

    # Drop classes with no forest pixels
    df = df[df["nfor"] != 0].copy()

    # Annual observed deforestation rate (informational; always computed)
    df["rate_obs"] = 1 - (1 - df["ndefor"] / df["nfor"]) ** (1 / time_interval)

    # Relative model rate: use supplied model rates or fall back to observed
    if deforate_model is not None:
        df_mod = pd.read_csv(deforate_model)
        df = df.merge(
            right=df_mod, on="cat", how="left", suffixes=(None, "_mod")
        )
    else:
        df["rate_mod"] = df["ndefor"] / df["nfor"]

    # Quantity-adjustment correction: predicted total deforestation = observed
    sum_ndefor = df["ndefor"].sum()
    sum_pi = (df["nfor"] * df["rate_mod"]).sum()
    correction_factor = sum_ndefor / sum_pi

    df["rate_abs"] = df["rate_mod"] * correction_factor
    df["time_interval"] = time_interval
    df["pixel_area"] = pixel_area
    df["defor_dens"] = df["rate_abs"] * pixel_area / time_interval

    tab_file_defrate = Path(tab_file_defrate)
    tab_file_defrate.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(tab_file_defrate, sep=",", header=True, index=False)

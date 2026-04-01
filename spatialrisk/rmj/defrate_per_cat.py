"""Compute deforestation rates per moving-window category from binary rasters.

Replaces ``riskmapjnr.defrate_per_cat`` with a generic implementation that
accepts separate binary forest and deforestation rasters instead of a
multi-period FCC stack.  The period-name logic and hard-coded FCC pixel-value
filters are removed entirely.
"""

from pathlib import Path
from typing import Union


def defrate_per_cat(
    forest_file: Union[str, Path],
    deforestation_file: Union[str, Path],
    riskmap_file: Union[str, Path],
    time_interval: int,
    tab_file_defrate: Union[str, Path],
    forest_value: int = 1,
    defor_value: int = 1,
    blk_rows: int = 256,
) -> None:
    """Compute deforestation rates per moving-window risk category.

    Accepts separate binary forest and deforestation rasters rather than a
    multi-period FCC stack.  Mirrors ``defrate_per_class`` for JNR but works
    with the uint16 ldefrate categories produced by ``local_defor_rate`` and
    ``set_defor_cat_zero``.

    Parameters
    ----------
    forest_file : str or Path
        Binary forest raster at the initial year of the period.
        Pixels equal to ``forest_value`` are treated as "forest at start"
        (includes pixels that will be deforested during the period).
    deforestation_file : str or Path
        Binary deforestation raster for the period.
        Pixels equal to ``defor_value`` are treated as "deforested".
    riskmap_file : str or Path
        Risk-category raster produced by ``set_defor_cat_zero``
        (uint16, values 1–65535; 0 = no-risk / beyond distance threshold).
    time_interval : int
        Number of years in the period.
    tab_file_defrate : str or Path
        Output CSV path for deforestation rates per risk category.
    forest_value : int
        Pixel value in ``forest_file`` meaning "forest" (default: ``1``).
    defor_value : int
        Pixel value in ``deforestation_file`` meaning "deforested"
        (default: ``1``).
    blk_rows : int
        Number of raster rows per processing block (default: ``256``).

    Output CSV columns
    ------------------
    cat           Risk-category pixel value (uint16, 1–65535)
    nfor          Forest-pixel count for this category
    ndefor        Deforested-pixel count for this category
    rate          Annual deforestation rate for this category
    time_interval Period length in years
    pixel_area    Pixel area in hectares
    defor_dens    Deforestation density (ha / pixel / year)
    """
    import numpy as np
    import pandas as pd
    import rasterio
    from rasterio.windows import Window

    # ldefrate categories: uint16 values 1–65535
    # 0 = zeroed (beyond dist_thresh or non-forest); excluded from accumulation
    n_cat_max = 65535
    cat = list(range(1, n_cat_max + 1))

    data = {"cat": cat, "nfor": np.zeros(n_cat_max, dtype=np.int64), "ndefor": np.zeros(n_cat_max, dtype=np.int64)}
    df = pd.DataFrame(data)

    with rasterio.open(forest_file) as f_src:
        pixel_area = (f_src.res[0] * f_src.res[1]) / 10000  # m² → ha
        height = f_src.height
        width = f_src.width

    with (
        rasterio.open(forest_file) as f_src,
        rasterio.open(deforestation_file) as d_src,
        rasterio.open(riskmap_file) as r_src,
    ):
        for row_start in range(0, height, blk_rows):
            row_count = min(blk_rows, height - row_start)
            window = Window(0, row_start, width, row_count)

            forest_data = f_src.read(1, window=window)
            defor_data = d_src.read(1, window=window)
            risk_data = r_src.read(1, window=window)

            # Forest at initial year (population for rate computation)
            # Only count pixels with a valid risk category (> 0)
            forest_mask = (forest_data == forest_value) & (risk_data > 0)
            data_for = risk_data[forest_mask]

            # Deforested during the period with a valid risk category
            defor_mask = (defor_data == defor_value) & (risk_data > 0)
            data_defor = risk_data[defor_mask]

            cat_for = pd.Categorical(data_for.flatten(), categories=cat)
            df["nfor"] += cat_for.value_counts().values

            cat_defor = pd.Categorical(data_defor.flatten(), categories=cat)
            df["ndefor"] += cat_defor.value_counts().values

    # Drop categories with no forest pixels
    df = df[df["nfor"] != 0].copy()

    # Annual deforestation rate per category
    df["rate"] = 1 - (1 - df["ndefor"] / df["nfor"]) ** (1 / time_interval)
    df["time_interval"] = time_interval
    df["pixel_area"] = pixel_area
    df["defor_dens"] = df["rate"] * pixel_area / time_interval

    tab_file_defrate = Path(tab_file_defrate)
    tab_file_defrate.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(tab_file_defrate, sep=",", header=True, index=False)

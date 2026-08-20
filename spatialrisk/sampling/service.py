"""Generate sample point locations from a raster + mask (Solara-free)."""
from pathlib import Path
from typing import Optional

from spatialrisk.sampling.types import SamplingStrategy
from spatialrisk.sampling.random import RandomSampling
from spatialrisk.sampling.stratified import StratifiedSampling
from spatialrisk.sampling.systematic import SystematicSampling

_STRATEGIES = {
    SamplingStrategy.random: RandomSampling,
    SamplingStrategy.stratified: StratifiedSampling,
    SamplingStrategy.systematic: SystematicSampling,
}


def generate_points(
    raster_path,
    mask_path: Optional[Path] = None,
    *,
    strategy: str,
    n_samples: Optional[int],
    allocation: Optional[str] = None,
    seed: Optional[int] = None,
    adapt: bool = False,
    spacing_m: Optional[float] = None,
):
    """Draw sample locations and return a GeoDataFrame of point centres."""
    import numpy as np
    import geopandas as gpd
    import rasterio
    from shapely.geometry import Point

    with rasterio.open(raster_path) as src:
        raster = src.read(1)
        transform = src.transform
        crs = src.crs
        nodata = src.nodata
        shape = raster.shape

    valid = ~np.isnan(raster)
    if nodata is not None:
        valid &= raster != nodata

    if mask_path is not None:
        with rasterio.open(mask_path) as msrc:
            mask = msrc.read(1)
            if mask.shape != shape:
                raise ValueError(
                    f"Mask shape {mask.shape} != raster shape {shape}; "
                    "raster and mask must be co-registered."
                )
            m_valid = mask != 0
            if msrc.nodata is not None:
                m_valid &= mask != msrc.nodata
        valid &= m_valid

    valid_indices = np.where(valid)
    strata_values = raster[valid_indices]

    # pixel area in hectares from a projected transform (m^2 -> ha)
    pixel_area_ha = abs(transform.a * transform.e) / 10_000.0
    # pixel size (row, col) in metres for distance-based spacing
    res_m = (abs(transform.e), abs(transform.a))

    strat = SamplingStrategy(strategy)
    impl = _STRATEGIES[strat]()
    rows, cols = impl.select(
        valid_indices,
        n_samples=n_samples,
        seed=seed,
        strata_values=strata_values,
        shape=shape,
        allocation=allocation,
        adapt=adapt,
        pixel_area_ha=pixel_area_ha,
        spacing_m=spacing_m,
        res_m=res_m,
    )

    xs, ys = rasterio.transform.xy(transform, list(rows), list(cols), offset="center")
    gdf = gpd.GeoDataFrame(
        {
            "strata": raster[rows, cols].astype(int),
            "row": np.asarray(rows, dtype=int),
            "col": np.asarray(cols, dtype=int),
        },
        geometry=[Point(x, y) for x, y in zip(xs, ys)],
        crs=crs,
    )
    return gdf

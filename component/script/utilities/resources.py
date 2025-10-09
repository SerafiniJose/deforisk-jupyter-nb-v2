import os
import uuid
import psutil
import numpy as np
import pandas as pd
from osgeo import gdal
from concurrent.futures import ThreadPoolExecutor, as_completed


# ================================================================
# Helpers
# ================================================================
def estimate_raster_size(path):
    """Return (ny, nx, nband, est_bytes, est_gb) for a raster."""
    r = gdal.Open(path)
    ny, nx = r.RasterYSize, r.RasterXSize
    nband = r.RasterCount

    # Get actual pixel size in bytes using GDAL's data type info
    band = r.GetRasterBand(1)
    bytes_per_pixel = gdal.GetDataTypeSizeInBytes(band.DataType)

    est_bytes = ny * nx * nband * bytes_per_pixel  # Dynamically calculated
    est_gb = est_bytes / (1024**3)
    return ny, nx, nband, est_bytes, est_gb


def optimal_block_shape(ny, nx, nband, ram_gb, ram_fraction=0.7):
    """
    Estimate optimal block height for sequential reading given available RAM.
    Returns (blk_rows, blk_cols).
    """
    target_gb = ram_gb * ram_fraction
    bytes_per_row = nx * nband * 4  # float32
    max_rows = int((target_gb * 1024**3) / bytes_per_row)
    max_rows = max(1, min(ny, max_rows))
    return max_rows, nx

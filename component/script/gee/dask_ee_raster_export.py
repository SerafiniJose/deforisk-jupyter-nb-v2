# --------------------------------------------------------------
#  dask_ee_raster_export.py
# --------------------------------------------------------------

"""
Utility that wraps a raster export from Earth Engine to Dask.

The helper keeps the following items as explicit function parameters:

*'client'           :class:'dask.distributed.Client'
*'ee_img'           an''ee.Image'' instance (serialisable via''serialize()'')
*'region_geom'      optional geometry or featurecollection that defines
                      the export window
*'scale','crs', …  all raster specific parameters

Everything else is forwarded to :func:'download_ee_image'.

The implementation follows the pattern of the original 'dask_safe_export_raster_no_geemap'
wrapper but adds type annotations and a clean public API.
"""

# ------------------------------------------------------------------
# Imports
# ------------------------------------------------------------------
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional, Union

import ee  # Earth Engine
import geedim as gd  # used only inside the worker – imported here for typing
import geemap  # (only required for its'ee_export_vector' fallback)
from dask.distributed import Client, Future


# ------------------------------------------------------------------
# Public helper
# ------------------------------------------------------------------
def export_raster_with_dask(
    ee_img: ee.Image,
    filename: str,
    scale: float,
    crs: str,
    region_geom: Optional[Union[ee.Geometry, ee.FeatureCollection]] = None,
    unmask_value: Optional[int] = None,
    nodata_value: Optional[float] = None,
    client: Client = None,
    project: [str] = None,
    overwrite: bool = False,
    **kwargs: Any,
) -> Future:
    """
    Export an Earth-Engine image to a GeoTIFF via Dask.

    Parameters
    ----------
    ee_img : ee.Image
        The EE image you want to download.  Must be serialisable by''serialize()''.
    filename : str
        Target file path for the exported GeoTIFF.
    scale : float
        Desired pixel resolution (meters).
    crs : str
        CRS in EPSG or WKT notation to reproject the image to.
    region_geom : ee.Geometry | ee.FeatureCollection | None, optional
        Geometry/feature collection that defines the export window.  If''None'',
        the entire image granule is exported.
    unmask_value : float | None, optional
        Value used for masked pixels.  Set to a non zero value if your data
        contains zeros that you do not want treated as missing.
    nodata_value : int | None, optional
        Value used for no data raster value.
    client : dask.distributed.Client
        The Dask client that will run the export job.
    project : str | None, optional
        Earth Engine project name.  If omitted the default EE project will be used.
    overwrite : bool, optional
        If False (default), the function will skip export if the file already exists.
        If True, the function will overwrite existing files.
    **kwargs : Any
        Additional keyword arguments forwarded verbatim to''download_ee_image''
        (e.g.''overwrite=True'',''resampling="bilinear"'', etc.).

    Returns:
    -------
    dask.distributed.Future
        Future that resolves to the result of'download_ee_image'
        (normally''None'' you use it for its side effects).
    """
    # ------------------------------------------------------------------
    # 1. Check if file exists and overwrite is False
    # ------------------------------------------------------------------
    if not overwrite and Path(filename).exists():
        logging.warning(
            f"File {filename} already exists and overwrite=False. Skipping export."
        )
        # Return a completed future to continue execution without error
        return client.submit(lambda: None)

    # ------------------------------------------------------------------
    # 2. Ensure Earth Engine is initialised locally
    # ------------------------------------------------------------------
    # if not ee.data._credentials:
    #     ee.Initialize(project=project)

    # ------------------------------------------------------------------
    # 3. Serialise the EE objects so they can be sent to a worker
    # ------------------------------------------------------------------
    ee_img_json = ee_img.serialize()
    region_json: Optional[str] = None
    if region_geom is not None:
        region_json = region_geom.serialize()

    # ------------------------------------------------------------------
    # 4. Worker function – runs on a Dask worker
    # ------------------------------------------------------------------
    def _raster_export(
        img_json: str,
        fn: str,
        scl: int | float,
        crs_name: str,
        reg_json: Optional[str],
        um_val: Optional[int],
        nd_val: Optional[int],
        proj: [str] = None,
        overw: bool = False,
        **kw: Any,
    ) -> Any:
        """
        Minimal wrapper that reinitialises EE and calls''download_ee_image''.
        All arguments are typed for clarity.
        """
        import ee  # Import inside the worker
        import geedim as gd

        ee.Initialize(project=proj)

        img = ee.deserializer.fromJSON(img_json)
        region: Optional[Union[ee.Geometry, ee.FeatureCollection]] = None
        if reg_json is not None:
            region = ee.deserializer.fromJSON(reg_json)

        # If'unmask_value' is supplied we apply it before export.
        if um_val is not None and isinstance(
            region, (ee.Geometry, ee.FeatureCollection)
        ):
            img = img.unmask(um_val, sameFootprint=False).clip(region)

        return download_ee_image(
            image=img,
            filename=fn,
            scale=scl,
            crs=crs_name,
            region=region,
            unmask_value=um_val,
            nodata_value=nd_val,
            overwrite=overw,
            **kw,
        )

    # ------------------------------------------------------------------
    # 4. Submit the task to Dask and return the Future
    # ------------------------------------------------------------------
    return client.submit(
        _raster_export,
        ee_img_json,
        filename,
        scale,
        crs,
        region_json,
        unmask_value,
        nodata_value,
        proj=project,
        overw=overwrite,
        **kwargs,
    )


# ------------------------------------------------------------------
#  Helper that actually performs the download
# ------------------------------------------------------------------
def download_ee_image(
    image: ee.Image,
    filename: str,
    region: Optional[Union[ee.Geometry, ee.FeatureCollection]] = None,
    crs: Optional[str] = None,
    crs_transform: Optional[list] = None,
    scale: Optional[float] = None,
    resampling: str = "near",
    dtype: Optional[str] = None,
    overwrite: bool = True,
    num_threads: Optional[int] = None,
    max_tile_size: Optional[int] = None,
    max_tile_dim: Optional[int] = None,
    shape: Optional[tuple[int, int]] = None,
    scale_offset: bool = False,
    unmask_value: Optional[int] = None,
    nodata_value: Optional[int] = None,
    **kwargs: Any,
) -> None:
    """
    Download an Earth Engine Image as a GeoTIFF.

    Parameters
    ----------
    image : ee.Image
        The image to be downloaded.
    filename : str
        Name of the destination file.
    region : ee.Geometry | ee.FeatureCollection | None, optional
        Region defined by geojson polygon in WGS84. Defaults to the entire image granule.
    crs : str | None, optional
        Reproject image(s) to this EPSG or WKT CRS.  Where image bands have different CRSs,
        all are re‑projected to this CRS. Defaults to the CRS of the minimum scale band.
    crs_transform : list[float] | None, optional
        List of 6 numbers specifying an affine transform in the specified CRS.
    scale : float | None, optional
        Resample image(s) to this pixel scale (meters).  Where image bands have different scales,
        all are resampled to this scale. Defaults to the minimum scale of image bands.
    resampling : str, optional
        Resampling method 'near', 'bilinear', 'bicubic', or 'average'.
    dtype : str | None, optional
        Convert to this data type ('uint8','int8','uint16','int16','uint32','int32',
       'float32' or'float64').  Defaults to auto select.
    overwrite : bool, optional
        Overwrite the destination file if it exists. Defaults to True.
    num_threads : int | None, optional
        Number of tiles to download concurrently.
    max_tile_size : int | None, optional
        Maximum tile size (MB).  If None, defaults to the Earth Engine download size limit (32 MB).
    max_tile_dim : int | None, optional
        Maximum tile width/height (pixels).  Defaults to Earth Engine download limit (10 000).
    shape : tuple[int, int] | None, optional
        Desired output dimensions (height, width) in pixels.
    scale_offset : bool, optional
        Whether to apply any EE band scales and offsets to the image.
    unmask_value : int | None, optional
        Value used for masked pixels.  Set to a non zero value if you want zeros to be treated as data.
    nodata_value : int | None, optional
        Value used for no data raster value.

    Returns:
    -------
    None
        The function writes the GeoTIFF in place; it returns''None''.
    """
    # The original implementation has an early exit when running under MkDocs,
    # which we preserve for compatibility.
    if getattr(os, "environ", {}).get("USE_MKDOCS") is not None:
        return

    try:
        import geedim as gd
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "Please install geedim using'pip install geedim' or "
            "'conda install -c conda-forge geedim'"
        ) from exc

    if not isinstance(image, ee.Image):  # pragma: no cover
        raise ValueError("image must be an ee.Image.")

    # Apply unmasking/clip logic before export.
    if unmask_value is not None:
        if isinstance(region, (ee.Geometry, ee.FeatureCollection)):
            image = image.clip(region)
        image = image.unmask(unmask_value, sameFootprint=False)

    img = image.gd.prepareForExport(
        crs=crs,
        region=region,
        scale=scale,
        resampling=resampling,
        dtype=dtype,
    )

    if nodata_value is None:
        img.gd.toGeoTIFF(file=filename, overwrite=overwrite, nodata=True, **kwargs)
    elif nodata_value is not None:
        img.gd.toGeoTIFF(
            file=filename, overwrite=overwrite, nodata=nodata_value, **kwargs
        )


# ------------------------------------------------------------------
#  Example usage (commented out – uncomment to run in a real environment)
# ------------------------------------------------------------------
# if __name__ == "__main__":
#     from dask.distributed import Client
#
#     client = Client()  # or connect to an existing cluster
#
#     # Build the EE image and region geometry (e.g. from'geemap' or direct EE API)
#     ee_image = ee.Image("COPERNICUS/S2_SR/20200101")   # example
#     aoi_geom = ee.Geometry.Rectangle([0, 0, 10, 10])    # example
#
#     future = export_raster_with_dask(
#         client,
#         ee_img=ee_image,
#         filename="output.tif",
#         scale=30.0,
#         crs="EPSG:4326",
#         region_geom=aoi_geom,
#         unmask_value=255,
#     )
#
#     future.result()  # wait for completion

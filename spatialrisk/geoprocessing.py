"""Stateless variable-geoprocessing functions.

Pure offload seams extracted from ``variables/local_raster_var.py`` and
``variables/local_vector_var.py``. Each function takes an EXPLICIT ``out_path``
plus an input spec plus a base/geobox, RETURNS a new ``LocalRasterSpec``, and
NEVER reaches into a live ``Project`` (no live-Project attribute access, no
implicit save).
Numeric/geospatial behavior is the verbatim current path: these wrap the
existing primitives in ``geo_utils`` / ``processing`` only.
"""

from __future__ import annotations

from spatialrisk.document import LocalRasterSpec
from spatialrisk.geo_utils import xr_reproject
from spatialrisk.variables.models import PostProcessing, RasterizationMethod, RasterType


def reproject_and_match(
    in_spec: "LocalRasterSpec",
    geobox,
    out_path: str,
    resampling: str | None = None,
    output_suffix: str = "reprojected_matched",
) -> "LocalRasterSpec":
    """Reproject ``in_spec``'s raster to match ``geobox``, writing to ``out_path``.

    Stateless replacement for ``LocalRasterVar.reproject_and_match``: explicit
    ``out_path`` in, new ``LocalRasterSpec`` out, no live Project. Resampling
    auto-selection and the underlying ``xr_reproject`` call are verbatim from
    the old method so output is byte-for-byte identical.
    """
    if resampling is None:
        if in_spec.raster_type == RasterType.categorical:
            resampling = "nearest"
        elif in_spec.raster_type == RasterType.continuous:
            resampling = "bilinear"
        else:
            resampling = "nearest"

    xr_reproject(
        raster_path=str(in_spec.path),
        geobox=geobox,
        resampling_method=resampling,
        output_path=str(out_path),
    )

    target_crs = f"EPSG:{geobox.crs.to_epsg()}"
    target_resolution = abs(geobox.resolution.x)
    new_history = (*in_spec.processing_history, output_suffix)

    return LocalRasterSpec(
        name=in_spec.name,
        year=in_spec.year,
        active=in_spec.active,
        tags=in_spec.tags,
        path=str(out_path),
        raster_type=in_spec.raster_type,
        post_processing=in_spec.post_processing,
        processing_history=new_history,
        default_crs=target_crs,
        default_resolution=target_resolution,
        derived_from=in_spec.name,
    )


def rasterize_vector(*args, **kwargs):  # implemented in F3
    raise NotImplementedError


def apply_post_processing(*args, **kwargs):  # implemented in F4
    raise NotImplementedError

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
from spatialrisk.processing import distance_to_edge_gdal_no_mask, xr_rasterize
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


def rasterize_vector(
    in_spec: "LocalVectorSpec",
    base_geobox,
    out_path: str,
    rasterization_method: "RasterizationMethod | None" = None,
    **kwargs,
) -> "LocalRasterSpec":
    """Rasterize ``in_spec``'s vector onto ``base_geobox``, writing to ``out_path``.

    Stateless replacement for ``LocalVectorVar.rasterize``: the geobox is passed
    in explicitly (no ``base.get_base_geobox()`` reach-through), the output path
    is explicit, and a new ``LocalRasterSpec`` is returned. The mode mapping and
    the ``xr_rasterize`` call are verbatim from the old method.
    """
    _method = rasterization_method or in_spec.rasterization_method
    if _method is None:
        raise ValueError(
            "rasterization_method must be provided either as an argument or set "
            "on the LocalVectorSpec"
        )

    mode_mapping = {
        RasterizationMethod.binary: "binary",
        RasterizationMethod.unique: "unique",
    }
    mode = mode_mapping.get(_method, "binary")

    xr_rasterize(
        shapefile_path=str(in_spec.path),
        geobox=base_geobox,
        output_path=str(out_path),
        mode=mode,
        **kwargs,
    )

    raster_type = (
        RasterType.categorical if mode == "unique" else RasterType.continuous
    )

    return LocalRasterSpec(
        name=in_spec.name,
        year=in_spec.year,
        active=in_spec.active,
        tags=in_spec.tags,
        path=str(out_path),
        raster_type=raster_type,
        default_crs=in_spec.default_crs,
        processing_history=("rasterized",),
        derived_from=in_spec.name,
    )


def apply_post_processing(
    in_spec: "LocalRasterSpec",
    post_process: "PostProcessing",
    out_path: str,
) -> "LocalRasterSpec":
    """Apply an edge/dist distance transform to ``in_spec``, writing to ``out_path``.

    Stateless replacement for ``LocalRasterVar.apply_post_processing`` /
    ``_create_post_var``: explicit ``out_path`` in, new ``LocalRasterSpec`` out,
    no live Project. The ``distance_to_edge_gdal_no_mask`` argument set per step
    is verbatim from the old method (edge -> values=0; dist -> values=1).
    """
    try:
        step = PostProcessing(post_process)  # validate / normalize
    except ValueError as exc:
        raise ValueError(
            f"Unknown post-processing step: {post_process!r}"
        ) from exc

    if step == PostProcessing.edge:
        # Distance to feature pixels (value==1): VALUES=0.
        values = 0
    elif step == PostProcessing.dist:
        # Distance from background (value==0) into the feature: VALUES=1.
        values = 1
    else:  # pragma: no cover - PostProcessing has only edge/dist
        raise ValueError(f"Unknown post-processing step: {post_process}")

    distance_to_edge_gdal_no_mask(
        input_file=str(in_spec.path),
        dist_file=str(out_path),
        values=values,
        nodata=0,
        max_distance_value=4294967295,
        input_nodata=True,
        verbose=False,
    )

    suffix = step.value
    new_name = f"{in_spec.name}_{suffix}"
    new_history = (*in_spec.processing_history, suffix)
    new_post = (*in_spec.post_processing, step)

    return LocalRasterSpec(
        name=new_name,
        year=in_spec.year,
        active=in_spec.active,
        tags=in_spec.tags,
        path=str(out_path),
        raster_type=RasterType.continuous,
        post_processing=new_post,
        processing_history=new_history,
        default_crs=in_spec.default_crs,
        default_resolution=in_spec.default_resolution,
        derived_from=in_spec.name,
    )

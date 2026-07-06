"""Pure orchestration for the Process tab (no Solara).

Mirrors notebooks/2.process_factory.ipynb: download GEE layers to local,
set a reprojected base raster, generate deferred forest-loss targets, then
reproject/match + rasterize, and apply edge/dist post-processing.
"""

import logging
from pathlib import Path
from typing import List

from spatialrisk.processing import make_forest_loss_var

logger = logging.getLogger("spatial_risk")


def _is_geevar(var) -> bool:
    """True if a variable still needs downloading (GEE-backed, not local)."""
    return type(var).__name__ == "GEEVar"


def materialize_raw_layers(project) -> List[str]:
    """Download every raw GEEVar to a local var, replacing it in raw_variables.

    Raster GEEVars -> to_local_raster(); vector GEEVars -> to_local_vector().
    Idempotent: already-local variables are skipped. Returns the list of keys
    that were materialized.
    """
    from spatialrisk.variables.models import DataType

    from spatialrisk.log_utils import log_progress

    # Snapshot pairs: to_local_*().add_as_raw() mutates raw_variables in place.
    pending = [(k, v) for k, v in list(project.raw_variables.items()) if _is_geevar(v)]
    if pending:
        logger.info("Downloading %d GEE layer(s)…", len(pending))

    materialized: List[str] = []
    for key, var in log_progress(pending, "Downloading layer", label=lambda kv: kv[0]):
        if var.data_type == DataType.vector:
            local = var.to_local_vector()
        else:
            local = var.to_local_raster()
        # Single-image GEEVars return one var; lists are flattened defensively.
        locals_ = local if isinstance(local, list) else [local]
        for lv in locals_:
            lv.add_as_raw(auto_save=False)
        materialized.append(key)

    if materialized:
        logger.info("Downloaded %d layer(s).", len(materialized))
    return materialized


def auto_utm_epsg(path) -> str:
    """Compute the UTM EPSG for a raster path via calculate_utm_rioxarray."""
    from spatialrisk.geo_utils import calculate_utm_rioxarray

    epsg = calculate_utm_rioxarray(Path(path))
    epsg = str(epsg)
    return epsg if epsg.startswith("EPSG:") else f"EPSG:{epsg}"


def base_raster_resolution(var) -> "float | None":
    """Native pixel resolution (m) of a raw raster var, for pre-filling the base field.

    Prefers the var's recorded scale (``default_resolution`` / ``default_scale``, in
    metres). Falls back to the GeoTIFF's native pixel size, converting degrees -> metres
    for geographic CRSs. Returns ``None`` when nothing is available.
    """
    res = getattr(var, "default_resolution", None) or getattr(var, "default_scale", None)
    if res:
        return float(res)
    path = getattr(var, "path", None)
    if path is None:
        return None
    import rasterio

    with rasterio.open(path) as src:
        xres = abs(src.res[0])
        if src.crs is not None and src.crs.is_geographic:
            xres *= 111320.0  # approx metres per degree of longitude at the equator
    return float(xres)


def set_base_raster(project, base_key: str, epsg: str, resolution: float):
    """Reproject the chosen raw raster to `epsg`/`resolution` and set it as base."""
    base = project.raw_variables[base_key]
    reprojected = base.reproject(target_epsg=epsg, resolution=resolution)
    reprojected.use_as_base_raster()
    return reprojected


def generate_forest_loss_targets(project) -> List:
    """Materialize every ForestLossSpec into a raw forest-loss variable."""
    created = []
    for spec in project.forest_loss_specs:
        start = project.raw_variables.get(spec.start_key)
        end = project.raw_variables.get(spec.end_key)
        if start is None or end is None:
            logger.warning(
                "forest-loss target '%s' skipped: missing %s",
                spec.name,
                spec.start_key if start is None else spec.end_key,
            )
            continue
        var = make_forest_loss_var(project, start, end)
        var.add_as_raw(auto_save=False)
        created.append(var)
    return created


def run_processing(project) -> None:
    """Full Process run, in notebook order. Requires base_raster to be set."""
    if project.base_raster is None:
        raise ValueError("Set a base raster before running processing.")
    materialize_raw_layers(project)
    generate_forest_loss_targets(project)
    logger.info("Reprojecting & matching all raw variables…")
    project.reproject_and_match_all(source="raw")
    logger.info("Rasterizing all raw variables…")
    project.rasterize_all(source="raw")
    project.save()
    logger.info("Processing complete.")


def apply_post_processing(project, processed_key: str, step: str):
    """Apply edge/dist to a processed variable and register the result."""
    logger.info("Applying %s to %s…", step, processed_key)
    var = project.processed_variables[processed_key]
    derived = var.apply_post_processing(step)
    derived.add_as_processed()
    logger.info("%s complete for %s.", step, processed_key)
    return derived


def forest_loss_candidates(project) -> dict:
    """Map variable name -> sorted years for raw raster vars with >=2 years.

    These are the temporal masks (e.g. forest_gfc) a forest-loss target can be
    derived from. Static vars (year is None) and vectors are excluded.
    """
    from spatialrisk.variables.models import DataType

    years_by_name: dict = {}
    for var in project.raw_variables.values():
        if getattr(var, "data_type", None) == DataType.vector:
            continue
        if getattr(var, "year", None) is None:
            continue
        years_by_name.setdefault(var.name, set()).add(var.year)
    return {
        name: sorted(years)
        for name, years in years_by_name.items()
        if len(years) >= 2
    }


def add_forest_loss_spec(project, var_name: str, start_year: int, end_year: int):
    """Append a ForestLossSpec for a (start -> end) forest pair. Idempotent."""
    from spatialrisk.variables.models import ForestLossSpec

    if start_year >= end_year:
        raise ValueError("start_year must be earlier than end_year.")
    name = f"forest_loss_{start_year}_{end_year}"
    if any(s.name == name for s in project.forest_loss_specs):
        return next(s for s in project.forest_loss_specs if s.name == name)
    spec = ForestLossSpec(
        name=name,
        start_key=f"{var_name}_{start_year}",
        end_key=f"{var_name}_{end_year}",
        start_year=start_year,
        end_year=end_year,
    )
    project.forest_loss_specs.append(spec)
    return spec


def change_layer_candidates(project) -> List[str]:
    """Sorted keys of processed temporal raster vars (change-detection inputs).

    Any two of these can be paired — same source or cross-source; both must be
    presence masks (1 = present, 0 = absent) of the same phenomenon, which is
    the user's responsibility.
    """
    from spatialrisk.variables.models import DataType

    return sorted(
        k
        for k, v in project.processed_variables.items()
        if getattr(v, "data_type", None) != DataType.vector
        and getattr(v, "year", None) is not None
    )


def _check_same_grid(start_var, end_var) -> None:
    """Raise if the two processed rasters are not on the same grid.

    Post-alignment they always should be; a mismatch means one predates the
    current base raster — differencing it would produce garbage.
    """
    import rasterio

    with rasterio.open(start_var.path) as a, rasterio.open(end_var.path) as b:
        if a.crs != b.crs or a.transform != b.transform or a.shape != b.shape:
            raise ValueError(
                f"'{start_var.name}' and '{end_var.name}' are not on the same "
                "grid — re-run Process so both layers are aligned to the base "
                "raster."
            )


def generate_change_var(project, op: str, start_key: str, end_key: str):
    """Generate a loss/gain change layer from two aligned processed masks.

    Output convention: 1 = event, 0 = stable, 255 = nodata. Registers the
    result as a static processed variable and saves the project. Idempotent:
    an existing variable (or output file) is reused, and reuse of an existing
    variable does NOT save the project.
    """
    from spatialrisk.variables import LocalRasterVar
    from spatialrisk.variables.models import RasterType

    if op not in ("loss", "gain"):
        raise ValueError(f"op must be 'loss' or 'gain', got {op!r}")
    if start_key == end_key:
        raise ValueError("Choose two different layers.")

    start = project.processed_variables.get(start_key)
    end = project.processed_variables.get(end_key)
    if start is None:
        raise ValueError(f"Processed variable '{start_key}' not found.")
    if end is None:
        raise ValueError(f"Processed variable '{end_key}' not found.")

    y1 = getattr(start, "year", None)
    y2 = getattr(end, "year", None)
    if y1 is None or y2 is None:
        raise ValueError("Both layers must be temporal (have a year).")
    if y1 >= y2:
        raise ValueError("The start layer's year must be earlier than the end layer's.")

    if start.name == end.name:
        name = f"{op}_{start.name}_{y1}_{y2}"
    else:
        name = f"{op}_{start.name}_{y1}_{end.name}_{y2}"

    existing = project.processed_variables.get(name)
    if existing is not None:
        logger.info("Change layer '%s' already exists — reusing it.", name)
        return existing

    _check_same_grid(start, end)

    out_path = Path(project.folders.processed_data_folder) / f"{name}.tif"
    if not out_path.exists():
        from spatialrisk.processing import process_change_xarray

        logger.info("Generating %s layer '%s'…", op, name)
        process_change_xarray(str(start.path), str(end.path), str(out_path), op=op)

    var = LocalRasterVar(
        name=name,
        path=out_path,
        raster_type=RasterType.categorical,
        project=project,
        tags=[op, "change", f"{y1}_{y2}"],
    )
    var.add_as_processed(auto_save=False)
    project.save()
    logger.info("Change layer '%s' registered.", name)
    return var

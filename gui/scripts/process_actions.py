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

    materialized: List[str] = []
    # Snapshot keys: to_local_*().add_as_raw() mutates raw_variables in place.
    for key, var in list(project.raw_variables.items()):
        if not _is_geevar(var):
            continue
        if var.data_type == DataType.vector:
            local = var.to_local_vector()
        else:
            local = var.to_local_raster()
        # Single-image GEEVars return one var; lists are flattened defensively.
        locals_ = local if isinstance(local, list) else [local]
        for lv in locals_:
            lv.add_as_raw(auto_save=False)
        materialized.append(key)
    return materialized


def auto_utm_epsg(path) -> str:
    """Compute the UTM EPSG for a raster path via calculate_utm_rioxarray."""
    from spatialrisk.geo_utils import calculate_utm_rioxarray

    epsg = calculate_utm_rioxarray(Path(path))
    epsg = str(epsg)
    return epsg if epsg.startswith("EPSG:") else f"EPSG:{epsg}"


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
    project.reproject_and_match_all(source="raw")
    project.rasterize_all(source="raw")
    project.save()


def apply_post_processing(project, processed_key: str, step: str):
    """Apply edge/dist to a processed variable and register the result."""
    var = project.processed_variables[processed_key]
    derived = var.apply_post_processing(step)
    derived.add_as_processed()
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

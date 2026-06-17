"""The dead dask.distributed export/geoprocessing modules must be gone."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DEAD = [
    "spatialrisk/gee/dask_ee_raster_export.py",
    "spatialrisk/gee/dask_ee_vector_export.py",
    "spatialrisk/xarray/dask_distance_xarray_spatial.py",
    "spatialrisk/xarray/dask_reproject_rio.py",
]


def test_dead_dask_modules_deleted():
    present = [p for p in DEAD if (REPO_ROOT / p).exists()]
    assert not present, f"dead dask modules still present: {present}"


def test_no_remaining_imports_of_dead_modules():
    # No source file should reference these modules by import path.
    needles = ("dask_ee_raster_export", "dask_ee_vector_export",
               "dask_distance_xarray_spatial", "dask_reproject_rio")
    offenders = []
    for py in (REPO_ROOT / "spatialrisk").rglob("*.py"):
        text = py.read_text()
        for n in needles:
            if f"import {n}" in text or f"from spatialrisk.gee.{n}" in text \
               or f"from spatialrisk.xarray.{n}" in text:
                offenders.append(f"{py}: {n}")
    assert not offenders, offenders

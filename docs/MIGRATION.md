<!-- docs/MIGRATION.md -->
# Migration: legacy `Project`/`GEEVar` → `ProjectSession`

The Project target-state rewrite replaces the live, auto-saving `Project` +
`Variable` objects (which embedded live `ee` images and a `project=` back-ref)
with an immutable `ProjectDocument` driven by an ergonomic `ProjectSession`.
GEE recipes are stored declaratively (`CatalogueRecipe` / `AssetRecipe`) and
materialized by the single `GEEAdapter`; persistence is now explicit.

## Call-site mapping

| Before (legacy) | After (Session API) |
| --- | --- |
| `from spatialrisk.project import Project` | `from spatialrisk.session import ProjectSession` |
| `project = Project(project_name=name)` | `session = ProjectSession.create(name)` |
| `project = Project.load(project_name=name)` | `session = ProjectSession.open(name, store=store)` |
| `GEEVar(name=..., gee_images=[live_ee_image], aoi=ee_geom, project=p)` | `session.add_gee_variable(GEESpec(name=..., recipe=CatalogueRecipe(catalogue_key=..., params={...}, export_kind="raster")))` |
| ad-hoc user asset via `GEEVar(gee_images=[ee.Image("users/...")])` | `session.add_gee_variable(GEESpec(recipe=AssetRecipe(asset_id="users/...", band=..., export_kind="raster")))` |
| `var.to_local_raster(); var.add_as_raw()` | `session.add_gee_variable(...)` then `session.process_all()` (materialize is part of orchestration) |
| `LocalRasterVar(name=..., path=..., project=p).add_as_raw()` | `session.add_local_raster(LocalRasterSpec(name=..., path=..., raster_type=...))` |
| `LocalVectorVar(name=..., path=..., project=p).add_as_raw()` | `session.add_local_vector(LocalVectorSpec(name=..., path=..., rasterization_method=...))` |
| `project.reproject_and_match_all()` | `session.process_all()` (= materialize → reproject → rasterize) |
| `project.base_raster = var` | `session.set_base_raster(VariableId(source=..., name=..., year=...))` |
| AOI from `aoi_var.to_local_vector()` + `project.aoi` | `session.add_gee_variable(GEESpec(recipe=CatalogueRecipe(catalogue_key="aoi_fao_gaul", ...)))` → `session.materialize_all()` → read the product → `session.set_aoi(aoi_geojson_geometry)` |
| `model.fit()` | `ModelHandle.fit()` |
| `model.apply(output_file=...)` | `ModelHandle.apply(out_path=...)` |
| auto-save after every mutation | explicit `session.save()` |

## Removed / replaced internals

- **`geemap`** is no longer used; GEE vector export is geemap-free
  (`fc.getDownloadURL(filetype="SHP", selectors=...)` / `getInfo()` →
  `geopandas.GeoDataFrame.from_features`).
- **Dead `dask.distributed` modules** removed:
  `spatialrisk/gee/dask_ee_raster_export.py`,
  `spatialrisk/gee/dask_ee_vector_export.py`,
  `spatialrisk/xarray/dask_distance_xarray_spatial.py`,
  `spatialrisk/xarray/dask_reproject_rio.py`.
- The single-process raster write in `remap_xarray.py` no longer uses
  `dask.distributed.Lock`; it uses a local `threading.Lock`.

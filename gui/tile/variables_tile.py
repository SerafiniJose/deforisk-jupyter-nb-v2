"""Step 2 — Variables tile."""

import asyncio
import logging
from pathlib import Path

import ee
import solara

logger = logging.getLogger("spatial_risk")

from gui.widget.variable_list import DerivedVariableList, SourceVariableList
from gui.widget.variable_modal import VariableModal
from spatialrisk.project import Project
from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.local_raster_var import LocalRasterVar
from spatialrisk.variables.local_vector_var import LocalVectorVar

LocalRasterVar.model_rebuild()
GEEVar.model_rebuild()
LocalVectorVar.model_rebuild()

# Keys of source variables currently displayed on the map (drives the toggle state).
vars_on_map = solara.reactive(set())


def _map_layer_key(key: str) -> str:
    """Unique map-layer key for a source variable."""
    return f"var_{key}"


async def _grayscale_vis(image, var, gee_interface):
    """Grayscale palette stretched to the image's min/max over its AOI.

    Falls back to a bare grayscale palette (GEE's default 0–1 stretch) if the
    min/max can't be computed.
    """
    vis = {"palette": ["000000", "ffffff"]}
    aoi = getattr(var, "aoi", None)
    if aoi is None or gee_interface is None:
        return vis
    try:
        geom = aoi if isinstance(aoi, ee.Geometry) else aoi.geometry()
        stats = await gee_interface.get_info_async(
            image.reduceRegion(
                reducer=ee.Reducer.minMax(),
                geometry=geom,
                scale=getattr(var, "default_scale", None) or 100,
                maxPixels=1e8,
                bestEffort=True,
            )
        )
        mins = [v for k, v in stats.items() if k.endswith("_min") and v is not None]
        maxs = [v for k, v in stats.items() if k.endswith("_max") and v is not None]
        if mins and maxs:
            vis["min"], vis["max"] = min(mins), max(maxs)
    except Exception:
        logger.debug("grayscale min/max failed; using default stretch", exc_info=True)
    return vis


async def _styled_layer(image, var, gee_interface):
    """Choose visualization by raster type.

    categorical (incl. binary masks) -> ``ee.Image.randomVisualizer()`` (random RGB);
    continuous -> grayscale stretched to the image's min/max.

    Returns (image_to_add, vis_params).
    """
    rt = getattr(var, "raster_type", None)
    rt = rt.value if hasattr(rt, "value") else (str(rt) if rt is not None else "")
    if rt == "categorical":
        return image.randomVisualizer(), {}
    return image, await _grayscale_vis(image, var, gee_interface)


def _variable_to_entry(key: str, var, project) -> dict:
    """Reconstruct a modal entry dict from an existing variable object."""
    vtype = type(var).__name__
    is_base = project.base_raster is not None and project.base_raster.name == var.name
    pp = [p.value if hasattr(p, "value") else str(p) for p in (var.post_processing or [])]
    entry = {
        "type": vtype,
        "name": var.name,
        "year": str(var.year) if var.year else "",
        "is_base": is_base,
        "post_processing": pp,
    }
    if vtype == "LocalRasterVar":
        entry["path"] = str(var.path)
        entry["raster_type"] = var.raster_type.value if hasattr(var.raster_type, "value") else str(var.raster_type)
    elif vtype == "GEEVar":
        entry["asset_id"] = str(var.path)
        entry["scale"] = str(var.default_scale) if getattr(var, "default_scale", None) else ""
    elif vtype == "LocalVectorVar":
        entry["path"] = str(var.path)
        entry["rasterization_method"] = var.rasterization_method.value if hasattr(var.rasterization_method, "value") else str(var.rasterization_method)
    return entry


def _build_variable(entry: dict, project):
    """Instantiate the correct variable class from a modal entry dict."""
    if entry.get("source") == "predefined":
        return _build_predefined(entry, project)

    common = dict(
        name=entry["name"],
        year=entry.get("year"),
        post_processing=entry.get("post_processing", []),
        project=project,
    )
    vtype = entry["type"]
    if vtype == "LocalRasterVar":
        return LocalRasterVar(
            path=entry["path"],
            raster_type=entry.get("raster_type"),
            data_type=entry["data_type"],
            **common,
        )
    if vtype == "GEEVar":
        return GEEVar(
            path=entry["path"],
            default_scale=entry.get("default_scale"),
            data_type=entry["data_type"],
            **common,
        )
    if vtype == "LocalVectorVar":
        return LocalVectorVar(
            path=entry["path"],
            rasterization_method=entry.get("rasterization_method"),
            data_type=entry["data_type"],
            **common,
        )
    raise ValueError(f"Unknown variable type: {vtype}")


def _build_predefined(entry: dict, project):
    """Build a GEEVar from a predefined catalogue entry."""
    from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE, resolve_aoi_ee
    from gui.store.state_manager import app_state

    key = entry["predefined_key"]
    cat = PREDEFINED_CATALOGUE[key]

    aoi_result = app_state.aoi_result.value
    if aoi_result is None:
        raise ValueError("No AOI selected — complete the AOI step first.")

    aoi_ee = resolve_aoi_ee(aoi_result)
    year = entry.get("year")
    image = cat["get_image"](aoi_ee, year)

    return GEEVar(
        name=entry["name"],
        data_type=entry["data_type"],
        raster_type=entry.get("raster_type"),
        gee_images=[image],
        aoi=aoi_ee,
        project=project,
        year=year,
    )


def _drop_from_map(key: str, map_):
    """Remove a variable's layer from the map and forget its on-map state."""
    if map_ is not None:
        map_.remove_layer(_map_layer_key(key), none_ok=True)
    if key in vars_on_map.value:
        remaining = set(vars_on_map.value)
        remaining.discard(key)
        vars_on_map.set(remaining)


@solara.component
def VariablesTile(project, processing, process_error, map_=None):
    """Variables step: add, inspect, and process variables.

    Args:
        project: Reactive holding the current Project (or None).
        processing: Reactive bool — True while batch processing is running.
        process_error: Reactive str | None — error from last Process All.
        map_: SepalMap instance used by the per-variable "show on map" toggle.
    """
    modal_open = solara.use_reactive(False)
    editing_key, set_editing_key = solara.use_state(None)
    pending_toggle = solara.use_reactive(None)

    @solara.lab.use_task(dependencies=None, raise_error=False)
    async def _apply_map_toggle():
        """Add or remove a GEE-backed variable's layer using the async map API.

        The async path is required: the sync ``add_ee_layer`` blocks on the GEE
        interface's private event loop and hangs when called from a Solara handler.
        """
        key = pending_toggle.value
        if key is None or map_ is None:
            return
        p = project.value
        var = p.raw_variables.get(key) if p is not None else None
        images = getattr(var, "gee_images", None) if var is not None else None
        if not images:
            return
        try:
            if key in vars_on_map.value:
                _drop_from_map(key, map_)
            else:
                image, vis = await _styled_layer(images[0], var, map_.gee_interface)
                await map_.add_ee_layer_async(
                    image, vis, name=key, key=_map_layer_key(key), use_map_vis=False
                )
                vars_on_map.set(set(vars_on_map.value) | {key})
        except Exception as exc:
            logger.exception("map toggle failed for %s", key)
            process_error.set(f"Could not toggle '{key}' on map: {exc}")

    def on_toggle_map(key: str):
        """Trigger the async toggle task for one source variable."""
        if map_ is None:
            return
        pending_toggle.set(key)
        _apply_map_toggle()

    def on_add(entry: dict):
        logger.debug("on_add called: %s", entry)
        p = project.value
        if p is None:
            logger.warning("on_add: project is None")
            process_error.set("No active project — complete the AOI step first.")
            return
        try:
            var = _build_variable(entry, p)
            key = f"{var.name}_{var.year}" if var.year else var.name
            p.raw_variables[key] = var
            logger.debug("Added var '%s', raw_variables now: %s", key, list(p.raw_variables.keys()))
            if entry.get("is_base") and hasattr(var, "data_type") and str(var.data_type) in ("raster", "DataType.raster"):
                p.base_raster = var
                logger.debug("Set '%s' as base raster", key)
            project.set(p.model_copy())
            logger.debug("project.set() called, project.value.raw_variables: %s", list(project.value.raw_variables.keys()))
        except Exception as exc:
            logger.exception("on_add failed")
            process_error.set(f"Could not add variable: {exc}")

    def on_edit_open(key: str):
        set_editing_key(key)
        modal_open.set(True)

    def on_save(old_key: str, new_entry: dict):
        p = project.value
        if p is None:
            return
        try:
            old_var = p.raw_variables.pop(old_key, None)
            if old_var and p.base_raster and p.base_raster.name == old_var.name:
                p.base_raster = None
            # The key may change on edit — drop the stale layer so it doesn't linger.
            _drop_from_map(old_key, map_)
            var = _build_variable(new_entry, p)
            new_key = f"{var.name}_{var.year}" if var.year else var.name
            p.raw_variables[new_key] = var
            if new_entry.get("is_base") and hasattr(var, "data_type") and str(var.data_type) in ("raster", "DataType.raster"):
                p.base_raster = var
            set_editing_key(None)
            project.set(p.model_copy())
        except Exception as exc:
            logger.exception("on_save failed")
            process_error.set(f"Could not save variable: {exc}")

    def on_remove(key: str):
        p = project.value
        if p is None:
            return
        removed = p.raw_variables.pop(key, None)
        if removed and p.base_raster and p.base_raster.name == removed.name:
            p.base_raster = None
        _drop_from_map(key, map_)
        project.set(p.model_copy())

    def on_remove_derived(key: str):
        """Remove a derived (processed) variable and delete its raster from disk."""
        p = project.value
        if p is None:
            return
        removed = p.processed_variables.pop(key, None)
        path = getattr(removed, "path", None) if removed is not None else None
        if path:
            try:
                fp = Path(path)
                if fp.is_file():
                    fp.unlink()
                    logger.debug("Deleted derived raster file: %s", fp)
            except OSError as exc:
                logger.exception("Could not delete file for derived var '%s'", key)
                process_error.set(f"Removed '{key}' but could not delete its file: {exc}")
        project.set(p.model_copy())

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def process_all():
        p = project.value
        if p is None or p.base_raster is None:
            return
        processing.set(True)
        process_error.set(None)
        try:
            await asyncio.to_thread(p.reproject_and_match_all, source="raw")
            await asyncio.to_thread(p.rasterize_all, source="raw")
            await asyncio.to_thread(p.save)
        except Exception as exc:
            process_error.set(str(exc))
        finally:
            processing.set(False)
        project.set(project.value)  # trigger re-render

    p = project.value
    has_vars = p is not None and bool(p.raw_variables)
    has_base = p is not None and p.base_raster is not None
    can_process = has_vars and has_base and not processing.value

    # Explain why "Process All" is disabled (first unmet condition wins).
    if processing.value:
        process_hint = None  # progress bar already conveys the running state
    elif not has_vars:
        process_hint = "Add at least one variable to enable Process All."
    elif not has_base:
        process_hint = (
            "No base raster set — tick “is base” on a raster variable "
            "(in Add Variable or Edit) to enable Process All."
        )
    else:
        process_hint = None

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 2 — Variables")
        solara.Text("Add input variables for the risk model. Designate one raster as the base for reprojection.")

        # Action bar
        with solara.Row(style="gap: 8px; align-items: center;"):
            solara.Button(
                "Add Variable",
                icon_name="mdi-plus",
                color="primary",
                small=True,
                on_click=lambda: modal_open.set(True),
            )
            solara.Button(
                "Process All",
                icon_name="mdi-cog-play-outline",
                color="primary",
                outlined=True,
                small=True,
                on_click=lambda: process_all(),
                disabled=not can_process,
            )
            if processing.value:
                solara.ProgressLinear(True)

        if process_hint:
            solara.Text(
                process_hint,
                style="font-size: 0.8rem; color: rgba(0,0,0,0.6); font-style: italic;",
            )

        # Source variable list
        solara.Markdown("**SOURCE VARIABLES**" + (f" ({len(p.raw_variables)})" if p else " (0)"))
        SourceVariableList(
            project=project,
            on_remove=on_remove,
            on_edit=on_edit_open,
            on_toggle_map=on_toggle_map if map_ is not None else None,
            vars_on_map=vars_on_map,
        )

        # Derived variable list
        if p and p.processed_variables:
            DerivedVariableList(project=project, on_remove=on_remove_derived)

    p = project.value
    editing_entry = (
        _variable_to_entry(editing_key, p.raw_variables[editing_key], p)
        if editing_key and p and editing_key in p.raw_variables
        else None
    )
    VariableModal(
        open_=modal_open,
        on_add=on_add,
        on_save=on_save,
        editing_key=editing_key,
        initial_entry=editing_entry,
    )

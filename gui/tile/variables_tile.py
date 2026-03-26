"""Step 2 — Variables tile."""

import asyncio
import logging

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


@solara.component
def VariablesTile(project, processing, process_error):
    """Variables step: add, inspect, and process variables.

    Args:
        project: Reactive holding the current Project (or None).
        processing: Reactive bool — True while batch processing is running.
        process_error: Reactive str | None — error from last Process All.
    """
    modal_open = solara.use_reactive(False)
    editing_key, set_editing_key = solara.use_state(None)

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

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 2 — Variables")
        solara.Text("Add input variables for the risk model. Designate one raster as the base for reprojection.")

        # Action bar
        with solara.Row(style="gap: 8px; align-items: center;"):
            solara.Button(
                "Add Variable",
                icon_name="mdi-plus",
                color="primary",
                on_click=lambda: modal_open.set(True),
                outlined=True,
            )
            solara.Button(
                "Process All",
                icon_name="mdi-cog-play-outline",
                color="secondary",
                on_click=lambda: process_all(),
                disabled=not can_process,
            )
            if processing.value:
                solara.ProgressLinear(True)

        # Source variable list
        solara.Markdown("**SOURCE VARIABLES**" + (f" ({len(p.raw_variables)})" if p else " (0)"))
        SourceVariableList(project=project, on_remove=on_remove, on_edit=on_edit_open)

        # Derived variable list
        if p and p.processed_variables:
            DerivedVariableList(project=project)

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

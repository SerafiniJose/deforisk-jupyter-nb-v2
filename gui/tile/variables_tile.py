"""Step 2 — Variables tile."""

import asyncio

import solara

from gui.widget.variable_list import DerivedVariableList, SourceVariableList
from gui.widget.variable_modal import VariableModal
from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.local_raster_var import LocalRasterVar
from spatialrisk.variables.local_vector_var import LocalVectorVar


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

    def on_add(entry: dict):
        p = project.value
        if p is None:
            return
        var = _build_variable(entry, p)
        key = f"{var.name}_{var.year}" if var.year else var.name
        p.raw_variables[key] = var
        if entry.get("is_base") and hasattr(var, "data_type") and str(var.data_type) in ("raster", "DataType.raster"):
            p.base_raster = var
        # Trigger reactive update
        project.set(p)

    def on_remove(key: str):
        p = project.value
        if p is None:
            return
        removed = p.raw_variables.pop(key, None)
        if removed and p.base_raster and p.base_raster.name == removed.name:
            p.base_raster = None
        project.set(p)

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

        if not has_base and has_vars:
            solara.Warning("Set one variable as the base raster before processing.")

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

        if process_error.value:
            solara.Error(process_error.value)

        # Source variable list
        solara.Markdown("**SOURCE VARIABLES**" + (f" ({len(p.raw_variables)})" if p else " (0)"))
        SourceVariableList(project=project, on_remove=on_remove)

        # Derived variable list
        if p and p.processed_variables:
            DerivedVariableList(project=project)

    VariableModal(open_=modal_open, on_add=on_add)

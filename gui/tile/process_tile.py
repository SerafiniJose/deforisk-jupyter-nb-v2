"""Step 3 — Process tile (mirrors notebooks/2.process_factory.ipynb)."""

import asyncio
import logging

import reacton.ipyvuetify as rv
import solara

from gui.scripts import process_actions
from gui.widget.variable_list import DerivedVariableList
from spatialrisk.variables.models import PostProcessing

logger = logging.getLogger("spatial_risk")


def _raw_raster_keys(p):
    """Keys of raw raster variables (candidates for the base)."""
    from spatialrisk.variables.models import DataType
    if p is None:
        return []
    return [
        k for k, v in p.raw_variables.items()
        if getattr(v, "data_type", None) != DataType.vector
    ]


def base_raster_key(p) -> str:
    """Raw-variable key backing the current base raster ('' if none/unmatched).

    The base raster is a reprojected copy that keeps the source variable's
    ``name``, while the Process-tile Select is keyed by raw-variable key. We map
    name -> key so the Select can be restored after a project is loaded (the
    base lives in the model, but the Select's state is transient ``use_state``).
    """
    if p is None or getattr(p, "base_raster", None) is None:
        return ""
    name = p.base_raster.name
    for k, v in p.raw_variables.items():
        if getattr(v, "name", None) == name:
            return k
    return ""


@solara.component
def ProcessTile(project, processing, process_error):
    """Download → base/projection → run processing → post-processing."""
    base_key, set_base_key = solara.use_state("")
    epsg, set_epsg = solara.use_state("")
    resolution, set_resolution = solara.use_state("30")
    pp_key, set_pp_key = solara.use_state("")
    pp_step, set_pp_step = solara.use_state(PostProcessing.dist.value)

    p = project.value
    has_vars = p is not None and bool(p.raw_variables)
    has_base = p is not None and p.base_raster is not None

    pending_geevars = (
        [k for k, v in p.raw_variables.items() if type(v).__name__ == "GEEVar"]
        if p else []
    )

    # Restore the form from a loaded project. The base raster is stored in the
    # model, but base_key / epsg / resolution are transient use_state that
    # default empty, so after a load the "Base raster" Select looked unset. Keyed
    # on the stored base's key, so it fires when a project is loaded (or the base
    # changes) but not on an in-progress dropdown selection. We restore the
    # stored CRS / resolution too — recomputing them (see autofill_base) could
    # diverge for a non-UTM base CRS.
    restored_key = base_raster_key(p)

    def _restore_base_form():
        if not restored_key:
            return
        set_base_key(restored_key)
        if p.base_raster.default_crs:
            set_epsg(str(p.base_raster.default_crs))
        if p.base_raster.default_resolution:
            set_resolution(str(round(p.base_raster.default_resolution)))

    solara.use_effect(_restore_base_form, [restored_key])

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def download_task():
        # Runs on a worker thread (prefer_threaded) so the UI stays responsive;
        # progress is driven by download_task.pending, not the shared processing flag.
        if p is None:
            return
        process_error.set(None)
        try:
            await asyncio.to_thread(process_actions.materialize_raw_layers, p)
            await asyncio.to_thread(p.save)
        except Exception as exc:
            process_error.set(str(exc))
        project.set(p.model_copy())

    @solara.lab.use_task(dependencies=[base_key], raise_error=False, prefer_threaded=True)
    async def autofill_base():
        """On base-raster selection, pre-fill EPSG (UTM) + resolution; stay editable."""
        if p is None or not base_key:
            return
        var = p.raw_variables.get(base_key)
        if var is None:
            return
        # The selection already backs the current base raster (e.g. restored
        # after a project load): keep its stored CRS / resolution rather than
        # recomputing them from the source file, which could differ (e.g. a
        # non-UTM base CRS).
        if p.base_raster is not None and getattr(var, "name", None) == p.base_raster.name:
            return
        res = await asyncio.to_thread(process_actions.base_raster_resolution, var)
        if res:
            set_resolution(str(round(res)))
        path = getattr(var, "path", None)
        if path is None:
            return  # not downloaded yet — auto-UTM needs the GeoTIFF on disk
        set_epsg(await asyncio.to_thread(process_actions.auto_utm_epsg, path))

    def on_auto_utm():
        if p is None or not base_key:
            return
        try:
            base = p.raw_variables[base_key]
            path = getattr(base, "path", None)
            if path is None:
                process_error.set("Download layers before auto-computing UTM.")
                return
            set_epsg(process_actions.auto_utm_epsg(path))
        except Exception as exc:
            process_error.set(f"Auto-UTM failed: {exc}")

    def on_set_base():
        if p is None:
            return
        try:
            res = float(resolution) if str(resolution).strip() else 30.0
            process_actions.set_base_raster(p, base_key, epsg.strip(), res)
            project.set(p.model_copy())
        except Exception as exc:
            process_error.set(f"Could not set base raster: {exc}")

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def process_task():
        if p is None:
            return
        processing.set(True)
        process_error.set(None)
        try:
            await asyncio.to_thread(process_actions.run_processing, p)
        except Exception as exc:
            process_error.set(str(exc))
        finally:
            processing.set(False)
        project.set(p.model_copy())

    def on_apply_pp():
        if p is None:
            return
        try:
            process_actions.apply_post_processing(p, pp_key, pp_step)
            project.set(p.model_copy())
        except Exception as exc:
            process_error.set(f"Post-processing failed: {exc}")

    with solara.Column(style="gap:16px;"):
        solara.Markdown("### Step 3 — Process")
        solara.Text(
            "Download layers, set the base raster + projection, then reproject, "
            "rasterize, and post-process all variables."
        )
        if not has_vars:
            solara.Info("Add variables in Step 2 first.")
            return

        # A — Download layers
        solara.Markdown("**1 · DOWNLOAD LAYERS**")
        solara.Text(f"{len(pending_geevars)} GEE layer(s) need downloading.")
        solara.Button(
            "Download layers", icon_name="mdi-cloud-download-outline",
            color="primary", outlined=True, small=True,
            on_click=lambda: download_task(),
            loading=download_task.pending,
            disabled=download_task.pending or not pending_geevars,
        )
        if download_task.pending:
            solara.ProgressLinear(True)

        # B — Base & projection
        solara.Markdown("**2 · BASE & PROJECTION**")
        with solara.Row(style="gap:8px;align-items:center;flex-wrap:wrap;"):
            rv.Select(
                label="Base raster", items=_raw_raster_keys(p),
                v_model=base_key, on_v_model=set_base_key, dense=True, outlined=True,
                style_="min-width:200px;flex:1 1 200px;",
            )
            rv.TextField(
                label="EPSG", v_model=epsg, on_v_model=set_epsg,
                dense=True, outlined=True, placeholder="EPSG:5490",
                style_="min-width:130px;max-width:170px;",
            )
            rv.TextField(
                label="Resolution (m)", v_model=resolution, on_v_model=set_resolution,
                dense=True, outlined=True, type="number",
                style_="min-width:130px;max-width:170px;",
            )
            solara.Button(
                "Auto (UTM)", small=True, text=True, on_click=on_auto_utm,
                disabled=not base_key or autofill_base.pending,
            )
            solara.Button(
                "Set base", icon_name="mdi-target", color="primary", small=True,
                on_click=on_set_base,
                disabled=autofill_base.pending or not (base_key and epsg.strip()),
            )
        if autofill_base.pending:
            solara.Text(
                "Detecting projection & resolution…",
                style="font-size:0.8rem;color:rgba(0,0,0,0.6);font-style:italic;",
            )
        if has_base:
            solara.Text(
                f"Base: {p.base_raster.name} · {p.base_raster.default_crs} · "
                f"{p.base_raster.default_resolution} m",
                style="font-size:0.8rem;color:rgba(0,0,0,0.6);",
            )

        # C — Run processing
        solara.Markdown("**3 · RUN PROCESSING**")
        if not has_base:
            solara.Text(
                "Set a base raster above to enable processing.",
                style="font-size:0.8rem;color:rgba(0,0,0,0.6);font-style:italic;",
            )
        solara.Button(
            "Run processing", icon_name="mdi-cog-play-outline",
            color="primary", small=True, on_click=lambda: process_task(),
            disabled=processing.value or download_task.pending or not has_base,
        )
        if processing.value:
            solara.ProgressLinear(True)

        # D — Post-processing
        if p and p.processed_variables:
            solara.Markdown("**4 · POST-PROCESSING (edge / dist)**")
            with solara.Row(style="gap:8px;align-items:center;"):
                rv.Select(
                    label="Processed variable",
                    items=list(p.processed_variables.keys()),
                    v_model=pp_key, on_v_model=set_pp_key, dense=True, outlined=True,
                )
                rv.Select(
                    label="Step", items=[s.value for s in PostProcessing],
                    v_model=pp_step, on_v_model=set_pp_step, dense=True, outlined=True,
                )
                solara.Button(
                    "Generate", icon_name="mdi-auto-fix", color="primary", small=True,
                    on_click=on_apply_pp, disabled=not pp_key,
                )
            DerivedVariableList(project=project)

"""Step 3 — Process tile (mirrors notebooks/2.process_factory.ipynb)."""

import asyncio
import logging

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts import process_actions
from gui.widget.help import InfoButton

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
    """Download → base/projection → run processing."""
    base_key, set_base_key = solara.use_state("")
    epsg, set_epsg = solara.use_state("")
    resolution, set_resolution = solara.use_state("30")

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
                process_error.set(t("tiles.process.error_download_first"))
                return
            set_epsg(process_actions.auto_utm_epsg(path))
        except Exception as exc:
            process_error.set(t("tiles.process.error_auto_utm", exc=exc))

    def on_set_base():
        if p is None:
            return
        try:
            res = float(resolution) if str(resolution).strip() else 30.0
            process_actions.set_base_raster(p, base_key, epsg.strip(), res)
            project.set(p.model_copy())
        except Exception as exc:
            process_error.set(t("tiles.process.error_set_base", exc=exc))

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

    with solara.Column(style="gap:16px;"):
        solara.Markdown(t("tiles.process.header"))
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.process.description"))
            InfoButton(t("tiles.process.info_header"), t("tiles.process.info_md"))
        if not has_vars:
            solara.Info(t("tiles.process.error_no_variables"))
            return

        # A — Download layers
        solara.Markdown(t("tiles.process.download_section_header"))
        solara.Text(t("tiles.process.pending_geevars_count", count=len(pending_geevars)))
        solara.Button(
            t("tiles.process.download_button"), icon_name="mdi-cloud-download-outline",
            color="primary", outlined=True, small=True,
            on_click=lambda: download_task(),
            loading=download_task.pending,
            disabled=download_task.pending or not pending_geevars,
        )
        if download_task.pending:
            solara.ProgressLinear(True)

        # B — Base & projection
        solara.Markdown(t("tiles.process.base_projection_header"))
        with solara.Row(style="gap:8px;align-items:center;flex-wrap:wrap;"):
            rv.Select(
                label=t("tiles.process.base_raster_label"), items=_raw_raster_keys(p),
                v_model=base_key, on_v_model=set_base_key, dense=True, outlined=True,
                style_="min-width:200px;flex:1 1 200px;",
                hint=t("tiles.process.base_raster_hint"), persistent_hint=True,
            )
            rv.TextField(
                label=t("tiles.process.epsg_label"), v_model=epsg, on_v_model=set_epsg,
                dense=True, outlined=True, placeholder=t("tiles.process.epsg_placeholder"),
                style_="min-width:130px;max-width:170px;",
                hint=t("tiles.process.epsg_hint"), persistent_hint=True,
            )
            rv.TextField(
                label=t("tiles.process.resolution_label"), v_model=resolution, on_v_model=set_resolution,
                dense=True, outlined=True, type="number",
                style_="min-width:130px;max-width:170px;",
                hint=t("tiles.process.resolution_hint"), persistent_hint=True,
            )
            solara.Button(
                t("tiles.process.auto_utm_button"), small=True, text=True, on_click=on_auto_utm,
                disabled=not base_key or autofill_base.pending,
            )
            solara.Button(
                t("tiles.process.set_base_button"), icon_name="mdi-target", color="primary", small=True,
                on_click=on_set_base,
                disabled=autofill_base.pending or not (base_key and epsg.strip()),
            )
        if autofill_base.pending:
            solara.Text(
                t("tiles.process.detecting_projection"),
                style="font-size:0.8rem;font-style:italic;",
                classes=["text--secondary"],
            )
        if has_base:
            solara.Text(
                t("tiles.process.base_info",
                  name=p.base_raster.name,
                  crs=p.base_raster.default_crs,
                  resolution=p.base_raster.default_resolution),
                style="font-size:0.8rem;",
                classes=["text--secondary"],
            )

        # C — Run processing
        solara.Markdown(t("tiles.process.run_processing_header"))
        solara.Text(
            t("tiles.process.run_processing_subtitle"),
            style="font-size:0.8rem;",
            classes=["text--secondary"],
        )
        if not has_base:
            solara.Text(
                t("tiles.process.error_no_base"),
                style="font-size:0.8rem;font-style:italic;",
                classes=["text--secondary"],
            )
        solara.Button(
            t("tiles.process.run_processing_button"), icon_name="mdi-cog-play-outline",
            color="primary", small=True, on_click=lambda: process_task(),
            disabled=processing.value or download_task.pending or not has_base,
        )
        if processing.value:
            solara.ProgressLinear(True)

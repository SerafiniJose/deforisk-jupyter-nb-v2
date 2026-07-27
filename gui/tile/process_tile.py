"""Step 3 — Harmonization tile (mirrors notebooks/2.process_factory.ipynb)."""

import asyncio
import logging

import reacton.ipyvuetify as rv
import solara
from pysepal.solara.notifications import use_notifications

from gui.i18n import t
from gui.scripts import process_actions
from gui.scripts.notify_bridge import tracked_job
from gui.scripts.solara_threads import publish_if_current, to_thread_in_context
from gui.store.project_writers import writing
from gui.tile.derived_map import derived_on_map, use_derived_map_toggle
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.help import InfoButton
from gui.widget.variable_list import DerivedVariableList

logger = logging.getLogger("spatial_risk")


def _raw_raster_keys(p):
    """Keys of raw raster variables (candidates for the base)."""
    from spatialrisk.variables.models import DataType

    if p is None:
        return []
    return [
        k
        for k, v in p.raw_variables.items()
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
def BaseProjectionForm(
    project,
    base_key,
    set_base_key,
    epsg,
    set_epsg,
    resolution,
    set_resolution,
    on_auto_utm,
    on_set_base,
    autofill_pending,
):
    """Base & projection form (Select, EPSG ⌖ + resolution, full-width Set base).

    A separate component because ``rv.use_event`` is a hook and must run
    unconditionally every render — ProcessTile early-returns before the form
    when there are no variables, so the hook cannot live in its body (and no
    early return may precede the hook here either; ``_raw_raster_keys``
    already handles a ``None`` project). Receives the ``project`` reactive
    (not ``.value``): value-equal ``model_copy`` snapshots would suppress
    child re-renders.
    """
    with solara.Column(style="gap:14px;"):
        rv.Select(
            label=t("tiles.process.base_raster_label"),
            items=_raw_raster_keys(project.value),
            v_model=base_key,
            on_v_model=set_base_key,
            dense=True,
            outlined=True,
            hint=t("tiles.process.base_raster_hint"),
        )
        with solara.Row(style="gap:8px;align-items:flex-start;flex-wrap:nowrap;"):
            epsg_field = rv.TextField(
                label=t("tiles.process.epsg_label"),
                v_model=epsg,
                on_v_model=set_epsg,
                dense=True,
                outlined=True,
                placeholder=t("tiles.process.epsg_placeholder"),
                style_="flex:1 1 55%;min-width:0;",
                hint=t("tiles.process.epsg_hint"),
                append_icon="mdi-crosshairs-gps",
            )
            rv.TextField(
                label=t("tiles.process.resolution_label"),
                v_model=resolution,
                on_v_model=set_resolution,
                dense=True,
                outlined=True,
                type="number",
                style_="flex:1 1 45%;min-width:0;",
                hint=t("tiles.process.resolution_hint"),
            )
        rv.use_event(epsg_field, "click:append", lambda *_: on_auto_utm())
        solara.Button(
            t("tiles.process.set_base_button"),
            icon_name="mdi-target",
            color="primary",
            small=True,
            block=True,
            on_click=on_set_base,
            disabled=autofill_pending or not (base_key and epsg.strip()),
        )


@solara.component
def ProcessTile(project, processing, process_error, map_=None):
    """Base/projection → run harmonization (downloading lives in Step 2 — Variables)."""
    base_key, set_base_key = solara.use_state("")
    epsg, set_epsg = solara.use_state("")
    resolution, set_resolution = solara.use_state("30")
    on_toggle_map = use_derived_map_toggle(project, map_, process_error)
    pending_remove, set_pending_remove = solara.use_state(None)
    notifications = use_notifications()

    def _do_remove(key: str):
        """Unregister a harmonized output (the raster stays on disk)."""
        p = project.value
        if process_actions.remove_processed_variable(p, key, map_):
            project.set(p.model_copy())

    p = project.value
    has_vars = p is not None and bool(p.raw_variables)
    has_base = p is not None and p.base_raster is not None

    pending_geevars = (
        [k for k, v in p.raw_variables.items() if type(v).__name__ == "GEEVar"]
        if p
        else []
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

    @solara.lab.use_task(
        dependencies=[base_key], raise_error=False, prefer_threaded=True
    )
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
        if (
            p.base_raster is not None
            and getattr(var, "name", None) == p.base_raster.name
        ):
            return
        res = await asyncio.to_thread(process_actions.base_raster_resolution, var)
        if res:
            set_resolution(str(round(res)))
        path = getattr(var, "path", None)
        if path is None:
            return  # not downloaded yet — auto-UTM needs the GeoTIFF on disk
        set_epsg(await asyncio.to_thread(process_actions.auto_utm_epsg, path))

    def on_auto_utm():
        # The ⌖ icon has no disabled state — gate here (was the old
        # button's ``disabled=not base_key or autofill_base.pending``).
        if p is None or not base_key or autofill_base.pending:
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
        title = t("notifications.task_processing")

        def _tracked_run():
            # tracked_job is entered on the pool thread itself so the library's
            # per-stage log lines (download/reproject/rasterize) land on THIS
            # job's tracker; to_thread_in_context gives that thread the kernel
            # context its bus updates need to reach the browser.
            with tracked_job(notifications, title):
                process_actions.run_processing(p)

        with writing(p.project_name):
            try:
                await to_thread_in_context(_tracked_run)
            except Exception as exc:
                process_error.set(str(exc))
            finally:
                processing.set(False)
            publish_if_current(project, p)

    def run_processing():
        """Run button: drop a re-click while a run is already in flight.

        ``TaskAsyncio.__call__`` sets ``pending`` synchronously on this thread, so
        this is a hard guard; the button's ``disabled`` only reaches the browser a
        round-trip later, so a real double-click does land twice. Re-invoking would
        cancel the in-flight task — which does NOT stop its asyncio.to_thread body —
        unwinding the `with writing(...)` block (dropping the writer mark) while the
        orphaned executor thread keeps writing rasters and calling project.save().
        Same pattern as ProjectPanel.confirm_delete.
        """
        if process_task.pending:
            return
        process_task()

    with solara.Column(style="gap:16px;"):
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.process.description"))
            InfoButton(t("tiles.process.info_header"), t("tiles.process.info_md"))
        if not has_vars:
            solara.Info(t("tiles.process.error_no_variables"))
            return

        # Downloading now lives in Step 2 — Variables; point back if layers are
        # still cloud-backed (auto-UTM needs the GeoTIFF on disk).
        if pending_geevars:
            solara.Info(
                t("tiles.process.pending_geevars_hint", count=len(pending_geevars))
            )

        # A — Base & projection
        solara.Markdown(t("tiles.process.base_projection_header"))
        BaseProjectionForm(
            project=project,
            base_key=base_key,
            set_base_key=set_base_key,
            epsg=epsg,
            set_epsg=set_epsg,
            resolution=resolution,
            set_resolution=set_resolution,
            on_auto_utm=on_auto_utm,
            on_set_base=on_set_base,
            autofill_pending=autofill_base.pending,
        )
        if autofill_base.pending:
            solara.Text(
                t("tiles.process.detecting_projection"),
                style="font-size:0.8rem;font-style:italic;",
                classes=["text--secondary"],
            )
        if has_base:
            solara.Text(
                t(
                    "tiles.process.base_info",
                    name=p.base_raster.name,
                    crs=p.base_raster.default_crs,
                    resolution=p.base_raster.default_resolution,
                ),
                style="font-size:0.8rem;",
                classes=["text--secondary"],
            )

        # B — Run processing
        solara.Markdown(t("tiles.process.run_processing_header"))
        if not has_base:
            solara.Text(
                t("tiles.process.error_no_base"),
                style="font-size:0.8rem;font-style:italic;",
                classes=["text--secondary"],
            )
        solara.Button(
            t("tiles.process.run_processing_button"),
            icon_name="mdi-play-circle-outline",
            color="primary",
            small=True,
            block=True,
            on_click=run_processing,
            # `disabled=` is a render-time prop: like `processing` (only set INSIDE
            # the coroutine), it reaches the browser a round-trip after the task
            # starts, so neither actually stops a fast double-click. This is
            # cosmetic only — it makes the button also *look* disabled during that
            # window. The real guard is run_processing()'s synchronous
            # process_task.pending check above, same gate as ProjectPanel's
            # confirm_delete. variables_tile/postprocess_tile still wire
            # on_click straight to their task (same gap, filed as a follow-up).
            disabled=processing.value or process_task.pending or not has_base,
        )
        if processing.value:
            solara.ProgressLinear(True)

        # Processing outputs — the aligned rasters this step wrote, each
        # toggleable on the map (post-process outputs are listed in Step 4).
        DerivedVariableList(
            project=project,
            keys=process_actions.processing_output_keys(p),
            on_toggle_map=on_toggle_map,
            derived_on_map=derived_on_map,
            on_remove=set_pending_remove,
            title=t("widgets.variable_list.processed_title"),
        )

    ConfirmDialog(
        open=pending_remove is not None,
        on_cancel=lambda: set_pending_remove(None),
        on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
        title=t("tiles.process.confirm_remove_title"),
        message=t("tiles.process.confirm_remove_message", name=pending_remove or ""),
        confirm_label=t("common.remove"),
    )

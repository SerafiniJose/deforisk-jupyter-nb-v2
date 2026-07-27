"""Step 4 — Derived layers tile (list-first; form lives in DerivedLayerDialog)."""

import logging

import solara
from pysepal.solara.notifications import use_notifications

from gui.i18n import t
from gui.scripts import process_actions
from gui.scripts.notify_bridge import tracked_job
from gui.scripts.solara_threads import publish_if_current, to_thread_in_context
from gui.store.project_writers import writing
from gui.tile.derived_map import derived_on_map, use_derived_map_toggle
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.derived_layer_dialog import CHANGE_OPS, DerivedLayerDialog
from gui.widget.help import InfoButton
from gui.widget.variable_list import DerivedVariableList

logger = logging.getLogger("spatial_risk")


@solara.component
def PostProcessTile(project, process_error, map_=None):
    """Derived layers: change detection (loss/gain) + edge/dist on harmonized vars."""
    dialog_open = solara.use_reactive(False)
    pending_change = solara.use_reactive(None)
    pending_post = solara.use_reactive(None)
    on_toggle_map = use_derived_map_toggle(project, map_, process_error)
    pending_remove, set_pending_remove = solara.use_state(None)
    notifications = use_notifications()

    p = project.value

    def _do_remove(key: str):
        """Unregister a derived layer (the raster stays on disk)."""
        if process_actions.remove_processed_variable(p, key, map_):
            project.set(p.model_copy())

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def change_task():
        entry = pending_change.value
        if p is None or entry is None:
            return
        process_error.set(None)
        title = t("notifications.task_change", op=entry["op"])

        def _tracked_change():
            with tracked_job(notifications, title):
                process_actions.generate_change_var(
                    p,
                    entry["op"],
                    entry["start_key"],
                    entry["end_key"],
                )

        with writing(p.project_name):
            try:
                await to_thread_in_context(_tracked_change)
            except Exception as exc:
                logger.exception("change detection failed")
                process_error.set(t("tiles.postprocess.error_change", exc=exc))
                return
            publish_if_current(project, p)

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def post_task():
        entry = pending_post.value
        if p is None or entry is None:
            return
        process_error.set(None)
        title = t(
            "notifications.task_postprocess",
            step=entry["op"],
            name=entry["pp_key"],
        )

        def _tracked_post():
            with tracked_job(notifications, title):
                process_actions.apply_post_processing(p, entry["pp_key"], entry["op"])

        with writing(p.project_name):
            try:
                await to_thread_in_context(_tracked_post)
            except Exception as exc:
                logger.exception("post-processing failed")
                process_error.set(t("tiles.postprocess.error_post_processing", exc=exc))
                return
            publish_if_current(project, p)

    def on_submit(entry):
        """Dialog-validated entry -> background change or edge/dist task.

        Neither op may run inline: solara executes widget callbacks inside the
        session's websocket message loop, so a GDAL proximity pass over a large
        AOI would freeze the whole UI until it returned.
        """
        if entry["op"] in CHANGE_OPS:
            pending_change.set(entry)
            change_task()
            return
        pending_post.set(entry)
        post_task()

    with solara.Column(style="gap:16px;"):
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.postprocess.description"))
            InfoButton(
                t("tiles.postprocess.info_header"), t("tiles.postprocess.info_md")
            )
        if p is None or not p.processed_variables:
            solara.Info(t("tiles.postprocess.error_no_processed"))
            return

        solara.Button(
            t("tiles.postprocess.new_button"),
            icon_name="mdi-plus",
            color="primary",
            small=True,
            block=True,
            on_click=lambda: dialog_open.set(True),
        )
        if change_task.pending or post_task.pending:
            solara.ProgressLinear(True)

        DerivedVariableList(
            project=project,
            keys=process_actions.postprocess_output_keys(p),
            on_toggle_map=on_toggle_map,
            derived_on_map=derived_on_map,
            on_remove=set_pending_remove,
        )

    DerivedLayerDialog(project=project, open_=dialog_open, on_submit=on_submit)
    ConfirmDialog(
        open=pending_remove is not None,
        on_cancel=lambda: set_pending_remove(None),
        on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
        title=t("tiles.postprocess.confirm_remove_title"),
        message=t(
            "tiles.postprocess.confirm_remove_message", name=pending_remove or ""
        ),
        confirm_label=t("common.remove"),
    )

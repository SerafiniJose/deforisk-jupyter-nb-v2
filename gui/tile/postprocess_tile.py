"""Step 4 — Derived layers tile (list-first; form lives in DerivedLayerDialog)."""

import asyncio
import logging

import solara

from gui.i18n import t
from gui.scripts import process_actions
from gui.scripts.solara_threads import publish_if_current
from gui.store.project_writers import writing
from gui.tile.derived_map import derived_on_map, use_derived_map_toggle
from gui.widget.derived_layer_dialog import CHANGE_OPS, DerivedLayerDialog
from gui.widget.help import InfoButton
from gui.widget.variable_list import DerivedVariableList

logger = logging.getLogger("spatial_risk")


@solara.component
def PostProcessTile(project, process_error, map_=None):
    """Derived layers: change detection (loss/gain) + edge/dist on harmonized variables."""
    dialog_open = solara.use_reactive(False)
    pending_change = solara.use_reactive(None)
    on_toggle_map = use_derived_map_toggle(project, map_, process_error)

    p = project.value

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def change_task():
        entry = pending_change.value
        if p is None or entry is None:
            return
        process_error.set(None)
        with writing(p.project_name):
            try:
                await asyncio.to_thread(
                    process_actions.generate_change_var,
                    p, entry["op"], entry["start_key"], entry["end_key"],
                )
            except Exception as exc:
                logger.exception("change detection failed")
                process_error.set(t("tiles.postprocess.error_change", exc=exc))
                return
            publish_if_current(project, p)

    def on_submit(entry):
        """Dialog-validated entry -> background change task or sync edge/dist."""
        if entry["op"] in CHANGE_OPS:
            pending_change.set(entry)
            change_task()
            return
        try:
            process_actions.apply_post_processing(p, entry["pp_key"], entry["op"])
            project.set(p.model_copy())
        except Exception as exc:
            process_error.set(t("tiles.postprocess.error_post_processing", exc=exc))

    with solara.Column(style="gap:16px;"):
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.postprocess.description"))
            InfoButton(t("tiles.postprocess.info_header"), t("tiles.postprocess.info_md"))
        if p is None or not p.processed_variables:
            solara.Info(t("tiles.postprocess.error_no_processed"))
            return

        solara.Button(
            t("tiles.postprocess.new_button"),
            icon_name="mdi-plus",
            color="primary",
            small=True,
            on_click=lambda: dialog_open.set(True),
        )
        if change_task.pending:
            solara.ProgressLinear(True)

        DerivedVariableList(
            project=project,
            keys=process_actions.postprocess_output_keys(p),
            on_toggle_map=on_toggle_map,
            derived_on_map=derived_on_map,
        )

    DerivedLayerDialog(project=project, open_=dialog_open, on_submit=on_submit)

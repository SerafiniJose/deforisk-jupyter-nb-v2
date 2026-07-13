"""Step 4 — Post-process tile: change detection (loss/gain) + edge/dist.

Operates on processed (aligned) variables only, so any two temporal masks can
be paired — same source or cross-source.
"""

import asyncio
import logging

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts import process_actions
from gui.tile.derived_map import derived_on_map, use_derived_map_toggle
from gui.widget.help import InfoButton
from gui.widget.variable_list import DerivedVariableList
from spatialrisk.variables.models import PostProcessing

logger = logging.getLogger("spatial_risk")

CHANGE_OPS = ["loss", "gain"]
OPERATIONS = CHANGE_OPS + [s.value for s in PostProcessing]


@solara.component
def PostProcessTile(project, process_error, map_=None):
    """Change detection (loss/gain) + edge/dist on processed variables."""
    op, set_op = solara.use_state("loss")
    start_key, set_start_key = solara.use_state("")
    end_key, set_end_key = solara.use_state("")
    pp_key, set_pp_key = solara.use_state("")
    on_toggle_map = use_derived_map_toggle(project, map_, process_error)

    p = project.value

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def change_task():
        if p is None:
            return
        process_error.set(None)
        try:
            await asyncio.to_thread(
                process_actions.generate_change_var, p, op, start_key, end_key
            )
        except Exception as exc:
            logger.exception("change detection failed")
            process_error.set(t("tiles.postprocess.error_change", exc=exc))
            return
        project.set(p.model_copy())

    def on_apply_pp():
        if p is None:
            return
        try:
            process_actions.apply_post_processing(p, pp_key, op)
            project.set(p.model_copy())
        except Exception as exc:
            process_error.set(t("tiles.postprocess.error_post_processing", exc=exc))

    with solara.Column(style="gap:16px;"):
        solara.Markdown(t("tiles.postprocess.header"))
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.postprocess.description"))
            InfoButton(t("tiles.postprocess.info_header"), t("tiles.postprocess.info_md"))
        if p is None or not p.processed_variables:
            solara.Info(t("tiles.postprocess.error_no_processed"))
            return

        # One operation picker; only the selected operation's options render.
        rv.Select(
            label=t("tiles.postprocess.operation_label"),
            items=[
                {"text": t(f"tiles.postprocess.op_label_{o}"), "value": o}
                for o in OPERATIONS
            ],
            v_model=op, on_v_model=set_op, dense=True, outlined=True,
            style_="max-width:340px;",
            # Dynamic per-operation help (loss/gain/edge/dist semantics).
            hint=t(f"tiles.postprocess.op_hint_{op}"), persistent_hint=True,
        )

        if op in CHANGE_OPS:
            candidates = process_actions.change_layer_candidates(p)
            if candidates:
                with solara.Row(style="gap:8px;align-items:center;flex-wrap:wrap;"):
                    rv.Select(
                        label=t("tiles.postprocess.start_layer_label"), items=candidates,
                        v_model=start_key, on_v_model=set_start_key,
                        dense=True, outlined=True,
                    )
                    rv.Select(
                        label=t("tiles.postprocess.end_layer_label"), items=candidates,
                        v_model=end_key, on_v_model=set_end_key,
                        dense=True, outlined=True,
                    )
                    solara.Button(
                        t("tiles.postprocess.generate_button"),
                        icon_name="mdi-vector-difference", color="primary", small=True,
                        on_click=lambda: change_task(),
                        loading=change_task.pending,
                        disabled=change_task.pending
                        or not (start_key and end_key and start_key != end_key),
                    )
            solara.Text(
                t("tiles.postprocess.change_help_text"),
                style="font-size:0.8rem;font-style:italic;",
                classes=["text--secondary"],
            )
            if change_task.pending:
                solara.ProgressLinear(True)
        else:
            with solara.Row(style="gap:8px;align-items:center;"):
                rv.Select(
                    label=t("tiles.postprocess.processed_variable_label"),
                    items=list(p.processed_variables.keys()),
                    v_model=pp_key, on_v_model=set_pp_key, dense=True, outlined=True,
                    hint=t("tiles.postprocess.processed_variable_hint"),
                    persistent_hint=True,
                )
                solara.Button(
                    t("tiles.postprocess.generate_button"), icon_name="mdi-auto-fix",
                    color="primary", small=True,
                    on_click=on_apply_pp, disabled=not pp_key,
                )

        DerivedVariableList(
            project=project,
            keys=process_actions.postprocess_output_keys(p),
            on_toggle_map=on_toggle_map,
            derived_on_map=derived_on_map,
        )

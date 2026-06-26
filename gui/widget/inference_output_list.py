"""Inference outputs list widget for the Inference tab."""

import logging

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t

logger = logging.getLogger("spatial_risk")

STATUS_COLORS = {
    "running": "blue",
    "completed": "green",
    "failed": "red",
    "cancelled": "grey",
}

STATUS_ICONS = {
    "running": "mdi-loading mdi-spin",
    "completed": "mdi-check-circle",
    "failed": "mdi-alert-circle",
    "cancelled": "mdi-cancel",
}


@solara.component
def InferenceOutputItem(job: dict, on_remove, on_toggle_map=None, is_on=False, has_preds=False):
    """Single inference output row.

    A completed job whose prediction raster(s) are registered on the project
    (``has_preds``) gets a map-toggle button mirroring the Variables tab.
    """
    status = job["status"]
    color = STATUS_COLORS.get(status, "grey")
    icon = STATUS_ICONS.get(status, "mdi-help-circle")
    model_key = job.get("model_key", "—")
    dataset_name = job.get("dataset_name", "—")
    pred_name = job.get("pred_name")

    # Lead with the user-chosen prediction name when present; fall back to the
    # "model on dataset" form for imported/legacy jobs that have no pred_name.
    title = (
        f"{pred_name} ({model_key} on {dataset_name})"
        if pred_name
        else f"{model_key} on {dataset_name}"
    )

    with rv.ListItem(dense=True):
        with rv.ListItemIcon():
            rv.Icon(children=[icon], color=color, small=True)
        with rv.ListItemContent():
            rv.ListItemTitle(
                children=[title],
                style_="font-size: 0.875rem;",
            )
            if status == "completed":
                output_path = job.get("output_path", "—")
                rv.ListItemSubtitle(children=[t("widgets.inference_output_list.output_label", path=output_path)])
            elif status == "failed":
                error = job.get("error") or t("widgets.inference_output_list.unknown_error")
                rv.ListItemSubtitle(
                    children=[t("widgets.inference_output_list.error_label", error=error)],
                    style_="color: red;",
                )
            elif status == "cancelled":
                rv.ListItemSubtitle(children=[t("widgets.inference_output_list.status_cancelled")])

        with rv.ListItemAction():
            with solara.Row(style="gap:0;align-items:center;flex-direction:row;"):
                # Map toggle — only for completed jobs with a registered prediction.
                if status == "completed" and has_preds and on_toggle_map is not None:
                    solara.Button(
                        "",
                        icon_name="mdi-map-minus" if is_on else "mdi-map-plus",
                        on_click=lambda *_: on_toggle_map(job),
                        icon=True,
                        text=True,
                        x_small=True,
                        color="primary" if is_on else "grey darken-1",
                    )
                if status != "running":
                    rv.Btn(
                        children=[rv.Icon(children=["mdi-close"], small=True)],
                        icon=True,
                        x_small=True,
                        on_click=lambda *_: on_remove(job["id"]),
                    )


@solara.component
def InferenceOutputList(
    inference_jobs,
    on_remove,
    on_toggle_map=None,
    preds_on_map=None,
    predictions_for=None,
):
    """List of inference outputs with status and actions.

    Args:
        inference_jobs: solara.Reactive[list] — list of inference job dicts.
        on_remove: callback(job_id) — remove a finished/failed/cancelled job.
        on_toggle_map: callback(job) — add/remove the job's prediction on the map.
        preds_on_map: solara.Reactive[set] — job ids currently shown on the map.
        predictions_for: callable(job) -> dict — registered predictions for a job
            (empty when none); drives whether the map-toggle button is shown.
    """
    jobs = inference_jobs.value

    if not jobs:
        return

    on_map = preds_on_map.value if preds_on_map is not None else set()

    solara.Markdown(t("widgets.inference_output_list.outputs_header", count=len(jobs)))
    with rv.List(dense=True):
        for job in reversed(jobs):
            has_preds = bool(predictions_for(job)) if predictions_for is not None else False
            InferenceOutputItem(
                job=job,
                on_remove=on_remove,
                on_toggle_map=on_toggle_map,
                is_on=job["id"] in on_map,
                has_preds=has_preds,
            )

"""Inference outputs list widget for the Inference tab."""

import logging

import reacton.ipyvuetify as rv
import solara

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
def InferenceOutputItem(job: dict, on_remove):
    """Single inference output row."""
    status = job["status"]
    color = STATUS_COLORS.get(status, "grey")
    icon = STATUS_ICONS.get(status, "mdi-help-circle")
    model_key = job.get("model_key", "—")
    dataset_name = job.get("dataset_name", "—")

    with rv.ListItem(dense=True):
        with rv.ListItemIcon():
            rv.Icon(children=[icon], color=color, small=True)
        with rv.ListItemContent():
            rv.ListItemTitle(
                children=[f"{model_key} on {dataset_name}"],
                style_="font-size: 0.875rem;",
            )
            if status == "completed":
                output_path = job.get("output_path", "—")
                rv.ListItemSubtitle(children=[f"Output: {output_path}"])
            elif status == "failed":
                error = job.get("error", "Unknown error")
                rv.ListItemSubtitle(
                    children=[f"Error: {error}"],
                    style_="color: red;",
                )
            elif status == "cancelled":
                rv.ListItemSubtitle(children=["Cancelled by user"])

        with rv.ListItemAction():
            if status != "running":
                rv.Btn(
                    children=[rv.Icon(children=["mdi-close"], small=True)],
                    icon=True,
                    x_small=True,
                    on_click=lambda *_: on_remove(job["id"]),
                )


@solara.component
def InferenceOutputList(inference_jobs, on_remove):
    """List of inference outputs with status and actions.

    Args:
        inference_jobs: solara.Reactive[list] — list of inference job dicts.
        on_remove: callback(job_id) — remove a finished/failed/cancelled job.
    """
    jobs = inference_jobs.value

    if not jobs:
        return

    solara.Markdown(f"**OUTPUTS** ({len(jobs)})")
    with rv.List(dense=True):
        for job in reversed(jobs):
            InferenceOutputItem(job=job, on_remove=on_remove)

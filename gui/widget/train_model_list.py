"""Trained models list widget for the Train tab."""

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
def TrainJobItem(job: dict, on_cancel, on_remove):
    """Single training job row."""
    status = job["status"]
    color = STATUS_COLORS.get(status, "grey")
    icon = STATUS_ICONS.get(status, "mdi-help-circle")
    model_label = job.get("model_label", job["model_type"])
    dataset_name = job.get("dataset_name", "—")

    with rv.ListItem(dense=True):
        with rv.ListItemIcon():
            rv.Icon(children=[icon], color=color, small=True)
        with rv.ListItemContent():
            rv.ListItemTitle(
                children=[f"{model_label} — {dataset_name}"],
                style_="font-size: 0.875rem;",
            )
            if status == "completed":
                deviance = job.get("deviance")
                n_samples = job.get("n_samples")
                dev_str = f"{deviance:,.2f}" if deviance is not None else "—"
                samp_str = f"{n_samples:,}" if n_samples is not None else "—"
                rv.ListItemSubtitle(
                    children=[f"deviance: {dev_str} | samples: {samp_str}"],
                )
            elif status == "failed":
                error = job.get("error", "Unknown error")
                rv.ListItemSubtitle(
                    children=[f"Error: {error}"],
                    style_="color: red;",
                )
            elif status == "cancelled":
                rv.ListItemSubtitle(children=["Cancelled by user"])

        with rv.ListItemAction():
            if status == "running":
                rv.Btn(
                    children=[rv.Icon(children=["mdi-stop-circle"], small=True)],
                    icon=True,
                    x_small=True,
                    on_click=lambda *_: on_cancel(job["id"]),
                )
            else:
                rv.Btn(
                    children=[rv.Icon(children=["mdi-close"], small=True)],
                    icon=True,
                    x_small=True,
                    on_click=lambda *_: on_remove(job["id"]),
                )


@solara.component
def TrainModelList(train_jobs, on_cancel, on_remove):
    """List of all training jobs with status, metrics, and actions.

    Args:
        train_jobs: solara.Reactive[list] — list of job dicts.
        on_cancel: callback(job_id) — cancel a running job.
        on_remove: callback(job_id) — remove a finished/failed/cancelled job.
    """
    jobs = train_jobs.value

    if not jobs:
        return

    solara.Markdown(f"**TRAINED MODELS** ({len(jobs)})")
    with rv.List(dense=True):
        for job in reversed(jobs):
            TrainJobItem(job=job, on_cancel=on_cancel, on_remove=on_remove)

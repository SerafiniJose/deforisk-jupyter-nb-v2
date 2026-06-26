# gui/widget/evaluation_results.py
"""Step 8 evaluation output: job status, saved-runs list, and table popup.

Saved evaluations are persisted on the project (project.evaluations). The list
rows only set a pending key; a single top-level EvaluationTableDialog renders the
selected run's table — the proven ConfirmDialog pattern (nested row->Dialog
toggles are unreliable).
"""

import pandas as pd
import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.tile.evaluation_helpers import rows_for_record


@solara.component
def EvaluationResults(eval_jobs, project, on_open, on_delete):
    """Render running/failed job alerts and the list of saved evaluation runs.

    Args:
        eval_jobs: solara.Reactive[list] — transient per-run job dicts.
        project: solara.Reactive[Project] — source of project.evaluations.
        on_open: callback(key) — open the table popup for a saved run.
        on_delete: callback(key) — delete a saved run.
    """
    p = project.value
    with solara.Column(style="gap: 12px;"):
        for job in eval_jobs.value:
            status = job["status"]
            color = {"running": "info", "completed": "success",
                     "failed": "error"}.get(status, "grey")
            with rv.Alert(type_=color, dense=True, outlined=True):
                solara.Text(t("widgets.evaluation_results.job_status", id=job['id'], status=status))
                if job.get("error"):
                    solara.Text(job["error"])

        records = list(p.evaluations.items()) if p is not None else []
        if not records:
            return

        records.sort(key=lambda kv: kv[1].created_at, reverse=True)  # newest first
        solara.Markdown(t("widgets.evaluation_results.saved_evaluations_header", count=len(records)))
        with rv.List(dense=True):
            for key, rec in records:
                EvaluationRow(rec_key=key, record=rec,
                              on_open=on_open, on_delete=on_delete)


@solara.component
def EvaluationRow(rec_key, record, on_open, on_delete):
    """One saved-evaluation row: a 'view table' button opens the popup; × deletes.

    Both actions are explicit icon Buttons in ListItemAction — the codebase's
    proven click pattern (matches variable_list / inference_output_list). NOTE:
    ``on_click`` on ``rv.ListItem``/``rv.ListItemContent`` does NOT reliably fire
    in this reacton.ipyvuetify setup, so the row container is intentionally not
    clickable; opening goes through the dedicated Button instead.
    """
    n_maps = len(record.prediction_keys)
    with rv.ListItem(dense=True):
        with rv.ListItemContent():
            rv.ListItemTitle(
                children=[t("widgets.evaluation_results.row_title", truth_tag=record.truth_tag, n_maps=n_maps)],
                style_="font-size: 0.875rem;",
            )
            rv.ListItemSubtitle(children=[record.created_at])
        with rv.ListItemAction():
            with solara.Row(style="gap:0;align-items:center;flex-direction:row;"):
                solara.Button(
                    "",
                    icon_name="mdi-table-eye",
                    on_click=lambda *_: on_open(rec_key),
                    icon=True, text=True, x_small=True, color="primary",
                )
                rv.Btn(
                    children=[rv.Icon(children=["mdi-close"], small=True)],
                    icon=True, x_small=True,
                    on_click=lambda *_: on_delete(rec_key),
                )


@solara.component
def EvaluationTableDialog(project, eval_key, on_close):
    """Single popup showing the selected saved run's indices table.

    The dialog is sized to fit the table without scrolling: only the metric
    columns the run selected are shown (``displayed_indices``), the dialog is
    wide, and pagination is raised high enough that every row fits on one page.
    """
    p = project.value
    record = p.evaluations.get(eval_key) if (p is not None and eval_key) else None
    with rv.Dialog(
        v_model=eval_key is not None,
        on_v_model=lambda v: None if v else on_close(),
        max_width="1400px",
        eager=True,
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(
                    t("widgets.evaluation_results.dialog_title", truth_tag=record.truth_tag) if record else t("widgets.evaluation_results.dialog_title_fallback"))
            with rv.CardText():
                if record is not None and record.indices:
                    rows = rows_for_record(record)
                    solara.DataFrame(pd.DataFrame(rows), items_per_page=max(len(rows), 1))
                elif record is not None:
                    solara.Info(t("widgets.evaluation_results.no_indices_info"))
            with rv.CardActions(style_="justify-content: flex-end;"):
                solara.Button(t("common.close"), on_click=on_close, text=True, small=True)

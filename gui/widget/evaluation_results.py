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
                solara.Text(f"{job['id']}: {status}")
                if job.get("error"):
                    solara.Text(job["error"])

        records = list(p.evaluations.items()) if p is not None else []
        if not records:
            return

        records.sort(key=lambda kv: kv[1].created_at, reverse=True)  # newest first
        solara.Markdown(f"**SAVED EVALUATIONS** ({len(records)})")
        with rv.List(dense=True):
            for key, rec in records:
                EvaluationRow(rec_key=key, record=rec,
                              on_open=on_open, on_delete=on_delete)


@solara.component
def EvaluationRow(rec_key, record, on_open, on_delete):
    """One saved-evaluation row: clickable title + delete button."""
    n_maps = len(record.prediction_keys)
    with rv.ListItem(dense=True, ripple=True,
                     on_click=lambda *_: on_open(rec_key)):
        with rv.ListItemContent():
            rv.ListItemTitle(
                children=[f"{record.truth_tag} · {n_maps} maps"],
                style_="font-size: 0.875rem;",
            )
            rv.ListItemSubtitle(children=[record.created_at])
        with rv.ListItemAction():
            rv.Btn(
                children=[rv.Icon(children=["mdi-close"], small=True)],
                icon=True, x_small=True,
                on_click=lambda *_: on_delete(rec_key),
            )


@solara.component
def EvaluationTableDialog(project, eval_key, on_close):
    """Single popup showing the selected saved run's indices table."""
    p = project.value
    record = p.evaluations.get(eval_key) if (p is not None and eval_key) else None
    with rv.Dialog(
        v_model=eval_key is not None,
        on_v_model=lambda v: None if v else on_close(),
        max_width="900px",
        eager=True,
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(
                    f"Evaluation — {record.truth_tag}" if record else "Evaluation")
            with rv.CardText():
                if record is not None and record.indices:
                    solara.DataFrame(pd.DataFrame(record.indices))
                elif record is not None:
                    solara.Info("No indices stored for this run.")
            with rv.CardActions(style_="justify-content: flex-end;"):
                solara.Button("Close", on_click=on_close, text=True, small=True)

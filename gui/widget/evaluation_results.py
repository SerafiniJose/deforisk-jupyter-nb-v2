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
from gui.scripts.product_rows import evaluation_tab_rows
from gui.tile.evaluation_helpers import rows_for_record
from gui.widget.product_table import ProductTable


@solara.component
def EvaluationResults(eval_jobs, project, on_open, on_delete, on_dismiss=None):
    """Evaluations table: saved runs (registry) plus in-flight/failed jobs.

    Args:
        eval_jobs: solara.Reactive[list] — transient per-run job dicts.
        project: solara.Reactive[Project] — source of project.evaluations.
        on_open: callback(key) — open the table popup for a saved run.
        on_delete: callback(key) — delete a saved run.
        on_dismiss: callback(job_id) — discard a failed job row.
    """
    p = project.value
    data = evaluation_tab_rows(p, eval_jobs.value)

    rows = []
    for r in data:
        if r["kind"] == "evaluation":
            actions = [
                {"kind": "open", "on_click": lambda *_, k=r["key"]: on_open(k)},
                {"kind": "delete", "on_click": lambda *_, k=r["key"]: on_delete(k)},
            ]
        elif r["status"] != "running" and on_dismiss is not None:
            actions = [{"kind": "dismiss", "on_click": lambda *_, i=r["job_id"]: on_dismiss(i)}]
        else:
            actions = []

        error = r.get("error")
        if r["status"] == "failed" and not error:
            error = t("widgets.evaluation_results.unknown_error")
        rows.append(
            {
                "key": r["key"],
                "cells": [
                    {"type": "text", "value": r["truth_tag"]},
                    {"type": "chip", "value": str(r["n_maps"])},
                    {"type": "text", "value": r["created_at"], "size": "0.78rem", "muted": True},
                    {"type": "status", "status": r["status"]},
                ],
                "actions": actions,
                "error": error,
            }
        )

    ProductTable(
        title=t("widgets.evaluation_results.evaluations_title"),
        columns=[
            {"label": t("widgets.evaluation_results.col_truth"), "width": "minmax(0,1fr)"},
            {"label": t("widgets.evaluation_results.col_maps"), "width": "55px"},
            {"label": t("widgets.evaluation_results.col_created"), "width": "minmax(0,1fr)"},
            {"label": t("widgets.evaluation_results.col_status"), "width": "95px"},
        ],
        rows=rows,
        empty_text=t("widgets.evaluation_results.empty"),
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

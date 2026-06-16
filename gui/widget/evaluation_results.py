# gui/widget/evaluation_results.py
"""Renders evaluation output: per-job status + the aggregated indices table."""

import reacton.ipyvuetify as rv
import solara


@solara.component
def EvaluationResults(eval_jobs, indices_df):
    with solara.Column(style="gap: 12px;"):
        for job in eval_jobs.value:
            status = job["status"]
            color = {"running": "info", "completed": "success",
                     "failed": "error"}.get(status, "grey")
            with rv.Alert(type_=color, dense=True, outlined=True):
                solara.Text(f"{job['id']}: {status}")
                if job.get("error"):
                    solara.Text(job["error"])

        if indices_df.value is not None and len(indices_df.value):
            solara.Markdown("#### Accuracy indices (ha; lower is better, R² higher is better)")
            solara.DataFrame(indices_df.value)
        elif indices_df.value is not None:
            solara.Info("No predictions matched the current filters.")

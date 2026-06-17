# gui/tile/evaluation_tile.py
"""Step 6 — Evaluation tile."""

import logging
import threading
import uuid

import reacton.ipyvuetify as rv
import solara

from gui.widget.evaluation_results import EvaluationResults

logger = logging.getLogger("spatial_risk")

# Module-level reactives shared across re-renders
eval_jobs = solara.reactive([])
eval_indices = solara.reactive(None)


def _run_evaluation(job_id, project, dataset_filter, recompute):
    try:
        from spatialrisk.evaluation import evaluate_predictions

        df = evaluate_predictions(
            project,
            dataset_filter=dataset_filter or None,
            csizes=(300,),
            recompute_defrate=recompute,
        )
        eval_indices.set(df)
        jobs = list(eval_jobs.value)
        for j in jobs:
            if j["id"] == job_id and j["status"] != "cancelled":
                j["status"] = "completed"
        eval_jobs.set(jobs)
        logger.info("Evaluation completed (%d rows)", len(df))
    except Exception as exc:
        logger.exception("Evaluation failed")
        jobs = list(eval_jobs.value)
        for j in jobs:
            if j["id"] == job_id and j["status"] != "cancelled":
                j["status"] = "failed"
                j["error"] = str(exc)
        eval_jobs.set(jobs)


@solara.component
def EvaluationTile(project):
    p = project.value

    period_keys = sorted({pred.dataset_name for pred in p.predictions.values()}) if (
        p and p.predictions) else []
    selected_periods, set_selected_periods = solara.use_state([])
    recompute, set_recompute = solara.use_state(True)

    n_predictions = len(p.predictions) if (p and p.predictions) else 0

    def on_run():
        if p is None or n_predictions == 0:
            return
        job_id = str(uuid.uuid4())[:8]
        eval_jobs.set(list(eval_jobs.value) + [
            {"id": job_id, "status": "running", "error": None}])
        threading.Thread(
            target=_run_evaluation,
            args=(job_id, p, list(selected_periods), recompute),
            daemon=True,
        ).start()

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 7 — Evaluation")
        solara.Text(
            "Validate registered predictions against observed deforestation "
            "(udef-arp indices: MedAE / R² / RMSE / wRMSE).")

        rv.Select(
            label="Periods to evaluate (empty = all)",
            items=period_keys,
            v_model=selected_periods,
            on_v_model=set_selected_periods,
            multiple=True, chips=True, dense=True, outlined=True,
            no_data_text="No predictions registered. Run inference in Step 5.",
        )
        rv.Switch(label="Recompute defrate", v_model=recompute, on_v_model=set_recompute)

        solara.Button(
            "Run evaluation", icon_name="mdi-chart-bar", color="primary", small=True,
            on_click=on_run, disabled=n_predictions == 0,
        )
        if n_predictions == 0:
            solara.Info("No predictions available yet — run inference first.")

        EvaluationResults(eval_jobs=eval_jobs, indices_df=eval_indices)

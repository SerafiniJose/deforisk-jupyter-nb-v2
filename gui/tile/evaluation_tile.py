# gui/tile/evaluation_tile.py
"""Step 7 — Evaluation tile.

Score user-selected maps against ONE explicitly-chosen truth (deforestation
raster + forest-at-start variable + interval), so maps from different datasets
are directly comparable. See evaluate_against_truth in spatialrisk/evaluation.py.
"""

import logging
import uuid

import reacton.ipyvuetify as rv
import solara

from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.tile.evaluation_helpers import (
    build_truth_spec, default_forest_key, map_items, parse_interval,
    variable_items)
from gui.widget.evaluation_results import EvaluationResults

logger = logging.getLogger("spatial_risk")

# Module-level reactives shared across re-renders
eval_jobs = solara.reactive([])
eval_indices = solara.reactive(None)


def _run_evaluation(job_id, project, prediction_keys, spec, recompute):
    try:
        from spatialrisk.evaluation import evaluate_against_truth

        df = evaluate_against_truth(
            project,
            prediction_keys=prediction_keys or None,
            defor_file=spec["defor_file"],
            forest_file=spec["forest_file"],
            time_interval=spec["time_interval"],
            truth_tag=spec["truth_tag"],
            csizes=(300,),
            recompute_defrate=recompute,
        )
        eval_indices.set(df)
        update_job(eval_jobs, job_id, status="completed")
        logger.info("Evaluation completed (%d rows)", len(df))
    except Exception as exc:
        logger.exception("Evaluation failed")
        update_job(eval_jobs, job_id, status="failed", error=str(exc))


@solara.component
def EvaluationTile(project):
    p = project.value

    var_items = variable_items(p)
    pred_items = map_items(p)
    n_predictions = len(pred_items)

    truth_key, set_truth_key = solara.use_state("")
    forest_key, set_forest_key = solara.use_state(default_forest_key(p) or "")
    interval, set_interval = solara.use_state("")
    selected_maps, set_selected_maps = solara.use_state([])
    recompute, set_recompute = solara.use_state(True)
    form_error, set_form_error = solara.use_state(None)

    def on_truth_change(key):
        set_truth_key(key)
        # Auto-fill the interval parsed from the truth variable's name; the user
        # can still override it in the field afterwards.
        ti = parse_interval(p, key)
        set_interval(str(ti) if ti is not None else "")

    def on_run():
        if p is None or n_predictions == 0:
            return
        spec, err = build_truth_spec(p, truth_key, forest_key, interval)
        if err:
            set_form_error(err)
            return
        set_form_error(None)
        job_id = str(uuid.uuid4())[:8]
        eval_jobs.set(list(eval_jobs.value) + [
            {"id": job_id, "status": "running", "error": None}])
        spawn_in_context(
            _run_evaluation,
            (job_id, p, list(selected_maps), spec, recompute),
        )

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 7 — Evaluation")
        solara.Text(
            "Score selected maps against one chosen truth (observed deforestation "
            "+ forest-at-start + interval) so maps from different datasets are "
            "comparable. Indices: MedAE / R² / RMSE / wRMSE.")

        if p is None or not var_items:
            solara.Info("No processed variables yet — complete earlier steps first.")
            return

        rv.Select(
            label="Truth — observed deforestation",
            items=var_items, item_text="text", item_value="value",
            v_model=truth_key, on_v_model=on_truth_change,
            dense=True, outlined=True,
        )
        rv.Select(
            label="Forest at period start",
            items=var_items, item_text="text", item_value="value",
            v_model=forest_key, on_v_model=set_forest_key,
            dense=True, outlined=True,
        )
        rv.TextField(
            label="Interval (years)", v_model=interval, on_v_model=set_interval,
            type="number", dense=True, outlined=True,
            hint="Auto-parsed from the truth variable name; editable.",
            persistent_hint=True,
        )
        rv.Select(
            label="Maps to evaluate (empty = all)",
            items=pred_items, item_text="text", item_value="value",
            v_model=selected_maps, on_v_model=set_selected_maps,
            multiple=True, chips=True, dense=True, outlined=True,
            no_data_text="No predictions registered. Run inference in Step 6.",
        )
        rv.Switch(label="Recompute defrate", v_model=recompute,
                  on_v_model=set_recompute)

        solara.Button(
            "Run evaluation", icon_name="mdi-chart-bar", color="primary", small=True,
            on_click=on_run, disabled=n_predictions == 0,
        )
        if n_predictions == 0:
            solara.Info("No predictions available yet — run inference first.")
        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        EvaluationResults(eval_jobs=eval_jobs, indices_df=eval_indices)

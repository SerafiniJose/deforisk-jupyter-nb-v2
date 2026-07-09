# gui/tile/evaluation_tile.py
"""Step 8 — Evaluation tile.

Score user-selected maps against ONE explicitly-chosen truth (deforestation
raster + forest-at-start variable + interval), so maps from different datasets
are directly comparable. Each run is saved to project.evaluations and shown in a
list; click a row to view its table. See evaluate_against_truth in
spatialrisk/evaluation.py.
"""

import logging
import uuid
from datetime import datetime

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.tile.evaluation_helpers import (
    ALL_METRICS, build_evaluation_record, build_truth_spec, default_forest_key,
    map_items, metric_items, parse_csizes, parse_interval, variable_items)
from gui.widget.evaluation_results import (
    EvaluationResults, EvaluationTableDialog)

logger = logging.getLogger("spatial_risk")

# Module-level reactive shared across re-renders (transient per-run job status).
eval_jobs = solara.reactive([])


def _run_evaluation(job_id, project, prediction_keys, spec, recompute, created_at,
                    csizes, metrics):
    """Background job: evaluate, build + register a record, re-render the list."""
    try:
        from spatialrisk.evaluation import evaluate_against_truth

        p = project.value
        df = evaluate_against_truth(
            p,
            prediction_keys=prediction_keys or None,
            defor_file=spec["defor_file"],
            forest_file=spec["forest_file"],
            time_interval=spec["time_interval"],
            truth_tag=spec["truth_tag"],
            csizes=tuple(csizes),
            recompute_defrate=recompute,
            auto_save=False,
        )
        resolved = list(prediction_keys) or list(p.predictions.keys())
        record = build_evaluation_record(
            p, df, spec, resolved_keys=resolved, run_id=job_id,
            created_at=created_at, csizes=tuple(csizes), metrics=metrics)
        p.add_evaluation(record, auto_save=False)
        p.save()
        project.set(p.model_copy())
        update_job(eval_jobs, job_id, status="completed")
        logger.info("Evaluation saved as project.evaluations['%s'] (%d rows)",
                    record.storage_key(), len(df))
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
    csizes_text, set_csizes_text = solara.use_state("300")
    selected_metrics, set_selected_metrics = solara.use_state(list(ALL_METRICS))
    recompute, set_recompute = solara.use_state(True)
    form_error, set_form_error = solara.use_state(None)
    selected_eval, set_selected_eval = solara.use_state(None)

    def on_truth_change(key):
        set_truth_key(key)
        ti = parse_interval(p, key)
        set_interval(str(ti) if ti is not None else "")

    def on_run():
        if p is None or n_predictions == 0:
            return
        spec, err = build_truth_spec(p, truth_key, forest_key, interval)
        if err:
            set_form_error(err)
            return
        csizes, err = parse_csizes(csizes_text)
        if err:
            set_form_error(err)
            return
        if not selected_metrics:
            set_form_error(t("tiles.evaluation.error_select_metric"))
            return
        set_form_error(None)
        job_id = str(uuid.uuid4())[:8]
        created_at = datetime.now().isoformat(timespec="seconds")
        eval_jobs.set(list(eval_jobs.value) + [{
            "id": job_id, "status": "running", "error": None,
            "truth_tag": spec["truth_tag"],
            "n_maps": len(selected_maps) or n_predictions,
            "created_at": created_at,
        }])
        spawn_in_context(
            _run_evaluation,
            (job_id, project, list(selected_maps), spec, recompute, created_at,
             csizes, list(selected_metrics)),
        )

    def on_delete(key):
        cur = project.value
        if cur is None:
            return
        cur.delete_evaluation(key, auto_save=True)
        project.set(cur.model_copy())
        if selected_eval == key:
            set_selected_eval(None)

    def on_dismiss(job_id):
        eval_jobs.set([j for j in eval_jobs.value if j["id"] != job_id])

    with solara.Column(style="gap: 16px;"):
        solara.Markdown(t("tiles.evaluation.header"))
        solara.Text(t("tiles.evaluation.description"))

        if p is None or not var_items:
            solara.Info(t("tiles.evaluation.error_no_variables"))
            return

        rv.Select(
            label=t("tiles.evaluation.truth_label"),
            items=var_items, item_text="text", item_value="value",
            v_model=truth_key, on_v_model=on_truth_change,
            dense=True, outlined=True,
        )
        rv.Select(
            label=t("tiles.evaluation.forest_label"),
            items=var_items, item_text="text", item_value="value",
            v_model=forest_key, on_v_model=set_forest_key,
            dense=True, outlined=True,
        )
        rv.TextField(
            label=t("tiles.evaluation.interval_label"),
            v_model=interval, on_v_model=set_interval,
            type="number", dense=True, outlined=True,
            hint=t("tiles.evaluation.interval_hint"),
            persistent_hint=True,
        )
        rv.Select(
            label=t("tiles.evaluation.maps_label"),
            items=pred_items, item_text="text", item_value="value",
            v_model=selected_maps, on_v_model=set_selected_maps,
            multiple=True, chips=True, dense=True, outlined=True,
            no_data_text=t("tiles.evaluation.maps_no_data"),
        )
        rv.TextField(
            label=t("tiles.evaluation.csizes_label"),
            v_model=csizes_text, on_v_model=set_csizes_text,
            dense=True, outlined=True,
            hint=t("tiles.evaluation.csizes_hint"),
            persistent_hint=True,
        )
        rv.Select(
            label=t("tiles.evaluation.metrics_label"),
            items=metric_items(), item_text="text", item_value="value",
            v_model=selected_metrics, on_v_model=set_selected_metrics,
            multiple=True, chips=True, dense=True, outlined=True,
        )
        rv.Switch(label=t("tiles.evaluation.recompute_label"), v_model=recompute,
                  on_v_model=set_recompute)

        solara.Button(
            t("tiles.evaluation.run_button"),
            icon_name="mdi-chart-bar", color="primary", small=True,
            on_click=on_run, disabled=n_predictions == 0,
        )
        if n_predictions == 0:
            solara.Info(t("tiles.evaluation.error_no_predictions"))
        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        EvaluationResults(eval_jobs=eval_jobs, project=project,
                          on_open=set_selected_eval, on_delete=on_delete,
                          on_dismiss=on_dismiss)
        EvaluationTableDialog(project=project, eval_key=selected_eval,
                              on_close=lambda: set_selected_eval(None))

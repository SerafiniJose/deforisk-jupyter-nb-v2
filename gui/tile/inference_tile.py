"""Step 5 — Inference tile."""

import logging
import threading
import uuid

import reacton.ipyvuetify as rv
import solara

from gui.widget.inference_output_list import InferenceOutputList

logger = logging.getLogger("spatial_risk")

# Module-level reactive shared across re-renders
inference_jobs = solara.reactive([])


def _run_inference(job_id, model_key, dataset_key, project):
    """Run model inference in a background thread."""
    try:
        from gui.scripts.inference_runner import run_inference

        run_inference(project, model_key, dataset_key)

        jobs = list(inference_jobs.value)
        for j in jobs:
            if j["id"] == job_id:
                if j["status"] == "cancelled":
                    break
                j["status"] = "completed"
                j["output_path"] = "see project predictions"
                break
        inference_jobs.set(jobs)
        logger.info("Inference completed: %s on %s", model_key, dataset_key)

    except Exception as exc:
        logger.exception("Inference failed for %s on %s", model_key, dataset_key)
        jobs = list(inference_jobs.value)
        for j in jobs:
            if j["id"] == job_id:
                if j["status"] != "cancelled":
                    j["status"] = "failed"
                    j["error"] = str(exc)
                break
        inference_jobs.set(jobs)


@solara.component
def InferenceTile(project):
    """Inference tab: select trained model and dataset, run prediction."""
    p = project.value

    # Trained model selection
    model_keys = sorted(p.models.keys()) if p and p.models else []
    selected_model, set_selected_model = solara.use_state("")

    # Dataset selection
    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state("")

    # Form messages
    form_error, set_form_error = solara.use_state(None)

    def on_run():
        set_form_error(None)
        if p is None:
            set_form_error("No active project.")
            return
        if not selected_model or selected_model not in p.models:
            set_form_error("Select a valid trained model.")
            return
        if not selected_dataset or selected_dataset not in p.datasets:
            set_form_error("Select a valid dataset.")
            return

        job_id = str(uuid.uuid4())[:8]
        job = {
            "id": job_id,
            "model_key": selected_model,
            "dataset_name": selected_dataset,
            "status": "running",
            "error": None,
            "output_path": None,
        }
        inference_jobs.set(list(inference_jobs.value) + [job])

        thread = threading.Thread(
            target=_run_inference,
            args=(job_id, selected_model, selected_dataset, p),
            daemon=True,
        )
        thread.start()
        logger.info(
            "Inference started: %s on %s (job=%s)",
            selected_model,
            selected_dataset,
            job_id,
        )

    def on_remove(job_id):
        inference_jobs.set(
            [j for j in inference_jobs.value if j["id"] != job_id]
        )

    can_run = bool(selected_model and selected_dataset)

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 6 — Inference")
        solara.Text("Select a trained model and a dataset, then run inference.")

        # Trained model selector
        rv.Select(
            label="Trained model",
            items=model_keys,
            v_model=selected_model,
            on_v_model=set_selected_model,
            dense=True,
            outlined=True,
            no_data_text="No trained models available. Train one in Step 4.",
        )

        # Dataset selector
        rv.Select(
            label="Dataset",
            items=dataset_keys,
            v_model=selected_dataset,
            on_v_model=set_selected_dataset,
            dense=True,
            outlined=True,
            no_data_text="No datasets registered. Create one in Step 3.",
        )

        # Run button
        solara.Button(
            "Run inference",
            icon_name="mdi-play",
            color="primary",
            small=True,
            on_click=on_run,
            disabled=not can_run,
        )

        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        # Outputs list
        InferenceOutputList(
            inference_jobs=inference_jobs,
            on_remove=on_remove,
        )

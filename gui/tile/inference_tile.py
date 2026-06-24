"""Step 7 — Inference tile."""

import asyncio
import logging
import uuid

import reacton.ipyvuetify as rv
import solara
from pysepal.solara.components.inputs import FileInputComponent

from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.inference_output_list import InferenceOutputList

logger = logging.getLogger("spatial_risk")

# Map display palette choices offered when importing a local prediction raster.
_IMPORT_RASTER_EXTENSIONS = [".tif", ".tiff", ".vrt", ".nc"]
_IMPORT_PALETTES = {
    "FAR ramp (probability, pinned 1..65535)": "far",
    "Auto-stretch ramp to file range": "stretch",
}
_IMPORT_PALETTE_LABELS = list(_IMPORT_PALETTES.keys())

# Module-level reactives shared across re-renders
inference_jobs = solara.reactive([])
# Ids of completed jobs whose prediction raster(s) are currently on the map.
preds_on_map = solara.reactive(set())


def _pred_layer_key(storage_key: str) -> str:
    """Unique map-layer key for a registered prediction."""
    return f"pred_{storage_key}"


def _run_inference(job_id, model_key, dataset_key, project):
    """Run model inference in a background thread."""
    try:
        from gui.scripts.inference_runner import run_inference

        run_inference(project, model_key, dataset_key)

        update_job(
            inference_jobs,
            job_id,
            status="completed",
            output_path="see project predictions",
        )
        logger.info("Inference completed: %s on %s", model_key, dataset_key)

    except Exception as exc:
        logger.exception("Inference failed for %s on %s", model_key, dataset_key)
        update_job(inference_jobs, job_id, status="failed", error=str(exc))


def _run_import(job_id, src_path, name, palette, project, project_reactive):
    """Copy a local raster into the project as a Prediction (background thread).

    The copy can be large, so it runs off the render thread like inference does.
    On success the placeholder job is updated to the real (model_key, dataset_name)
    so the per-job map toggle resolves the registered raster, and the project is
    republished so the outputs list and Step 8 — Evaluation pick it up.
    """
    try:
        from gui.scripts.prediction_import import import_prediction

        pred = import_prediction(project, src_path, name, palette=palette, auto_save=True)

        update_job(
            inference_jobs,
            job_id,
            status="completed",
            model_key=pred.model_key,
            dataset_name=pred.dataset_name,
            output_path=str(pred.path),
        )
        if project_reactive is not None:
            project_reactive.set(project.model_copy())
        logger.info("Imported prediction '%s' registered as %s", name, pred.model_key)

    except Exception as exc:
        logger.exception("Prediction import failed for %s", src_path)
        update_job(inference_jobs, job_id, status="failed", error=str(exc))


@solara.component
def InferenceTile(project, map_=None, sepal_client=None):
    """Inference tab: select trained model and dataset, run prediction.

    Args:
        project: Reactive holding the current Project (or None).
        map_: SepalMap instance used by the per-prediction "add to map" toggle.
        sepal_client: SEPAL client backing the local-raster import file picker.
    """
    p = project.value

    # Trained model selection
    model_keys = sorted(p.models.keys()) if p and p.models else []
    selected_model, set_selected_model = solara.use_state("")

    # Dataset selection
    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state("")

    # Form messages
    form_error, set_form_error = solara.use_state(None)

    # Local-raster import state
    import_name, set_import_name = solara.use_state("")
    import_file, set_import_file = solara.use_state("")
    import_palette_label, set_import_palette_label = solara.use_state(_IMPORT_PALETTE_LABELS[0])
    import_error, set_import_error = solara.use_state(None)

    def on_import():
        set_import_error(None)
        if p is None:
            set_import_error("No active project.")
            return
        if not import_file or not str(import_file).strip():
            set_import_error("Select a raster file to import.")
            return
        if not import_name.strip():
            set_import_error("Enter a name for the imported prediction.")
            return

        palette = _IMPORT_PALETTES.get(import_palette_label, "far")
        job_id = str(uuid.uuid4())[:8]
        # Placeholder job; _run_import fills in the real model_key on completion.
        inference_jobs.set(list(inference_jobs.value) + [{
            "id": job_id,
            "model_key": import_name.strip(),
            "dataset_name": "imported",
            "status": "running",
            "error": None,
            "output_path": None,
        }])
        spawn_in_context(
            _run_import,
            (job_id, str(import_file), import_name.strip(), palette, p, project),
        )
        set_import_name("")
        set_import_file("")
        logger.info("Import started: '%s' (job=%s)", import_name.strip(), job_id)

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

        spawn_in_context(
            _run_inference,
            (job_id, selected_model, selected_dataset, p),
        )
        logger.info(
            "Inference started: %s on %s (job=%s)",
            selected_model,
            selected_dataset,
            job_id,
        )

    def _matching_predictions(job):
        """Predictions registered for *job*, keyed by storage_key (empty if none)."""
        if p is None:
            return {}
        return p.filter_predictions(
            model_key=job["model_key"], dataset_name=job["dataset_name"]
        )

    def predictions_for(job):
        """Registered predictions for a *completed* job (drives the map button)."""
        if job.get("status") != "completed":
            return {}
        return _matching_predictions(job)

    def _forget_on_map(job_id):
        remaining = set(preds_on_map.value)
        remaining.discard(job_id)
        preds_on_map.set(remaining)

    gen_overviews = solara.use_reactive(False)
    pending_toggle = solara.use_reactive(None)

    @solara.lab.use_task(dependencies=None, raise_error=False)
    async def _apply_pred_toggle():
        """Add/remove a completed job's prediction raster(s) on the map.

        The layer-add is offloaded to a worker thread (it builds overviews and a
        localtileserver tile client, both blocking) so Solara's event loop stays
        responsive. Removal is cheap and stays inline.
        """
        job = pending_toggle.value
        if job is None or map_ is None:
            return
        matches = predictions_for(job)
        if not matches:
            return
        job_id = job["id"]
        try:
            if job_id in preds_on_map.value:
                for sk in matches:
                    map_.remove_layer(_pred_layer_key(sk), none_ok=True)
                _forget_on_map(job_id)
            else:
                from gui.scripts.prediction_map import add_prediction_on_map

                added_any = False
                try:
                    for sk, pred in matches.items():
                        await asyncio.to_thread(
                            add_prediction_on_map,
                            map_,
                            str(pred.path),
                            model_key=job["model_key"],
                            layer_name=sk,
                            key=_pred_layer_key(sk),
                            fit_bounds=False,
                            build_overviews=gen_overviews.value,
                            display_palette=getattr(pred, "display_palette", None),
                        )
                        added_any = True
                finally:
                    # Mark the job on-map if ANY layer landed (even on partial
                    # failure) so toggle-off can remove all its keys; fire the
                    # reactive once, not per-iteration.
                    if added_any:
                        preds_on_map.set(set(preds_on_map.value) | {job_id})
        except Exception as exc:
            logger.exception("prediction map toggle failed for job %s", job_id)
            set_form_error(f"Could not toggle prediction on map: {exc}")

    def on_toggle_map(job):
        """Trigger the threaded add/remove task for a completed job."""
        if map_ is None:
            return
        pending_toggle.set(job)
        _apply_pred_toggle()

    pending_remove, set_pending_remove = solara.use_state(None)

    def _do_remove(job_id):
        job = next((j for j in inference_jobs.value if j["id"] == job_id), None)
        # Drop any prediction layers this job placed on the map before forgetting it.
        if map_ is not None and job_id in preds_on_map.value:
            if job is not None:
                for sk in _matching_predictions(job):
                    map_.remove_layer(_pred_layer_key(sk), none_ok=True)
            _forget_on_map(job_id)
        # Delete the registered predictions (registry + output rasters).
        cur = project.value
        if cur is not None and job is not None:
            keys = list(_matching_predictions(job).keys())
            for k in keys:
                cur.delete_prediction(k, auto_save=False)
            if keys:
                cur.save()
                project.set(cur.model_copy())
        inference_jobs.set(
            [j for j in inference_jobs.value if j["id"] != job_id]
        )

    can_run = bool(selected_model and selected_dataset)

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 7 — Inference")
        solara.Text("Select a trained model and a dataset, then run inference.")

        # Trained model selector
        rv.Select(
            label="Trained model",
            items=model_keys,
            v_model=selected_model,
            on_v_model=set_selected_model,
            dense=True,
            outlined=True,
            no_data_text="No trained models available. Train one in Step 6.",
        )

        # Dataset selector
        rv.Select(
            label="Dataset",
            items=dataset_keys,
            v_model=selected_dataset,
            on_v_model=set_selected_dataset,
            dense=True,
            outlined=True,
            no_data_text="No datasets registered. Create one in Step 4.",
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

        # Import a prediction produced outside the app (e.g. a QGIS export). The
        # raster is copied into the project and registered like a computed
        # prediction, so it shows below and is selectable in Step 8 — Evaluation.
        # Collapsed by default to keep the run form compact.
        with rv.ExpansionPanels(flat=True):
            with rv.ExpansionPanel():
                with rv.ExpansionPanelHeader():
                    solara.Text("Import a local prediction raster")
                with rv.ExpansionPanelContent():
                    with solara.Column(style="gap: 12px;"):
                        rv.TextField(
                            label="Name",
                            v_model=import_name,
                            on_v_model=set_import_name,
                            dense=True,
                            outlined=True,
                            placeholder="e.g. qgis-export-2020",
                        )
                        FileInputComponent(
                            label="Select raster file",
                            value=import_file,
                            on_value=set_import_file,
                            sepal_client=sepal_client,
                            root="",
                            extensions=_IMPORT_RASTER_EXTENSIONS,
                            clearable=True,
                        )
                        rv.Select(
                            label="Map palette",
                            items=_IMPORT_PALETTE_LABELS,
                            v_model=import_palette_label,
                            on_v_model=set_import_palette_label,
                            dense=True,
                            outlined=True,
                        )
                        solara.Text(
                            "The raster must be spatially comparable to the truth "
                            "chosen in Step 8 to be evaluated."
                        )
                        solara.Button(
                            "Import",
                            icon_name="mdi-upload",
                            color="primary",
                            small=True,
                            on_click=on_import,
                        )
                        if import_error:
                            rv.Alert(type_="error", dense=True, children=[import_error])

        # Optional raster optimisation before predictions hit the map.
        solara.Checkbox(
            label="Generate overviews before display",
            value=gen_overviews.value,
            on_value=gen_overviews.set,
        )
        if _apply_pred_toggle.pending:
            rv.ProgressLinear(indeterminate=True, color="primary")

        # Outputs list
        InferenceOutputList(
            inference_jobs=inference_jobs,
            on_remove=set_pending_remove,
            on_toggle_map=on_toggle_map if map_ is not None else None,
            preds_on_map=preds_on_map,
            predictions_for=predictions_for,
        )

        _pending_job = (
            next((j for j in inference_jobs.value if j["id"] == pending_remove), None)
            if pending_remove
            else None
        )
        _pending_pred_count = len(_matching_predictions(_pending_job)) if _pending_job else 0
        ConfirmDialog(
            open=pending_remove is not None,
            on_cancel=lambda: set_pending_remove(None),
            on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
            title="Delete predictions?" if _pending_pred_count else "Remove job?",
            message=(
                f"Delete {_pending_pred_count} prediction raster(s) from this run? This "
                "removes them from the project and deletes the files. This cannot be undone."
                if _pending_pred_count
                else "Remove this inference job from the list?"
            ),
            confirm_label="Delete" if _pending_pred_count else "Remove",
        )

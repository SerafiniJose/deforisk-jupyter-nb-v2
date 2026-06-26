"""Step 7 — Inference tile."""

import asyncio
import logging
import re
import uuid

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t, plural
from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.inference_output_list import InferenceOutputList
from gui.widget.prediction_import_modal import PredictionImportModal

logger = logging.getLogger("spatial_risk")


def _sanitize_pred_name(name: str) -> str:
    """Normalise a user-typed prediction name to a key/path-safe token.

    Keeps alphanumerics, dash and underscore (so the default ``model__dataset``
    token survives intact); collapses any other run into one underscore. The
    result is used as the prediction's registry key, output subfolder and the
    ``Prediction.name`` the outputs list matches on, so it must be path-safe.
    """
    return re.sub(r"[^A-Za-z0-9_-]+", "_", (name or "").strip()).strip("_")


def _default_pred_name(model_key: str, dataset_name: str) -> str:
    """Prefilled prediction name for a (model, dataset) selection."""
    if not model_key or not dataset_name:
        return ""
    return _sanitize_pred_name(f"{model_key}__{dataset_name}")


def _prediction_name_exists(project, name: str) -> bool:
    """True if a prediction already uses *name* (as its key or its name field).

    Covers both new name-keyed predictions (``Prediction.name == name``) and the
    legacy provenance key (``{model}__{dataset}`` matches the default token).
    """
    if project is None or not name:
        return False
    if name in getattr(project, "predictions", {}):
        return True
    return bool(project.filter_predictions(name=name))


# Module-level reactives shared across re-renders
inference_jobs = solara.reactive([])
# Ids of completed jobs whose prediction raster(s) are currently on the map.
preds_on_map = solara.reactive(set())


def _pred_layer_key(storage_key: str) -> str:
    """Unique map-layer key for a registered prediction."""
    return f"pred_{storage_key}"


def _run_inference(job_id, model_key, dataset_key, project, name=None):
    """Run model inference in a background thread."""
    try:
        from gui.scripts.inference_runner import run_inference

        run_inference(project, model_key, dataset_key, name=name)

        update_job(
            inference_jobs,
            job_id,
            status="completed",
            output_path="see project predictions",
        )
        logger.info("Inference completed: %s on %s (name=%s)", model_key, dataset_key, name)

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

    # Prediction name — names the output so re-runs don't silently overwrite.
    # Prefilled with the model+dataset default; until the user edits it, the
    # field tracks the current selection (touched flag freezes their choice).
    pred_name, set_pred_name = solara.use_state("")
    pred_name_touched, set_pred_name_touched = solara.use_state(False)
    default_pred_name = _default_pred_name(selected_model, selected_dataset)
    effective_pred_name = (
        _sanitize_pred_name(pred_name) if pred_name_touched else default_pred_name
    )
    pred_name_field = pred_name if pred_name_touched else default_pred_name

    def set_pred_name_input(v):
        set_pred_name_touched(True)
        set_pred_name(v)

    # Form messages
    form_error, set_form_error = solara.use_state(None)

    # Local-raster import — the form lives in PredictionImportModal; the tile only
    # owns the dialog's open state and turns its entry into a background job.
    import_modal_open = solara.use_reactive(False)

    def on_import(entry):
        """Spawn a background copy for a raster the modal validated.

        The modal already enforced the required fields; the project-level guard
        stays here (surfaced via the tile's form error, as the dialog is closed
        by the time this runs).
        """
        if p is None:
            set_form_error(t("tiles.inference.error_no_project"))
            return
        name = entry["name"]
        job_id = str(uuid.uuid4())[:8]
        # Placeholder job; _run_import fills in the real model_key on completion.
        inference_jobs.set(list(inference_jobs.value) + [{
            "id": job_id,
            "model_key": name,
            "dataset_name": "imported",
            "status": "running",
            "error": None,
            "output_path": None,
        }])
        spawn_in_context(
            _run_import,
            (job_id, entry["path"], name, entry["palette"], p, project),
        )
        logger.info("Import started: '%s' (job=%s)", name, job_id)

    pending_overwrite, set_pending_overwrite = solara.use_state(None)

    def _launch_inference(name):
        """Create the output job row and spawn the worker. Inputs pre-validated."""
        job_id = str(uuid.uuid4())[:8]
        job = {
            "id": job_id,
            "model_key": selected_model,
            "dataset_name": selected_dataset,
            "pred_name": name,
            "status": "running",
            "error": None,
            "output_path": None,
        }
        inference_jobs.set(list(inference_jobs.value) + [job])

        spawn_in_context(
            _run_inference,
            (job_id, selected_model, selected_dataset, p, name),
        )
        logger.info(
            "Inference started: %s on %s as '%s' (job=%s)",
            selected_model,
            selected_dataset,
            name,
            job_id,
        )

    def on_run():
        set_form_error(None)
        if p is None:
            set_form_error(t("tiles.inference.error_no_project"))
            return
        if not selected_model or selected_model not in p.models:
            set_form_error(t("tiles.inference.error_invalid_model"))
            return
        if not selected_dataset or selected_dataset not in p.datasets:
            set_form_error(t("tiles.inference.error_invalid_dataset"))
            return
        name = effective_pred_name
        if not name:
            set_form_error(t("tiles.inference.error_name_required"))
            return

        # An existing prediction with this name would be replaced — confirm first.
        if _prediction_name_exists(p, name):
            set_pending_overwrite({"name": name})
            return

        _launch_inference(name)

    def _matching_predictions(job):
        """Predictions registered for *job*, keyed by storage_key (empty if none)."""
        if p is None:
            return {}
        # Named runs (the current path) group their output(s) by the chosen name,
        # so two runs of the same model+dataset under different names stay
        # separate. Legacy/imported jobs (no pred_name) fall back to provenance.
        if job.get("pred_name"):
            return p.filter_predictions(name=job["pred_name"])
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
            set_form_error(t("tiles.inference.error_map_toggle", exc=exc))

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

    can_run = bool(selected_model and selected_dataset and effective_pred_name)

    with solara.Column(style="gap: 16px;"):
        solara.Markdown(t("tiles.inference.header"))
        solara.Text(t("tiles.inference.description"))

        # Trained model selector
        rv.Select(
            label=t("tiles.inference.model_select_label"),
            items=model_keys,
            v_model=selected_model,
            on_v_model=set_selected_model,
            dense=True,
            outlined=True,
            no_data_text=t("tiles.inference.model_select_no_data"),
        )

        # Dataset selector
        rv.Select(
            label=t("tiles.inference.dataset_select_label"),
            items=dataset_keys,
            v_model=selected_dataset,
            on_v_model=set_selected_dataset,
            dense=True,
            outlined=True,
            no_data_text=t("tiles.inference.dataset_select_no_data"),
        )

        # Prediction name — required; names the output so re-runs don't silently
        # overwrite. The hint shows the resulting registry key and flags when that
        # name is already taken (running will overwrite it).
        pred_exists = _prediction_name_exists(p, effective_pred_name)
        rv.TextField(
            label=t("tiles.inference.pred_name_label"),
            v_model=pred_name_field,
            on_v_model=set_pred_name_input,
            dense=True,
            outlined=True,
            messages=(
                t("tiles.inference.pred_name_exists_warning", name=effective_pred_name)
                if pred_exists
                else (
                    t("tiles.inference.pred_name_saved_as", name=effective_pred_name)
                    if effective_pred_name
                    else t("tiles.inference.pred_name_required")
                )
            ),
            error=not effective_pred_name,
        )

        # Run button
        solara.Button(
            t("tiles.inference.run_button"),
            icon_name="mdi-play",
            color="primary",
            small=True,
            on_click=on_run,
            disabled=not can_run,
        )

        # Import a prediction produced outside the app (e.g. a QGIS export). It is
        # copied + reprojected into the project and registered like a computed
        # prediction, so it shows in the outputs list and Step 8 — Evaluation.
        solara.Button(
            t("tiles.inference.import_button"),
            icon_name="mdi-plus",
            color="primary",
            small=True,
            on_click=lambda: import_modal_open.set(True),
        )

        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        # Optional raster optimisation before predictions hit the map.
        solara.Checkbox(
            label=t("tiles.inference.generate_overviews_label"),
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
            title=(
                t("tiles.inference.confirm_delete_predictions_title")
                if _pending_pred_count
                else t("tiles.inference.confirm_remove_job_title")
            ),
            message=(
                plural(
                    _pending_pred_count,
                    "tiles.inference.confirm_delete_predictions_message_one",
                    "tiles.inference.confirm_delete_predictions_message_other",
                )
                if _pending_pred_count
                else t("tiles.inference.confirm_remove_job_message")
            ),
            confirm_label=(
                t("common.delete") if _pending_pred_count else t("common.remove")
            ),
        )

        # Overwrite confirmation — shown when the chosen prediction name exists.
        def _confirm_overwrite():
            ov = pending_overwrite
            set_pending_overwrite(None)
            if ov:
                _launch_inference(ov["name"])

        ConfirmDialog(
            open=pending_overwrite is not None,
            on_cancel=lambda: set_pending_overwrite(None),
            on_confirm=_confirm_overwrite,
            title=t("tiles.inference.confirm_overwrite_title"),
            message=(
                t(
                    "tiles.inference.confirm_overwrite_message",
                    name=pending_overwrite["name"],
                )
                if pending_overwrite
                else ""
            ),
            confirm_label=t("tiles.inference.confirm_overwrite_label"),
        )

        # Import-a-local-prediction modal (opened from the top action bar).
        PredictionImportModal(
            open_=import_modal_open,
            on_import=on_import,
            sepal_client=sepal_client,
        )

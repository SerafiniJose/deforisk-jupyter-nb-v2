"""Step 7 — Inference tile."""

import asyncio
import logging
import re
import uuid

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t, plural
from gui.scripts.product_rows import job_row_key
from gui.scripts.solara_threads import publish_if_current, spawn_in_context, update_job
from gui.store.project_writers import writing
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
# Row keys (prediction name, else "{model_key}__{dataset_name}") of prediction
# groups currently shown on the map — works for predictions loaded from disk,
# not just same-session runs.
preds_on_map = solara.reactive(set())


def _pred_layer_key(storage_key: str) -> str:
    """Unique map-layer key for a registered prediction."""
    return f"pred_{storage_key}"


def _run_inference(job_id, model_key, dataset_key, project, name=None):
    """Run model inference in a background thread."""
    try:
        with writing(project.project_name):
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
        with writing(project.project_name):
            from gui.scripts.prediction_import import import_prediction

            pred = import_prediction(project, src_path, name, palette=palette, auto_save=True)

            update_job(
                inference_jobs,
                job_id,
                status="completed",
                pred_name=name,
                model_key=pred.model_key,
                dataset_name=pred.dataset_name,
                output_path=str(pred.path),
            )
            publish_if_current(project_reactive, project)
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
    # Only flag the empty-name field as an error once a run has been attempted,
    # so it doesn't show a red "Required." before the user has done anything.
    run_attempted, set_run_attempted = solara.use_state(False)

    def set_pred_name_input(v):
        set_pred_name_touched(True)
        set_run_attempted(False)
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
            set_run_attempted(True)
            set_form_error(t("tiles.inference.error_name_required"))
            return

        # An existing prediction with this name would be replaced — confirm first.
        if _prediction_name_exists(p, name):
            set_pending_overwrite({"name": name})
            return

        _launch_inference(name)

    def _forget_on_map(row_key):
        remaining = set(preds_on_map.value)
        remaining.discard(row_key)
        preds_on_map.set(remaining)

    gen_overviews = solara.use_reactive(False)
    pending_toggle = solara.use_reactive(None)

    @solara.lab.use_task(dependencies=None, raise_error=False)
    async def _apply_pred_toggle():
        """Add/remove a prediction row's raster(s) on the map.

        The layer-add is offloaded to a worker thread (it builds overviews and a
        localtileserver tile client, both blocking) so Solara's event loop stays
        responsive. Removal is cheap and stays inline.
        """
        row = pending_toggle.value
        if row is None or map_ is None or p is None:
            return
        storage_keys = [k for k in row.get("storage_keys", []) if k in p.predictions]
        if not storage_keys:
            return
        row_key = row["key"]
        try:
            if row_key in preds_on_map.value:
                for sk in storage_keys:
                    map_.remove_layer(_pred_layer_key(sk), none_ok=True)
                _forget_on_map(row_key)
            else:
                from gui.scripts.prediction_map import add_prediction_on_map

                added_any = False
                try:
                    for sk in storage_keys:
                        pred = p.predictions[sk]
                        await asyncio.to_thread(
                            add_prediction_on_map,
                            map_,
                            str(pred.path),
                            model_key=row["model_key"],
                            layer_name=sk,
                            key=_pred_layer_key(sk),
                            fit_bounds=False,
                            build_overviews=gen_overviews.value,
                            display_palette=getattr(pred, "display_palette", None),
                        )
                        added_any = True
                finally:
                    # Mark the row on-map if ANY layer landed (even on partial
                    # failure) so toggle-off can remove all its keys; fire the
                    # reactive once, not per-iteration.
                    if added_any:
                        preds_on_map.set(set(preds_on_map.value) | {row_key})
        except Exception as exc:
            logger.exception("prediction map toggle failed for row %s", row.get("key"))
            set_form_error(t("tiles.inference.error_map_toggle", exc=exc))

    def on_toggle_map(row):
        """Trigger the threaded add/remove task for a prediction row."""
        if map_ is None:
            return
        pending_toggle.set(row)
        _apply_pred_toggle()

    pending_delete, set_pending_delete = solara.use_state(None)  # row dict or None

    def on_dismiss(job_id):
        # Failed job rows only — never touches the prediction registry.
        inference_jobs.set([j for j in inference_jobs.value if j["id"] != job_id])

    def _delete_row(row):
        cur = project.value
        if cur is None:
            return
        row_key = row["key"]
        if map_ is not None and row_key in preds_on_map.value:
            for sk in row.get("storage_keys", []):
                map_.remove_layer(_pred_layer_key(sk), none_ok=True)
            _forget_on_map(row_key)
        deleted = False
        for sk in row.get("storage_keys", []):
            deleted = cur.delete_prediction(sk, auto_save=False) or deleted
        if deleted:
            cur.save()
            project.set(cur.model_copy())
        # Purge completed session jobs that produced this row, so a stale
        # "completed" job doesn't resurface once its registry group is gone.
        # A running/failed re-run of the same name is left alone.
        inference_jobs.set(
            [
                j for j in inference_jobs.value
                if job_row_key(j) != row_key or j.get("status") != "completed"
            ]
        )

    # Name intentionally left out: the button stays enabled so an empty name can
    # surface the "Required." error on click (on_run re-validates and bails).
    can_run = bool(selected_model and selected_dataset)

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
            hint=t("tiles.inference.model_select_hint"),
            persistent_hint=True,
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
            hint=t("tiles.inference.dataset_select_hint"),
            persistent_hint=True,
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
                    else (
                        t("tiles.inference.pred_name_required")
                        if run_attempted
                        else ""
                    )
                )
            ),
            error=run_attempted and not effective_pred_name,
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
            project=project,
            inference_jobs=inference_jobs,
            preds_on_map=preds_on_map,
            on_toggle_map=on_toggle_map if map_ is not None else None,
            on_dismiss=on_dismiss,
            on_delete=set_pending_delete,
        )

        _pending_count = len(pending_delete.get("storage_keys", [])) if pending_delete else 0
        ConfirmDialog(
            open=pending_delete is not None,
            on_cancel=lambda: set_pending_delete(None),
            on_confirm=lambda: (_delete_row(pending_delete), set_pending_delete(None)),
            title=t("tiles.inference.confirm_delete_predictions_title"),
            message=plural(
                _pending_count,
                "tiles.inference.confirm_delete_predictions_message_one",
                "tiles.inference.confirm_delete_predictions_message_other",
            ),
            confirm_label=t("common.delete"),
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

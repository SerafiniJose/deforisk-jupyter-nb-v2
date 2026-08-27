"""New Prediction dialog for the Inference step.

One dialog for both ways a prediction is created: running a trained model on a
dataset, or importing a raster produced outside the app. A Source select swaps
the input fields; the name field is shared. Import keeps the backend's
non-destructive duplicate policy (a taken name gets a ``-2`` suffix), so its
name preview shows the *resolved* key instead of asking to replace.
"""

from pathlib import Path
from typing import Callable

import reacton.ipyvuetify as rv
import solara
from pysepal.solara.components.inputs import FileInputComponent

from gui.i18n import t
from gui.scripts.artifact_names import (
    default_pred_name,
    prediction_name_exists,
    sanitize_key,
)
from gui.scripts.inference_runner import (
    is_ml_family,
    is_mw_family,
    mask_layer_candidates,
    mw_window_options,
    suggested_mask_layer,
)
from gui.scripts.prediction_import import resolve_import_key, sanitize_import_name
from gui.widget.artifact_name_field import ArtifactNameField, use_artifact_name
from gui.widget.creation_dialog import CreationDialog

# Raster file types accepted for a local prediction import.
_IMPORT_RASTER_EXTENSIONS = [".tif", ".tiff", ".vrt", ".nc"]

# Select value for the explicit "no mask" choice. A sentinel rather than ""
# because "" is the field's *unset* state, which validation refuses: predicting
# unmasked must be a decision, never a default.
NO_MASK = "__no_mask__"


def _source_items():
    return [
        {"text": t("tiles.inference.source_model"), "value": "model"},
        {"text": t("tiles.inference.source_import"), "value": "import"},
    ]


def _import_palette_items():
    return [
        {"text": t("widgets.prediction_import_modal.palette_far"), "value": "far"},
        {
            "text": t("widgets.prediction_import_modal.palette_stretch"),
            "value": "stretch",
        },
    ]


@solara.component
def PredictionFormDialog(
    project, open_, on_submit: Callable[[dict], None], sepal_client=None, prefill=None
):
    """Prediction form in the shared CreationDialog frame.

    on_submit(entry) receives {"kind": "model", "model_key", "dataset_key",
    "name"} or {"kind": "import", "name", "path", "palette"}; the tile owns
    the job row and the worker. A model entry for an ML family (GLM/RF/ICAR)
    also carries "mask_layer" — the storage key of the project raster to mask
    with (any processed raster, dataset membership not required), or None for
    an explicit "no mask".

    Args:
        project: solara.Reactive[Project].
        open_: solara.Reactive[bool].
        on_submit: callback receiving the entry dict described above.
        sepal_client: SEPAL client backing the import file picker.
        prefill: optional solara.Reactive holding a previously submitted entry
            dict (or None). While the dialog is open with a non-empty prefill,
            the fields are seeded from it — this is how a failed run reopens
            for editing. The tile owns clearing it before a fresh "New" open.
    """
    p = project.value

    source, set_source = solara.use_state("model")

    # --- model mode state
    model_keys = sorted(p.models.keys()) if p and p.models else []
    selected_model, set_selected_model = solara.use_state("")
    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state("")

    # --- mask layer (ML families only)
    # GLM/RF/ICAR mask their output with a project raster the user assigns —
    # any processed raster qualifies (it need not be a feature of the selected
    # dataset), and "no mask" is a valid explicit choice, so the algorithms
    # carry no assumption about the mask's origin. The candidate list and the
    # forest-layer suggestion both come from the runner module so the dialog
    # can never offer a layer the runner would reject.
    mask_layer, set_mask_layer = solara.use_state("")
    ml_family = source == "model" and is_ml_family(selected_model)
    mask_candidates = mask_layer_candidates(p) if ml_family else []
    show_mask = ml_family

    # A prefilled mask choice must survive the re-seed that fires when the
    # prefilled model changes the candidate set — the seed consumes it instead
    # of overwriting it with the forest suggestion.
    pending_mask = solara.use_ref(None)

    def seed_mask_layer():
        """Suggest the sole forest layer; anything ambiguous starts unset."""
        if pending_mask.current is not None and ml_family:
            set_mask_layer(pending_mask.current)
            pending_mask.current = None
            return
        set_mask_layer(suggested_mask_layer(p) if ml_family else "")

    # Re-seeds whenever the candidate set changes (a model-family switch) and
    # never in between, so a deliberate pick survives re-renders.
    solara.use_effect(seed_mask_layer, [tuple(mask_candidates)])

    # --- window sizes (MW family only)
    # MW inference fans out one map per trained window; the user narrows the
    # run here (e.g. re-run only the window that won evaluation). Seeded with
    # every trained window so the default equals the historic all-windows run.
    selected_windows, set_selected_windows = solara.use_state([])
    mw_family = source == "model" and is_mw_family(selected_model)
    window_options = mw_window_options(p, selected_model) if mw_family else []
    pending_windows = solara.use_ref(None)

    def seed_windows():
        """Select every trained window, unless a prefill narrowed the run."""
        if pending_windows.current is not None and mw_family:
            keep = [w for w in pending_windows.current if w in window_options]
            set_selected_windows(keep or list(window_options))
            pending_windows.current = None
            return
        set_selected_windows(list(window_options))

    # selected_model is a dependency on purpose: two MW models can expose the
    # SAME option tuple, and switching between them must still reseed to
    # all-selected — a subset chosen for model A must not leak into model B.
    solara.use_effect(seed_windows, [selected_model, tuple(window_options)])

    # --- import mode state
    file_path, set_file_path = solara.use_state("")
    palette, set_palette = solara.use_state("far")

    # Name tracks the mode's suggestion until the user edits it: model mode
    # suggests "model__dataset", import mode the sanitized file stem.
    if source == "model":
        suggestion = default_pred_name(selected_model, selected_dataset)
    else:
        suggestion = (
            sanitize_import_name(Path(str(file_path)).stem) if file_path else ""
        )
    name_value, on_name_input, reset_name = use_artifact_name(suggestion)

    def seed_from_prefill():
        """Seed every field from the prefill entry each time the dialog opens.

        Keyed on the open flag as well as the entry so re-editing the same
        failed job after a cancel (same entry, fields reset on close) seeds
        again. A fresh open with the prefill cleared is a no-op.
        """
        entry = prefill.value if prefill is not None else None
        if not open_.value or not entry:
            return
        if entry.get("kind") == "import":
            set_source("import")
            set_file_path(entry.get("path", ""))
            set_palette(entry.get("palette", "far"))
        else:
            set_source("model")
            set_selected_model(entry.get("model_key", ""))
            set_selected_dataset(entry.get("dataset_key", ""))
            if "mask_layer" in entry:
                # None was the explicit "no mask" submission — show the sentinel.
                pending_mask.current = entry["mask_layer"] or NO_MASK
            if "windows" in entry:
                pending_windows.current = list(entry["windows"])
        on_name_input(entry.get("name", ""))

    solara.use_effect(
        seed_from_prefill,
        [open_.value, prefill.value if prefill is not None else None],
    )

    clean = sanitize_key(name_value)
    # An MW run registers one prediction per selected window, suffixed
    # ``_w<size>`` (see BaseRiskModel._register_prediction), so both the name
    # preview and the "already taken" check work on the fanned-out keys.
    if source == "model" and mw_family and clean:
        mw_keys = [f"{clean}_w{w}" for w in sorted(selected_windows)]
        exists = any(prediction_name_exists(p, k) for k in mw_keys)
        preview_key = ", ".join(mw_keys) or clean
    else:
        mw_keys = []
        exists = prediction_name_exists(p, clean)
        preview_key = clean
    # Import never replaces: preview the key the import would actually get.
    src_suffix = Path(str(file_path)).suffix if file_path else ""
    resolved_import_key = (
        resolve_import_key(p, name_value.strip(), src_suffix)
        if p is not None and name_value.strip()
        else ""
    )

    def reset():
        pending_mask.current = None
        pending_windows.current = None
        set_selected_windows([])
        set_source("model")
        set_selected_model("")
        set_selected_dataset("")
        set_mask_layer("")
        set_file_path("")
        set_palette("far")
        reset_name()

    def on_source(v):
        # Re-arm the suggestion so the name follows the new mode's default.
        set_source(v)
        reset_name()

    def validate():
        if p is None:
            return t("tiles.inference.error_no_project")
        if source == "import":
            if not file_path or not str(file_path).strip():
                return t("widgets.prediction_import_modal.error_select_raster")
            if not name_value.strip():
                return t("widgets.prediction_import_modal.error_enter_name")
            return None
        if not selected_model or selected_model not in p.models:
            return t("tiles.inference.error_invalid_model")
        if not selected_dataset or selected_dataset not in p.datasets:
            return t("tiles.inference.error_invalid_dataset")
        if ml_family and not mask_layer:
            return t("tiles.inference.error_mask_required")
        # Gated on window_options: an MW model exposing none (untrained legacy
        # entry, empty win_size_list) renders no field and keeps today's
        # behaviour — apply() runs all windows or raises, exactly as before.
        if mw_family and window_options and not selected_windows:
            return t("tiles.inference.error_windows_required")
        if not clean:
            return t("tiles.inference.error_name_required")
        return None

    def will_replace():
        if source == "import":
            return None  # duplicate imports suffix instead of replacing
        if mw_family and mw_keys:
            # Report every colliding key, not just the first: one run can
            # overwrite several _w<size> maps and the confirmation must name
            # all of them.
            taken = [k for k in mw_keys if prediction_name_exists(p, k)]
            return ", ".join(taken) if taken else None
        return clean if prediction_name_exists(p, clean) else None

    def launch():
        if source == "import":
            on_submit(
                {
                    "kind": "import",
                    "name": name_value.strip(),
                    "path": str(file_path),
                    "palette": palette,
                }
            )
        else:
            entry = {
                "kind": "model",
                "model_key": selected_model,
                "dataset_key": selected_dataset,
                "name": clean,
            }
            if ml_family:
                # The user's assignment: a project-raster key, or None for the
                # explicit "no mask" choice (validate() guarantees one of the
                # two). Benchmark families resolve their own layers and never
                # carry the key.
                entry["mask_layer"] = None if mask_layer == NO_MASK else mask_layer
            if mw_family and window_options:
                # No key at all when the model exposes no options — the runner
                # then passes windows=None and apply() runs its usual set.
                entry["windows"] = sorted(selected_windows)
            on_submit(entry)

    with CreationDialog(
        open_=open_,
        title=t("tiles.inference.dialog_title"),
        create_label=t("tiles.inference.run_button"),
        validate=validate,
        will_replace=will_replace,
        launch=launch,
        on_close=reset,
        replace_message=lambda k: t(
            "tiles.inference.confirm_overwrite_message", name=k
        ),
    ):
        rv.Select(
            label=t("tiles.inference.source_label"),
            items=_source_items(),
            item_text="text",
            item_value="value",
            v_model=source,
            on_v_model=on_source,
            dense=True,
            outlined=True,
        )
        if source == "model":
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
            if mw_family:
                rv.Select(
                    label=t("tiles.inference.windows_label"),
                    items=[
                        {"text": f"{w}×{w}", "value": w}  # noqa: RUF001
                        for w in window_options
                    ],
                    item_text="text",
                    item_value="value",
                    v_model=selected_windows,
                    on_v_model=set_selected_windows,
                    multiple=True,
                    chips=True,
                    small_chips=True,
                    deletable_chips=True,
                    dense=True,
                    outlined=True,
                    class_="multi-chips",
                    hint=t("tiles.inference.windows_hint"),
                    persistent_hint=True,
                )
            if show_mask:
                rv.Select(
                    label=t("tiles.inference.mask_layer_label"),
                    items=[
                        {"text": t("tiles.inference.mask_layer_none"), "value": NO_MASK}
                    ]
                    + [{"text": n, "value": n} for n in mask_candidates],
                    item_text="text",
                    item_value="value",
                    v_model=mask_layer,
                    on_v_model=set_mask_layer,
                    dense=True,
                    outlined=True,
                    clearable=True,
                    hint=t("tiles.inference.mask_layer_hint"),
                    persistent_hint=True,
                )
        else:
            FileInputComponent(
                label=t("widgets.prediction_import_modal.label_file"),
                value=file_path,
                on_value=set_file_path,
                sepal_client=sepal_client,
                root="",
                extensions=_IMPORT_RASTER_EXTENSIONS,
                clearable=True,
            )
            rv.Select(
                label=t("widgets.prediction_import_modal.label_palette"),
                items=_import_palette_items(),
                item_text="text",
                item_value="value",
                v_model=palette,
                on_v_model=set_palette,
                dense=True,
                outlined=True,
            )
            solara.Text(t("widgets.prediction_import_modal.info_text"))
        ArtifactNameField(
            value=name_value,
            on_input=on_name_input,
            storage_key=preview_key if source == "model" else resolved_import_key,
            exists=exists if source == "model" else False,
            label=t("tiles.inference.pred_name_label"),
        )

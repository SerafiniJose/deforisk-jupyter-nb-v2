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
    mask_layer_candidates,
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
    project, open_, on_submit: Callable[[dict], None], sepal_client=None
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

    def seed_mask_layer():
        """Suggest the sole forest layer; anything ambiguous starts unset."""
        set_mask_layer(suggested_mask_layer(p) if ml_family else "")

    # Re-seeds whenever the candidate set changes (a model-family switch) and
    # never in between, so a deliberate pick survives re-renders.
    solara.use_effect(seed_mask_layer, [tuple(mask_candidates)])

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

    clean = sanitize_key(name_value)
    exists = prediction_name_exists(p, clean)
    # Import never replaces: preview the key the import would actually get.
    src_suffix = Path(str(file_path)).suffix if file_path else ""
    resolved_import_key = (
        resolve_import_key(p, name_value.strip(), src_suffix)
        if p is not None and name_value.strip()
        else ""
    )

    def reset():
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
        if not clean:
            return t("tiles.inference.error_name_required")
        return None

    def will_replace():
        if source == "import":
            return None  # duplicate imports suffix instead of replacing
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
            storage_key=clean if source == "model" else resolved_import_key,
            exists=exists if source == "model" else False,
            label=t("tiles.inference.pred_name_label"),
        )

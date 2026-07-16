"""New Prediction dialog for the Inference step."""

from typing import Callable

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.artifact_names import (
    default_pred_name,
    prediction_name_exists,
    sanitize_key,
)
from gui.widget.artifact_name_field import ArtifactNameField, use_artifact_name
from gui.widget.creation_dialog import CreationDialog


@solara.component
def PredictionFormDialog(project, open_, on_submit: Callable[[dict], None]):
    """Prediction form in the shared CreationDialog frame.

    on_submit(entry) receives {"model_key","dataset_key","name"}; the tile
    owns the job row and the inference worker.
    """
    p = project.value

    model_keys = sorted(p.models.keys()) if p and p.models else []
    selected_model, set_selected_model = solara.use_state("")
    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state("")

    # Name tracks the model+dataset default until the user edits it.
    name_value, on_name_input, reset_name = use_artifact_name(
        default_pred_name(selected_model, selected_dataset)
    )
    clean = sanitize_key(name_value)
    exists = prediction_name_exists(p, clean)

    def reset():
        set_selected_model("")
        set_selected_dataset("")
        reset_name()

    def validate():
        if p is None:
            return t("tiles.inference.error_no_project")
        if not selected_model or selected_model not in p.models:
            return t("tiles.inference.error_invalid_model")
        if not selected_dataset or selected_dataset not in p.datasets:
            return t("tiles.inference.error_invalid_dataset")
        if not clean:
            return t("tiles.inference.error_name_required")
        return None

    def will_replace():
        return clean if prediction_name_exists(p, clean) else None

    def launch():
        on_submit(
            {"model_key": selected_model, "dataset_key": selected_dataset, "name": clean}
        )

    with CreationDialog(
        open_=open_,
        title=t("tiles.inference.dialog_title"),
        create_label=t("tiles.inference.run_button"),
        validate=validate,
        will_replace=will_replace,
        launch=launch,
        on_close=reset,
        replace_message=lambda k: t("tiles.inference.confirm_overwrite_message", name=k),
    ):
        rv.Select(
            label=t("tiles.inference.model_select_label"), items=model_keys,
            v_model=selected_model, on_v_model=set_selected_model,
            dense=True, outlined=True,
            no_data_text=t("tiles.inference.model_select_no_data"),
            hint=t("tiles.inference.model_select_hint"), persistent_hint=True,
        )
        rv.Select(
            label=t("tiles.inference.dataset_select_label"), items=dataset_keys,
            v_model=selected_dataset, on_v_model=set_selected_dataset,
            dense=True, outlined=True,
            no_data_text=t("tiles.inference.dataset_select_no_data"),
            hint=t("tiles.inference.dataset_select_hint"), persistent_hint=True,
        )
        ArtifactNameField(
            value=name_value,
            on_input=on_name_input,
            storage_key=clean,
            exists=exists,
            label=t("tiles.inference.pred_name_label"),
        )

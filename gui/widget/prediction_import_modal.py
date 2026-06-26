"""Import-prediction-raster modal for the Inference step.

Mirrors :class:`gui.widget.variable_modal.VariableModal`: the modal owns its own
form state and does light required-field validation, then hands a resolved entry
dict to ``on_import``. The Inference tile keeps the project-level guard and
spawns the background copy, exactly as VariablesTile keeps ``on_add``.
"""

from typing import Callable

import reacton.ipyvuetify as rv
import solara
from pysepal.solara.components.inputs import FileInputComponent

from gui.i18n import t

# Raster file types accepted for a local prediction import.
_IMPORT_RASTER_EXTENSIONS = [".tif", ".tiff", ".vrt", ".nc"]


def _import_palette_items():
    return [
        {"text": t("widgets.prediction_import_modal.palette_far"), "value": "far"},
        {"text": t("widgets.prediction_import_modal.palette_stretch"), "value": "stretch"},
    ]


@solara.component
def PredictionImportModal(
    open_: solara.Reactive[bool],
    on_import: Callable[[dict], None],
    sepal_client=None,
):
    """Modal dialog for importing a prediction raster produced outside the app.

    Args:
        open_: Reactive bool controlling the dialog's visibility.
        on_import: Called with a resolved entry dict
            ``{"name", "path", "palette"}`` once required fields validate.
        sepal_client: SEPAL client backing the file picker.
    """
    name, set_name = solara.use_state("")
    file_path, set_file_path = solara.use_state("")
    palette, set_palette = solara.use_state("far")
    error, set_error = solara.use_state(None)

    def reset():
        set_name("")
        set_file_path("")
        set_palette("far")
        set_error(None)

    def on_cancel():
        reset()
        open_.set(False)

    def on_submit():
        if not file_path or not str(file_path).strip():
            set_error(t("widgets.prediction_import_modal.error_select_raster"))
            return
        if not name.strip():
            set_error(t("widgets.prediction_import_modal.error_enter_name"))
            return
        entry = {
            "name": name.strip(),
            "path": str(file_path),
            "palette": palette,
        }
        reset()
        open_.set(False)
        on_import(entry)

    with rv.Dialog(
        v_model=open_.value, on_v_model=open_.set, max_width="560px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(t("widgets.prediction_import_modal.title"))

            with rv.CardText():
                rv.TextField(
                    label=t("widgets.prediction_import_modal.label_name"),
                    v_model=name,
                    on_v_model=set_name,
                    dense=True,
                    outlined=True,
                    placeholder=t("widgets.prediction_import_modal.placeholder_name"),
                )
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
                if error:
                    rv.Alert(type_="error", dense=True, children=[error])

            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(t("common.cancel"), on_click=on_cancel, text=True, small=True)
                solara.Button(t("widgets.prediction_import_modal.btn_import"), on_click=on_submit, color="primary", small=True)

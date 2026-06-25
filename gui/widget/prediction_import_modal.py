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

# Raster file types accepted for a local prediction import.
_IMPORT_RASTER_EXTENSIONS = [".tif", ".tiff", ".vrt", ".nc"]

# Map display palette choices offered when importing a local prediction raster.
# Label -> palette key understood by prediction_import / prediction_map.
_IMPORT_PALETTES = {
    "FAR ramp (probability, pinned 1..65535)": "far",
    "Auto-stretch ramp to file range": "stretch",
}
_IMPORT_PALETTE_LABELS = list(_IMPORT_PALETTES.keys())


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
    palette_label, set_palette_label = solara.use_state(_IMPORT_PALETTE_LABELS[0])
    error, set_error = solara.use_state(None)

    def reset():
        set_name("")
        set_file_path("")
        set_palette_label(_IMPORT_PALETTE_LABELS[0])
        set_error(None)

    def on_cancel():
        reset()
        open_.set(False)

    def on_submit():
        if not file_path or not str(file_path).strip():
            set_error("Select a raster file to import.")
            return
        if not name.strip():
            set_error("Enter a name for the imported prediction.")
            return
        entry = {
            "name": name.strip(),
            "path": str(file_path),
            "palette": _IMPORT_PALETTES.get(palette_label, "far"),
        }
        reset()
        open_.set(False)
        on_import(entry)

    with rv.Dialog(
        v_model=open_.value, on_v_model=open_.set, max_width="560px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("Import prediction raster")

            with rv.CardText():
                rv.TextField(
                    label="Name",
                    v_model=name,
                    on_v_model=set_name,
                    dense=True,
                    outlined=True,
                    placeholder="e.g. qgis-export-2020",
                )
                FileInputComponent(
                    label="Select raster file",
                    value=file_path,
                    on_value=set_file_path,
                    sepal_client=sepal_client,
                    root="",
                    extensions=_IMPORT_RASTER_EXTENSIONS,
                    clearable=True,
                )
                rv.Select(
                    label="Map palette",
                    items=_IMPORT_PALETTE_LABELS,
                    v_model=palette_label,
                    on_v_model=set_palette_label,
                    dense=True,
                    outlined=True,
                )
                solara.Text(
                    "The raster must be spatially comparable to the truth "
                    "chosen in Step 8 to be evaluated."
                )
                if error:
                    rv.Alert(type_="error", dense=True, children=[error])

            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button("Cancel", on_click=on_cancel, text=True, small=True)
                solara.Button("Import", on_click=on_submit, color="primary", small=True)

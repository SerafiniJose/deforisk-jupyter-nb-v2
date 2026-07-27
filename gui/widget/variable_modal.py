"""Add / Edit Variable modal for the Variables step."""

from pathlib import Path
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara
from pysepal.solara.components.inputs import FileInputComponent

from gui.i18n import t
from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE
from gui.widget.artifact_name_field import ArtifactNameField
from spatialrisk.variables.models import (
    DataType,
    RasterizationMethod,
    RasterType,
)

VAR_TYPES = ["LocalRasterVar", "GEEVar", "LocalVectorVar"]
SOURCES = ["predefined", "custom"]

# Stored discriminator value -> i18n key for its user-facing display label.
# The internal values above are persisted and branched on throughout the app,
# so we only translate them for display and never change the stored strings.
_SOURCE_LABEL_KEYS = {
    "predefined": "vars.modal.source_predefined",
    "custom": "vars.modal.source_user",
}
_TYPE_LABEL_KEYS = {
    "LocalRasterVar": "vars.modal.type_local_raster",
    "GEEVar": "vars.modal.type_gee",
    "LocalVectorVar": "vars.modal.type_local_vector",
}

_RASTER_EXTENSIONS = [".tif", ".tiff", ".vrt", ".nc"]
_VECTOR_EXTENSIONS = [".geojson", ".gpkg", ".shp", ".json"]


def _predefined_items():
    """Build dropdown items at render time so labels respect the active locale."""
    return [
        {"text": t(meta["label_key"]), "value": key}
        for key, meta in PREDEFINED_CATALOGUE.items()
    ]


def _source_items():
    """Source dropdown items — friendly labels over the stored values."""
    return [{"text": t(_SOURCE_LABEL_KEYS[s]), "value": s} for s in SOURCES]


def _type_items():
    """Variable-type dropdown items — friendly labels over the class names."""
    return [{"text": t(_TYPE_LABEL_KEYS[v]), "value": v} for v in VAR_TYPES]


def _type_display(var_type: str) -> str:
    """Friendly label for a variable-type discriminator (falls back to raw)."""
    key = _TYPE_LABEL_KEYS.get(var_type)
    return t(key) if key else var_type


@solara.component
def VariableModal(
    open_: solara.Reactive[bool],
    on_add: Callable,
    on_save: Optional[Callable[[str, dict], None]] = None,
    editing_key: Optional[str] = None,
    initial_entry: Optional[dict] = None,
    sepal_client=None,
    existing_keys=frozenset(),
):
    """Modal dialog for adding or editing a variable."""
    # --- state ---
    source, set_source = solara.use_state(SOURCES[0])
    predefined_key, set_predefined_key = solara.use_state("")
    var_type, set_var_type = solara.use_state(VAR_TYPES[0])
    name, set_name = solara.use_state("")
    year, set_year = solara.use_state("")
    file_path, set_file_path = solara.use_state("")
    asset_id, set_asset_id = solara.use_state("")
    scale, set_scale = solara.use_state("")
    raster_type, set_raster_type = solara.use_state(RasterType.continuous.value)
    rasterization_method, set_rasterization_method = solara.use_state(
        RasterizationMethod.binary.value
    )
    error, set_error = solara.use_state(None)

    is_edit = editing_key is not None

    # Derived: catalogue entry for the selected predefined variable
    cat = PREDEFINED_CATALOGUE.get(predefined_key) if predefined_key else None

    # Storage key the entry will land under (mirrors variables_tile.entry_key).
    _key_name = predefined_key if source == "predefined" else name.strip()
    storage_key = (
        f"{_key_name}_{year}"
        if (_key_name and year and str(year).strip())
        else _key_name
    )
    key_exists = (
        bool(storage_key)
        and storage_key in existing_keys
        and storage_key != editing_key
    )

    def reset():
        set_source(SOURCES[0])
        set_predefined_key("")
        set_var_type(VAR_TYPES[0])
        set_name("")
        set_year("")
        set_file_path("")
        set_asset_id("")
        set_scale("")
        set_raster_type(RasterType.continuous.value)
        set_rasterization_method(RasterizationMethod.binary.value)
        set_error(None)

    def prefill_from_initial():
        if not open_.value or initial_entry is None:
            return
        set_source(initial_entry.get("source", "custom"))
        set_predefined_key(initial_entry.get("predefined_key", ""))
        set_var_type(initial_entry.get("type", VAR_TYPES[0]))
        set_name(initial_entry.get("name", ""))
        set_year(str(initial_entry.get("year") or ""))
        set_file_path(initial_entry.get("path", ""))
        set_asset_id(initial_entry.get("asset_id", ""))
        set_scale(initial_entry.get("scale", ""))
        set_raster_type(initial_entry.get("raster_type", RasterType.continuous.value))
        set_rasterization_method(
            initial_entry.get("rasterization_method", RasterizationMethod.binary.value)
        )
        set_error(None)

    solara.use_effect(prefill_from_initial, [open_.value])

    def on_cancel():
        reset()
        open_.set(False)

    def _submit_predefined():
        if not predefined_key:
            set_error(t("vars.modal.error_no_predefined_selected"))
            return
        cat_entry = PREDEFINED_CATALOGUE[predefined_key]
        yr = int(year) if year and str(year).strip() else None
        if cat_entry["temporal"] and yr is None:
            set_error(t("vars.modal.error_year_required"))
            return
        entry = {
            "source": "predefined",
            "type": "GEEVar",
            "name": predefined_key,
            "year": yr,
            "data_type": DataType.raster,
            "raster_type": RasterType(cat_entry["raster_type"]),
            "predefined_key": predefined_key,
        }
        reset()
        open_.set(False)
        if is_edit and on_save is not None:
            on_save(editing_key, entry)
        else:
            on_add(entry)

    def _submit_custom():
        if not name.strip():
            set_error(t("vars.modal.error_name_required"))
            return
        yr = int(year) if year and str(year).strip() else None
        entry = {
            "source": "custom",
            "type": var_type,
            "name": name.strip(),
            "year": yr,
        }
        if var_type == "LocalRasterVar":
            entry["path"] = (
                Path(file_path.strip())
                if file_path.strip()
                else Path(f"/tmp/{name.strip()}.tif")
            )
            entry["raster_type"] = RasterType(raster_type)
            entry["data_type"] = DataType.raster
        elif var_type == "GEEVar":
            entry["path"] = (
                asset_id.strip()
                if asset_id.strip()
                else f"projects/dummy/assets/{name.strip()}"
            )
            entry["default_scale"] = float(scale) if scale.strip() else None
            entry["data_type"] = DataType.raster
        elif var_type == "LocalVectorVar":
            entry["path"] = (
                Path(file_path.strip())
                if file_path.strip()
                else Path(f"/tmp/{name.strip()}.geojson")
            )
            entry["rasterization_method"] = RasterizationMethod(rasterization_method)
            entry["data_type"] = DataType.vector
        reset()
        open_.set(False)
        if is_edit and on_save is not None:
            on_save(editing_key, entry)
        else:
            on_add(entry)

    def on_submit():
        if source == "predefined":
            _submit_predefined()
        else:
            _submit_custom()

    title = t("vars.modal.title_edit") if is_edit else t("vars.modal.title_add")
    submit_label = (
        t("vars.modal.submit_save") if is_edit else t("vars.modal.submit_add")
    )

    with rv.Dialog(
        v_model=open_.value, on_v_model=open_.set, max_width="560px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(title)

            with rv.CardText():
                # ---- Source toggle (first field) ----
                rv.Select(
                    label=t("vars.modal.source_label"),
                    items=_source_items(),
                    v_model=source,
                    on_v_model=set_source,
                    dense=True,
                    outlined=True,
                    hint=t("vars.modal.source_hint"),
                    persistent_hint=True,
                )

                if source == "predefined":
                    _render_predefined_fields(
                        predefined_key,
                        set_predefined_key,
                        year,
                        set_year,
                        cat,
                        storage_key,
                        key_exists,
                    )
                else:
                    _render_custom_fields(
                        var_type,
                        set_var_type,
                        name,
                        set_name,
                        year,
                        set_year,
                        file_path,
                        set_file_path,
                        asset_id,
                        set_asset_id,
                        scale,
                        set_scale,
                        raster_type,
                        set_raster_type,
                        rasterization_method,
                        set_rasterization_method,
                        sepal_client=sepal_client,
                        storage_key=storage_key,
                        key_exists=key_exists,
                    )

                if error:
                    rv.Alert(type_="error", dense=True, children=[error])

            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    t("common.cancel"), on_click=on_cancel, text=True, small=True
                )
                solara.Button(
                    submit_label,
                    on_click=on_submit,
                    color="primary",
                    small=True,
                    icon_name="mdi-plus",
                )


# ---------------------------------------------------------------------------
# Sub-renderers
# ---------------------------------------------------------------------------


def _render_predefined_fields(
    predefined_key,
    set_predefined_key,
    year,
    set_year,
    cat,
    storage_key,
    key_exists,
):
    """Fields shown when source == 'predefined'."""
    rv.Select(
        label=t("vars.modal.predefined_variable_label"),
        items=_predefined_items(),
        v_model=predefined_key,
        on_v_model=set_predefined_key,
        dense=True,
        outlined=True,
    )

    if cat:
        # Data-source blurb: where the layer comes from and how it is generated
        # (catalogue link included). Rendered inline so it updates per selection.
        if cat.get("description_key"):
            with solara.Div(
                style="font-size:0.85rem;padding:0 4px 4px 4px;",
                classes=["text--secondary"],
            ):
                solara.Markdown(t(cat["description_key"]))

        # Year — selectable list for temporal variables
        if cat["temporal"]:
            available_years = cat.get("years", [])
            rv.Select(
                label=t("vars.modal.predefined_year_label"),
                items=available_years,
                v_model=int(year) if year and str(year).strip() else None,
                on_v_model=lambda v: set_year(str(v) if v else ""),
                dense=True,
                outlined=True,
            )

        # Read-only info fields
        rv.TextField(
            label=t("vars.modal.predefined_var_type_label"),
            v_model=_type_display(cat.get("var_type", "GEEVar")),
            dense=True,
            outlined=True,
            disabled=True,
        )
        rv.TextField(
            label=t("vars.modal.predefined_raster_type_label"),
            v_model=cat["raster_type"],
            dense=True,
            outlined=True,
            disabled=True,
        )

    if storage_key:
        solara.Text(
            t(
                "widgets.artifact_name.exists_warning"
                if key_exists
                else "widgets.artifact_name.saved_as",
                key=storage_key,
            ),
            style="font-size:0.8rem;padding:0 4px 4px;",
            classes=["text--secondary"],
        )


def _render_custom_fields(
    var_type,
    set_var_type,
    name,
    set_name,
    year,
    set_year,
    file_path,
    set_file_path,
    asset_id,
    set_asset_id,
    scale,
    set_scale,
    raster_type,
    set_raster_type,
    rasterization_method,
    set_rasterization_method,
    sepal_client,
    storage_key,
    key_exists,
):
    """Fields shown when source == 'custom'."""
    ArtifactNameField(
        value=name,
        on_input=set_name,
        storage_key=storage_key,
        exists=key_exists,
        label=t("vars.modal.custom_name_label"),
    )
    rv.Select(
        label=t("vars.modal.custom_type_label"),
        items=_type_items(),
        v_model=var_type,
        on_v_model=set_var_type,
        dense=True,
        outlined=True,
        hint=t("vars.modal.custom_type_hint"),
        persistent_hint=True,
    )
    rv.TextField(
        label=t("vars.modal.custom_year_label"),
        v_model=year,
        on_v_model=set_year,
        dense=True,
        outlined=True,
        type="number",
        hint=t("vars.modal.custom_year_hint"),
        persistent_hint=True,
    )
    if var_type in ("LocalRasterVar", "LocalVectorVar"):
        FileInputComponent(
            label=t("vars.modal.custom_file_label"),
            value=file_path,
            on_value=set_file_path,
            sepal_client=sepal_client,
            root="",
            extensions=(
                _RASTER_EXTENSIONS
                if var_type == "LocalRasterVar"
                else _VECTOR_EXTENSIONS
            ),
            clearable=True,
        )
    if var_type == "GEEVar":
        rv.TextField(
            label=t("vars.modal.custom_asset_id_label"),
            v_model=asset_id,
            on_v_model=set_asset_id,
            dense=True,
            outlined=True,
            placeholder=t("vars.modal.custom_asset_id_placeholder"),
        )
        rv.TextField(
            label=t("vars.modal.custom_scale_label"),
            v_model=scale,
            on_v_model=set_scale,
            dense=True,
            outlined=True,
            type="number",
            hint=t("vars.modal.custom_scale_hint"),
            persistent_hint=True,
        )
    if var_type in ("LocalRasterVar", "GEEVar"):
        rv.Select(
            label=t("vars.modal.custom_raster_type_label"),
            items=[r.value for r in RasterType],
            v_model=raster_type,
            on_v_model=set_raster_type,
            dense=True,
            outlined=True,
            hint=t("vars.modal.custom_raster_type_hint"),
            persistent_hint=True,
        )
    if var_type == "LocalVectorVar":
        rv.Select(
            label=t("vars.modal.custom_rasterization_method_label"),
            items=[r.value for r in RasterizationMethod],
            v_model=rasterization_method,
            on_v_model=set_rasterization_method,
            dense=True,
            outlined=True,
            hint=t("vars.modal.custom_rasterization_method_hint"),
            persistent_hint=True,
        )

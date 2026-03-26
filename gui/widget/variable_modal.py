"""Add / Edit Variable modal for the Variables step."""

from pathlib import Path
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE
from spatialrisk.variables.models import (
    DataType,
    PostProcessing,
    RasterType,
    RasterizationMethod,
)

VAR_TYPES = ["LocalRasterVar", "GEEVar", "LocalVectorVar"]
SOURCES = ["predefined", "custom"]

# Build dropdown items: [{"text": label, "value": key}, ...]
_PREDEFINED_ITEMS = [
    {"text": v["label"], "value": k} for k, v in PREDEFINED_CATALOGUE.items()
]


@solara.component
def VariableModal(
    open_: solara.Reactive[bool],
    on_add: Callable,
    on_save: Optional[Callable[[str, dict], None]] = None,
    editing_key: Optional[str] = None,
    initial_entry: Optional[dict] = None,
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
    post_processing, set_post_processing = solara.use_state([])
    is_base, set_is_base = solara.use_state(False)
    error, set_error = solara.use_state(None)

    is_edit = editing_key is not None

    # Derived: catalogue entry for the selected predefined variable
    cat = PREDEFINED_CATALOGUE.get(predefined_key) if predefined_key else None

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
        set_post_processing([])
        set_is_base(False)
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
        set_post_processing(initial_entry.get("post_processing", []))
        set_is_base(initial_entry.get("is_base", False))
        set_error(None)

    solara.use_effect(prefill_from_initial, [open_.value])

    def on_cancel():
        reset()
        open_.set(False)

    def _submit_predefined():
        if not predefined_key:
            set_error("Select a variable from the list.")
            return
        cat_entry = PREDEFINED_CATALOGUE[predefined_key]
        yr = int(year) if year and str(year).strip() else None
        if cat_entry["temporal"] and yr is None:
            set_error("Year is required for this variable.")
            return
        entry = {
            "source": "predefined",
            "type": "GEEVar",
            "name": predefined_key,
            "year": yr,
            "is_base": is_base,
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
            set_error("Variable name is required.")
            return
        yr = int(year) if year and str(year).strip() else None
        pp = [PostProcessing(p) for p in post_processing]
        entry = {
            "source": "custom",
            "type": var_type,
            "name": name.strip(),
            "year": yr,
            "is_base": is_base and var_type != "LocalVectorVar",
            "post_processing": pp,
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

    title = "Edit Variable" if is_edit else "Add Variable"
    submit_label = "Save" if is_edit else "Add"

    with rv.Dialog(
        v_model=open_.value, on_v_model=open_.set, max_width="560px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(title)

            with rv.CardText():
                # ---- Source toggle (first field) ----
                rv.Select(
                    label="Source",
                    items=SOURCES,
                    v_model=source,
                    on_v_model=set_source,
                    dense=True,
                    outlined=True,
                )

                if source == "predefined":
                    _render_predefined_fields(
                        predefined_key, set_predefined_key,
                        year, set_year,
                        is_base, set_is_base,
                        cat,
                    )
                else:
                    _render_custom_fields(
                        var_type, set_var_type,
                        name, set_name,
                        year, set_year,
                        file_path, set_file_path,
                        asset_id, set_asset_id,
                        scale, set_scale,
                        raster_type, set_raster_type,
                        rasterization_method, set_rasterization_method,
                        post_processing, set_post_processing,
                        is_base, set_is_base,
                    )

                if error:
                    rv.Alert(type_="error", dense=True, children=[error])

            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button("Cancel", on_click=on_cancel, text=True)
                solara.Button(submit_label, on_click=on_submit, color="primary")


# ---------------------------------------------------------------------------
# Sub-renderers
# ---------------------------------------------------------------------------


def _render_predefined_fields(
    predefined_key, set_predefined_key,
    year, set_year,
    is_base, set_is_base,
    cat,
):
    """Fields shown when source == 'predefined'."""
    rv.Select(
        label="Variable",
        items=_PREDEFINED_ITEMS,
        v_model=predefined_key,
        on_v_model=set_predefined_key,
        dense=True,
        outlined=True,
    )

    if cat:
        # Year — selectable list for temporal variables
        if cat["temporal"]:
            available_years = cat.get("years", [])
            rv.Select(
                label="Year",
                items=available_years,
                v_model=int(year) if year and str(year).strip() else None,
                on_v_model=lambda v: set_year(str(v) if v else ""),
                dense=True,
                outlined=True,
            )

        # Read-only info fields
        rv.TextField(
            label="Variable type",
            v_model=cat.get("var_type", "GEEVar"),
            dense=True,
            outlined=True,
            disabled=True,
        )
        rv.TextField(
            label="Raster type",
            v_model=cat["raster_type"],
            dense=True,
            outlined=True,
            disabled=True,
        )

        # Base raster switch
        rv.Switch(
            label="Set as base raster (used for reprojection alignment)",
            v_model=is_base,
            on_v_model=set_is_base,
        )


def _render_custom_fields(
    var_type, set_var_type,
    name, set_name,
    year, set_year,
    file_path, set_file_path,
    asset_id, set_asset_id,
    scale, set_scale,
    raster_type, set_raster_type,
    rasterization_method, set_rasterization_method,
    post_processing, set_post_processing,
    is_base, set_is_base,
):
    """Fields shown when source == 'custom' (original behaviour)."""
    rv.TextField(
        label="Name",
        v_model=name,
        on_v_model=set_name,
        dense=True,
        outlined=True,
    )
    rv.Select(
        label="Variable type",
        items=VAR_TYPES,
        v_model=var_type,
        on_v_model=set_var_type,
        dense=True,
        outlined=True,
    )
    rv.TextField(
        label="Year (optional)",
        v_model=year,
        on_v_model=set_year,
        dense=True,
        outlined=True,
        type="number",
    )
    if var_type in ("LocalRasterVar", "LocalVectorVar"):
        rv.TextField(
            label="File path",
            v_model=file_path,
            on_v_model=set_file_path,
            dense=True,
            outlined=True,
            placeholder="/path/to/file.tif",
        )
    if var_type == "GEEVar":
        rv.TextField(
            label="GEE asset ID",
            v_model=asset_id,
            on_v_model=set_asset_id,
            dense=True,
            outlined=True,
            placeholder="projects/your-project/assets/name",
        )
        rv.TextField(
            label="Scale (m, optional)",
            v_model=scale,
            on_v_model=set_scale,
            dense=True,
            outlined=True,
            type="number",
        )
    if var_type in ("LocalRasterVar", "GEEVar"):
        rv.Select(
            label="Raster type",
            items=[r.value for r in RasterType],
            v_model=raster_type,
            on_v_model=set_raster_type,
            dense=True,
            outlined=True,
        )
        rv.Select(
            label="Post-processing (optional)",
            items=[p.value for p in PostProcessing],
            v_model=post_processing,
            on_v_model=set_post_processing,
            multiple=True,
            dense=True,
            outlined=True,
        )
    if var_type == "LocalVectorVar":
        rv.Select(
            label="Rasterization method",
            items=[r.value for r in RasterizationMethod],
            v_model=rasterization_method,
            on_v_model=set_rasterization_method,
            dense=True,
            outlined=True,
        )
    if var_type != "LocalVectorVar":
        rv.Switch(
            label="Set as base raster (used for reprojection alignment)",
            v_model=is_base,
            on_v_model=set_is_base,
        )

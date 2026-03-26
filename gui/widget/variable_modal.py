"""Add Variable modal for the Variables step."""

from pathlib import Path
from typing import Callable

import reacton.ipyvuetify as rv
import solara

from spatialrisk.variables.models import (
    DataType,
    PostProcessing,
    RasterType,
    RasterizationMethod,
)

VAR_TYPES = ["LocalRasterVar", "GEEVar", "LocalVectorVar"]


@solara.component
def VariableModal(open_: solara.Reactive[bool], on_add: Callable):
    """Modal dialog for adding a new variable to the project."""
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

    def reset():
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

    def on_cancel():
        reset()
        open_.set(False)

    def on_submit():
        if not name.strip():
            set_error("Variable name is required.")
            return

        yr = int(year) if year.strip() else None
        pp = [PostProcessing(p) for p in post_processing]

        entry = {
            "type": var_type,
            "name": name.strip(),
            "year": yr,
            "is_base": is_base and var_type != "LocalVectorVar",
            "post_processing": pp,
        }

        if var_type == "LocalRasterVar":
            if not file_path.strip():
                set_error("File path is required.")
                return
            entry["path"] = Path(file_path.strip())
            entry["raster_type"] = RasterType(raster_type)
            entry["data_type"] = DataType.raster

        elif var_type == "GEEVar":
            if not asset_id.strip():
                set_error("GEE asset ID is required.")
                return
            entry["path"] = asset_id.strip()
            entry["default_scale"] = float(scale) if scale.strip() else None
            entry["data_type"] = DataType.raster

        elif var_type == "LocalVectorVar":
            if not file_path.strip():
                set_error("File path is required.")
                return
            entry["path"] = Path(file_path.strip())
            entry["rasterization_method"] = RasterizationMethod(rasterization_method)
            entry["data_type"] = DataType.vector

        reset()
        open_.set(False)
        on_add(entry)

    with rv.Dialog(v_model=open_.value, on_v_model=open_.set, max_width="560px", eager=True):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("Add Variable")

            with rv.CardText():
                rv.Select(
                    label="Variable type",
                    items=VAR_TYPES,
                    v_model=var_type,
                    on_v_model=set_var_type,
                    dense=True,
                    outlined=True,
                )
                rv.TextField(
                    label="Name",
                    v_model=name,
                    on_v_model=set_name,
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
                if error:
                    rv.Alert(type_="error", dense=True, children=[error])

            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                rv.Btn(children=["Cancel"], text=True, on_click=lambda *_: on_cancel())
                rv.Btn(children=["Add"], color="primary", on_click=lambda *_: on_submit())

"""Spatial Risk — Solara GUI entry point.

Run locally:
    ./run_solara.sh gui/solara_app.py 8910
"""

import logging
from datetime import datetime

import reacton.ipyvuetify as rv
import solara
from solara.lab.components.theming import theme

from pysepal import mapping as sm
from pysepal.logger import setup_logging
from pysepal.sepalwidgets.vue_app import MapApp, ThemeToggle
from pysepal.solara import (
    get_current_gee_interface,
    setup_sessions,
    setup_solara_server,
    setup_theme_colors,
    with_sepal_sessions,
)

from spatialrisk.project import Project, DATA_DIR
from gui.store.state_manager import app_state
from gui.scripts.project_io import (
    list_project_infos,
    load_project,
    save_project,
)
from gui.scripts.project_ui_helpers import (
    compute_app_title,
    format_last_saved,
    format_relative,
    overwrite_needed,
    validate_project_name,
)
from gui.tile.aoi_tile import AoiTile
from gui.tile.dataset_tile import DatasetTile
from gui.tile.variables_tile import VariablesTile
from gui.tile.process_tile import ProcessTile
from gui.tile.train_tile import TrainTile
from gui.tile.inference_tile import InferenceTile
from gui.tile.evaluation_tile import EvaluationTile
from gui.widget.notification_area import NotificationArea

logger = setup_logging(logger_name="spatial_risk")
logger.setLevel(logging.DEBUG)
logger.debug("Spatial Risk app initialized")
logger.debug("Solara version: %s", solara.__version__)

setup_solara_server(extra_asset_locations=[])


@solara.lab.on_kernel_start
def on_kernel_start():
    return setup_sessions()


@solara.component
def ProjectPanel():
    """Current-project status + New / Load / Save controls (left drawer)."""
    p = app_state.project.value
    dirty = app_state.project_dirty.value
    last_saved = app_state.last_saved.value

    # Dialog / transient UI state
    load_open, set_load_open = solara.use_state(False)
    new_open, set_new_open = solara.use_state(False)
    discard_open, set_discard_open = solara.use_state(False)
    overwrite_open, set_overwrite_open = solara.use_state(False)

    infos, set_infos = solara.use_state([])          # list[ProjectInfo] for load
    selected, set_selected = solara.use_state(None)  # selected project name
    load_error, set_load_error = solara.use_state(None)
    load_busy, set_load_busy = solara.use_state(False)

    new_name, set_new_name = solara.use_state("")

    def existing_names() -> list:
        return [i.name for i in list_project_infos(DATA_DIR)]

    # ---- Load -----------------------------------------------------------
    def open_load():
        set_load_error(None)
        set_selected(None)
        try:
            set_infos(list_project_infos(DATA_DIR))
        except Exception as exc:  # pragma: no cover - defensive
            set_infos([])
            set_load_error(str(exc))
        set_load_open(True)

    def do_load():
        if not selected:
            return
        set_load_busy(True)
        set_load_error(None)
        try:
            loaded = load_project(selected)
            when = next(
                (i.modified for i in infos if i.name == selected), None
            )
            app_state.load_project_state(loaded, when)
            app_state.status_message.set(f"Project '{selected}' loaded.")
            app_state.error_message.set(None)
            set_load_open(False)
        except Exception as exc:
            set_load_error(str(exc))
        finally:
            set_load_busy(False)

    # ---- New ------------------------------------------------------------
    def open_new():
        if dirty and p is not None:
            set_discard_open(True)
        else:
            _open_new_dialog()

    def _open_new_dialog():
        set_discard_open(False)
        set_new_name("")
        set_new_open(True)

    def do_create():
        validation = validate_project_name(new_name, existing_names())
        if not validation.valid:
            return  # Create button is disabled in this state; no-op guard
        app_state.new_project_state(Project(project_name=validation.cleaned))
        app_state.status_message.set(f"Project '{validation.cleaned}' created.")
        set_new_open(False)

    def load_instead():
        name = validate_project_name(new_name, existing_names()).cleaned
        set_new_open(False)
        set_selected(name)
        try:
            set_infos(list_project_infos(DATA_DIR))
        except Exception:
            set_infos([])
        set_load_open(True)

    # ---- Save -----------------------------------------------------------
    def do_save():
        if p is None:
            app_state.error_message.set(
                "No project to save. Create or load a project first."
            )
            return
        if overwrite_needed(p.project_name, last_saved, existing_names()):
            set_overwrite_open(True)
            return
        _really_save()

    def _really_save():
        set_overwrite_open(False)
        try:
            path = save_project(p)
            app_state.mark_saved(datetime.now())
            note = ""
            if not p.raw_variables:
                note = " (note: no variables yet)"
            elif p.base_raster is None:
                note = " (note: no base raster set yet)"
            app_state.status_message.set(f"Saved to {path}{note}")
            app_state.error_message.set(None)
        except Exception as exc:
            app_state.error_message.set(str(exc))

    # ---- Status block ---------------------------------------------------
    with solara.Column(style="gap: 8px; padding: 8px;"):
        if p is None:
            solara.Text(
                "No project open — select an AOI or click New to start.",
                style="color: var(--md-grey-500); font-style: italic;",
            )
        else:
            with solara.Row(style="gap: 8px; align-items: center;"):
                solara.Text(p.project_name, style="font-weight: 600;")
                rv.Chip(
                    children=["unsaved" if dirty else "saved"],
                    color="amber" if dirty else "green",
                    text_color="white",
                    x_small=True,
                )
            solara.Text(
                f"{len(p.raw_variables)} raw · "
                f"{len(p.processed_variables)} processed · "
                f"{len(p.models)} models",
                style="font-size: 12px; color: var(--md-grey-500);",
            )
            solara.Text(
                format_last_saved(last_saved, datetime.now()),
                style="font-size: 12px; color: var(--md-grey-500);",
            )

        with solara.Row(style="gap: 8px;"):
            solara.Button(
                "New",
                icon_name="mdi-plus",
                color="primary",
                outlined=True,
                small=True,
                on_click=open_new,
            )
            solara.Button(
                "Load",
                icon_name="mdi-folder-open-outline",
                color="primary",
                outlined=True,
                small=True,
                on_click=open_load,
            )
            solara.Button(
                "Save",
                icon_name="mdi-content-save-outline",
                color="primary",
                outlined=True,
                small=True,
                disabled=p is None,
                on_click=do_save,
            )

    # ---- New dialog -----------------------------------------------------
    validation = validate_project_name(new_name, existing_names()) if new_open else None
    with rv.Dialog(
        v_model=new_open, on_v_model=set_new_open, max_width="400px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("New Project")
            with rv.CardText():
                rv.TextField(
                    label="Project name",
                    v_model=new_name,
                    on_v_model=set_new_name,
                    dense=True,
                    outlined=True,
                    autofocus=True,
                )
                if validation and new_name and not validation.valid:
                    solara.Error(validation.error)
                if validation and validation.valid and validation.exists:
                    solara.Warning(
                        f"A project named '{validation.cleaned}' already exists "
                        "— saving later will overwrite it."
                    )
                    solara.Button(
                        "Load it instead",
                        text=True,
                        small=True,
                        on_click=load_instead,
                    )
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    "Cancel", on_click=lambda: set_new_open(False), text=True, small=True
                )
                solara.Button(
                    "Create",
                    on_click=do_create,
                    color="primary",
                    small=True,
                    disabled=not (validation and validation.valid),
                )

    # ---- Discard-unsaved confirm (New while dirty) ----------------------
    with rv.Dialog(
        v_model=discard_open, on_v_model=set_discard_open, max_width="380px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("Discard unsaved changes?")
            with rv.CardText():
                solara.Text(
                    f"Project '{p.project_name}' has unsaved changes. "
                    "Starting a new project will discard them."
                    if p is not None
                    else "Discard unsaved changes?"
                )
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    "Cancel", on_click=lambda: set_discard_open(False), text=True, small=True
                )
                solara.Button(
                    "Discard & New",
                    on_click=_open_new_dialog,
                    color="error",
                    small=True,
                )

    # ---- Overwrite confirm (Save over an existing project) --------------
    with rv.Dialog(
        v_model=overwrite_open, on_v_model=set_overwrite_open, max_width="380px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("Overwrite existing project?")
            with rv.CardText():
                solara.Text(
                    f"A saved project named '{p.project_name}' already exists. "
                    "Overwrite it?"
                    if p is not None
                    else "Overwrite existing project?"
                )
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    "Cancel",
                    on_click=lambda: set_overwrite_open(False),
                    text=True,
                    small=True,
                )
                solara.Button(
                    "Overwrite", on_click=_really_save, color="error", small=True
                )

    # ---- Load dialog ----------------------------------------------------
    with rv.Dialog(
        v_model=load_open, on_v_model=set_load_open, max_width="440px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("Load Project")
            with rv.CardText():
                if not infos:
                    solara.Info("No saved projects found.")
                else:
                    now = datetime.now()
                    with rv.List(dense=True):
                        with rv.ListItemGroup(
                            v_model=selected, on_v_model=set_selected
                        ):
                            for info in infos:
                                with rv.ListItem(
                                    value=info.name, disabled=not info.readable
                                ):
                                    with rv.ListItemContent():
                                        rv.ListItemTitle(children=[info.name])
                                        if info.readable:
                                            sub = (
                                                f"{info.raw_count} raw · "
                                                f"{info.processed_count} processed · "
                                                f"modified {format_relative(info.modified, now)}"
                                            )
                                        else:
                                            sub = info.error or "unreadable project file"
                                        rv.ListItemSubtitle(children=[sub])
                if load_busy:
                    rv.ProgressLinear(indeterminate=True)
                if load_error:
                    solara.Error(load_error)
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    "Cancel", on_click=lambda: set_load_open(False), text=True, small=True
                )
                solara.Button(
                    "Load",
                    on_click=do_load,
                    color="primary",
                    small=True,
                    disabled=not selected or load_busy,
                )


@solara.component
def WorkflowTabs(map_, gee_interface):
    """Three-tab workflow panel rendered in the right side panel."""
    active_tab, set_active_tab = solara.use_state(0)

    aoi_complete = app_state.aoi_result.value is not None
    p = app_state.project.value
    has_raw = p is not None and bool(p.raw_variables)
    has_processed = p is not None and bool(p.processed_variables)

    with rv.Tabs(v_model=active_tab, on_v_model=set_active_tab, grow=True):
        rv.Tab(children=["Area of Interest"])
        rv.Tab(children=["Variables"], disabled=not aoi_complete)
        rv.Tab(children=["Process"], disabled=not has_raw)
        rv.Tab(children=["Dataset"], disabled=not has_processed)
        rv.Tab(children=["Train"])
        rv.Tab(children=["Inference"])
        rv.Tab(children=["Evaluation"])

    with rv.TabsItems(v_model=active_tab):
        with rv.TabItem():
            AoiTile(
                map_=map_,
                gee_interface=gee_interface,
                aoi_result=app_state.aoi_result,
                loading=app_state.loading,
            )
        with rv.TabItem():
            VariablesTile(
                project=app_state.project,
                processing=app_state.processing,
                process_error=app_state.process_error,
                map_=map_,
            )
        with rv.TabItem():
            ProcessTile(
                project=app_state.project,
                processing=app_state.processing,
                process_error=app_state.process_error,
            )
        with rv.TabItem():
            DatasetTile(project=app_state.project)

        with rv.TabItem():
            TrainTile(project=app_state.project)

        with rv.TabItem():
            InferenceTile(project=app_state.project)

        with rv.TabItem():
            EvaluationTile(project=app_state.project)

    NotificationArea(
        active_tab=active_tab,
        aoi_result=app_state.aoi_result.value,
        project=app_state.project.value,
        process_error=app_state.process_error.value,
        status_message=app_state.status_message.value,
        error_message=app_state.error_message.value,
    )


@solara.component
@with_sepal_sessions(module_name="spatial_risk")
def Page():
    """Main application page using MapApp layout.

    Left drawer  — Project (load / save dialog)
    Center       — SepalMap (fullscreen, always visible)
    Right panel  — Workflow tabs: AOI / Variables / Dataset
    """
    setup_theme_colors()

    gee_interface = get_current_gee_interface()
    theme_toggle = solara.use_memo(lambda: ThemeToggle(), [])

    def _observe_theme():
        handler = lambda e: setattr(theme, "dark", e["new"])
        theme_toggle.observe(handler, "dark")
        return lambda: theme_toggle.unobserve(handler, "dark")

    solara.use_effect(_observe_theme, [])

    sepal_map = solara.use_memo(
        lambda: sm.SepalMap(
            zoom=3,
            center=[0, 0],
            gee=True,
            gee_interface=gee_interface,
            theme_toggle=theme_toggle,
            fullscreen=True,
        ),
        [id(gee_interface)],
    )

    def sync_project_from_aoi():
        aoi = app_state.aoi_result.value
        if aoi is None or app_state.project.value is not None:
            return
        from spatialrisk.project import Project

        app_state.project.set(Project(project_name=aoi.name))

    solara.use_effect(sync_project_from_aoi, [app_state.aoi_result.value])

    def _seed_test_aoi():
        import os
        if os.getenv("SOLARA_TEST", "false").lower() != "true":
            return
        if app_state.aoi_result.value is not None:
            return
        import geopandas as gpd
        from shapely.geometry import box
        from pysepal.solara.components.aoi import AoiResult
        gdf = gpd.GeoDataFrame(
            {"name": ["San Marino"]},
            geometry=[box(12.403, 43.893, 12.517, 43.993)],
            crs="EPSG:4326",
        )
        logger.debug("SOLARA_TEST: seeding AOI with San Marino")
        app_state.aoi_result.set(AoiResult(method="DRAW", name="San Marino", gdf=gdf))

    # Test AOI seeding disabled for now — start from an empty project.
    # (SOLARA_TEST stays on; re-enable by uncommenting the line below.)
    # solara.use_effect(_seed_test_aoi, [])

    def _seed_test_variables():
        import os
        if os.getenv("SOLARA_TEST", "false").lower() != "true":
            return
        p = app_state.project.value
        aoi_result = app_state.aoi_result.value
        if p is None or p.raw_variables or aoi_result is None:
            return
        from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE, get_aoi_ee_feature
        from spatialrisk.variables.gee_var import GEEVar
        from spatialrisk.variables.models import DataType, RasterType

        GEEVar.model_rebuild()
        aoi_ee = get_aoi_ee_feature(aoi_result.gdf)

        # Non-temporal variables
        for name in ["altitude", "slope", "protected_area", "roads", "rivers", "subj"]:
            cat = PREDEFINED_CATALOGUE[name]
            image = cat["get_image"](aoi_ee)
            p.raw_variables[name] = GEEVar(
                name=name,
                data_type=DataType.raster,
                raster_type=RasterType(cat["raster_type"]),
                gee_images=[image],
                aoi=aoi_ee,
                project=p,
            )

        # Temporal variables
        for name, years in [("forest_gfc", [2015, 2020, 2024]), ("towns", [2015, 2020])]:
            cat = PREDEFINED_CATALOGUE[name]
            for year in years:
                key = f"{name}_{year}"
                image = cat["get_image"](aoi_ee, year)
                p.raw_variables[key] = GEEVar(
                    name=name,
                    data_type=DataType.raster,
                    raster_type=RasterType(cat["raster_type"]),
                    gee_images=[image],
                    aoi=aoi_ee,
                    project=p,
                    year=year,
                )

        p.base_raster = p.raw_variables["altitude"]

        # Seed dummy processed variables so the Dataset tab is usable
        from pathlib import Path
        from spatialrisk.variables.local_raster_var import LocalRasterVar
        LocalRasterVar.model_rebuild()

        for name in ["altitude", "slope", "protected_area", "roads", "rivers", "subj"]:
            cat = PREDEFINED_CATALOGUE[name]
            p.processed_variables[name] = LocalRasterVar(
                name=name,
                path=Path(f"/tmp/processed/{name}.tif"),
                raster_type=RasterType(cat["raster_type"]),
                data_type=DataType.raster,
                project=p,
            )
        for name, years in [("forest_gfc", [2015, 2020, 2024]), ("towns", [2015, 2020])]:
            cat = PREDEFINED_CATALOGUE[name]
            for yr in years:
                key = f"{name}_{yr}"
                p.processed_variables[key] = LocalRasterVar(
                    name=name,
                    path=Path(f"/tmp/processed/{key}.tif"),
                    raster_type=RasterType(cat["raster_type"]),
                    data_type=DataType.raster,
                    year=yr,
                    project=p,
                )

        logger.debug("SOLARA_TEST: seeded %d raw + %d processed variables",
                      len(p.raw_variables), len(p.processed_variables))
        app_state.project.set(p.model_copy())

    # Test variable seeding disabled for now — Step 2 starts with no variables.
    # (SOLARA_TEST stays on; re-enable by uncommenting the line below.)
    # solara.use_effect(_seed_test_variables, [app_state.project.value])

    def _seed_test_model_and_prediction():
        import os
        if os.getenv("SOLARA_TEST", "false").lower() != "true":
            return
        p = app_state.project.value
        if p is None or p.models or not p.processed_variables:
            return
        from spatialrisk.mlmodels import GLMModel
        from spatialrisk.predictions.prediction import Prediction
        from pathlib import Path

        model = GLMModel(name="glm_v1", model_type="glm", year=2015)
        p.add_model(model, auto_save=False)
        Prediction(
            path=Path("/tmp/processed/glm_calibration.tif"),
            model_key="glm_glm_v1",
            dataset_name="calibration",
            year=2015,
        ).add_to_project(p, auto_save=False)
        logger.debug("SOLARA_TEST: seeded GLM model + one prediction")

    solara.use_effect(_seed_test_model_and_prediction, [])

    # Left drawer: only the Project step (opens as a dialog for load/save)
    steps_data = [
        {
            "id": 1,
            "name": "Project",
            "icon": "mdi-folder-outline",
            "display": "dialog",
            "content": ProjectPanel(),
        },
    ]

    # Right panel: workflow tabs
    right_panel_config = {
        "title": "Workflow",
        "icon": "mdi-tune",
        "width": 480,
        "toggle_icon": "mdi-chevron-right",
    }

    right_panel_content = [
        {
            "content": [WorkflowTabs(map_=sepal_map, gee_interface=gee_interface)],
        },
    ]

    solara.Title("Spatial Risk")

    app_title = compute_app_title(
        app_state.project.value, app_state.project_dirty.value
    )

    MapApp.element(
        app_title=app_title,
        app_icon="mdi-tree",
        main_map=[sepal_map],
        steps_data=steps_data,
        initial_step=None,
        theme_toggle=[theme_toggle],
        right_panel_config=right_panel_config,
        right_panel_content=right_panel_content,
        right_panel_open=True,
        is_pinned=False,
        dialog_width=420,
        repo_url="https://github.com/openforis/spatial-risk",
    )


routes = [
    solara.Route(path="/", component=Page, label="Spatial Risk"),
]

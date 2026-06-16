"""Spatial Risk — Solara GUI entry point.

Run locally:
    ./run_solara.sh gui/solara_app.py 8910
"""

import logging
from pathlib import Path

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

from gui.store.state_manager import app_state
from gui.scripts.project_io import list_projects, load_project, save_project
from gui.tile.aoi_tile import AoiTile
from gui.tile.dataset_tile import DatasetTile
from gui.tile.variables_tile import VariablesTile
from gui.tile.train_tile import TrainTile
from gui.tile.inference_tile import InferenceTile
from gui.widget.notification_area import NotificationArea

logger = setup_logging(logger_name="spatial_risk")
logger.setLevel(logging.DEBUG)
logger.debug("Spatial Risk app initialized")
logger.debug("Solara version: %s", solara.__version__)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

setup_solara_server(extra_asset_locations=[])


@solara.lab.on_kernel_start
def on_kernel_start():
    return setup_sessions()


@solara.component
def ProjectPanel():
    """Load / Save controls — rendered as dialog content in the left drawer."""
    load_dialog_open, set_load_dialog_open = solara.use_state(False)
    projects, set_projects = solara.use_state([])
    selected_project, set_selected_project = solara.use_state(None)
    load_error, set_load_error = solara.use_state(None)

    def open_load():
        set_projects(list_projects(DATA_DIR))
        set_load_dialog_open(True)

    def do_load():
        if not selected_project:
            return
        try:
            p = load_project(selected_project)
            app_state.project.set(p)
            app_state.status_message.set(f"Project '{selected_project}' loaded.")
            set_load_dialog_open(False)
        except Exception as exc:
            set_load_error(str(exc))

    def do_save():
        p = app_state.project.value
        if p is None:
            app_state.error_message.set(
                "No project to save. Complete the AOI step first."
            )
            return
        try:
            path = save_project(p)
            app_state.status_message.set(f"Saved to {path}")
        except Exception as exc:
            app_state.error_message.set(str(exc))

    with solara.Column(style="gap: 8px; padding: 8px;"):
        with solara.Row(style="gap: 8px;"):
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
                on_click=do_save,
            )
    with rv.Dialog(
        v_model=load_dialog_open, on_v_model=set_load_dialog_open, max_width="400px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text("Load Project")
            with rv.CardText():
                if projects:
                    rv.Select(
                        label="Select project",
                        items=projects,
                        v_model=selected_project,
                        on_v_model=set_selected_project,
                        dense=True,
                        outlined=True,
                    )
                else:
                    solara.Info("No saved projects found.")
                if load_error:
                    solara.Error(load_error)
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    "Cancel",
                    on_click=lambda: set_load_dialog_open(False),
                    text=True,
                    small=True,
                )
                solara.Button(
                    "Load",
                    on_click=do_load,
                    color="primary",
                    small=True,
                    disabled=not selected_project,
                )


@solara.component
def WorkflowTabs(map_, gee_interface):
    """Three-tab workflow panel rendered in the right side panel."""
    active_tab, set_active_tab = solara.use_state(0)

    aoi_complete = app_state.aoi_result.value is not None
    variables_complete = (
        app_state.project.value is not None
        and bool(app_state.project.value.raw_variables)
        and app_state.project.value.base_raster is not None
    )

    with rv.Tabs(v_model=active_tab, on_v_model=set_active_tab, grow=True):
        rv.Tab(children=["Area of Interest"])
        rv.Tab(children=["Variables"], disabled=not aoi_complete)
        rv.Tab(
            children=["Dataset"],
            # disabled=not variables_complete
        )
        rv.Tab(
            children=["Train"],
            # disabled=not variables_complete
        )
        rv.Tab(
            children=["Inference"],
            # disabled=not variables_complete
        )

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
            )
        with rv.TabItem():
            DatasetTile(project=app_state.project)

        with rv.TabItem():
            TrainTile(project=app_state.project)

        with rv.TabItem():
            InferenceTile(project=app_state.project)

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

    solara.use_effect(_seed_test_aoi, [])

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

    solara.use_effect(_seed_test_variables, [app_state.project.value])

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

    MapApp.element(
        app_title="Spatial Risk",
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

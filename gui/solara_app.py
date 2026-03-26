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
                outlined=True,
                small=True,
                on_click=open_load,
            )
            solara.Button(
                "Save",
                icon_name="mdi-content-save-outline",
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
                    outlined=True,
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
            # TODO: Put real stuff
            with solara.Column(style="gap: 16px;"):
                solara.Markdown("### Step 4 — Model")
                solara.Info(
                    "Model configuration is not yet implemented. This step will allow you to define target and feature variables for model training."
                )

        with rv.TabItem():
            # TODO: Put real stuff
            with solara.Column(style="gap: 16px;"):
                solara.Markdown("### Step  — Model")
                solara.Info(
                    "Model configuration is not yet implemented. This step will allow you to define target and feature variables for model training."
                )

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
    theme_toggle = ThemeToggle()
    theme_toggle.observe(lambda e: setattr(theme, "dark", e["new"]), "dark")

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
        if p is None or p.raw_variables:
            return
        from pathlib import Path
        from spatialrisk.variables.local_raster_var import LocalRasterVar
        from spatialrisk.variables.models import DataType, RasterType

        LocalRasterVar.model_rebuild()

        single_vars = [
            ("altitude", RasterType.continuous),
            ("slope", RasterType.continuous),
            ("protected_area", RasterType.categorical),
            ("roads", RasterType.categorical),
            ("rivers", RasterType.categorical),
        ]
        for name, rtype in single_vars:
            p.raw_variables[name] = LocalRasterVar(
                name=name,
                path=Path(f"/tmp/{name}.tif"),
                raster_type=rtype,
                data_type=DataType.raster,
                project=p,
            )

        for year in [2015, 2020, 2024]:
            key = f"forest_gfc_{year}"
            p.raw_variables[key] = LocalRasterVar(
                name="forest_gfc",
                path=Path(f"/tmp/forest_gfc_{year}.tif"),
                raster_type=RasterType.categorical,
                data_type=DataType.raster,
                year=year,
                project=p,
            )

        p.base_raster = p.raw_variables["altitude"]
        logger.debug("SOLARA_TEST: seeded %d variables", len(p.raw_variables))
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

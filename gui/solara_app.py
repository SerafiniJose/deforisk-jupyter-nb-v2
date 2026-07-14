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
    get_current_sepal_client,
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
    aoi_project_name,
    compute_app_title,
    format_last_saved,
    format_relative,
    manage_projects_label,
    overwrite_needed,
    project_count_chips,
    validate_project_name,
)
from gui.scripts.map_helpers import show_aoi_on_map, clear_project_overlays
from gui.scripts.aoi_io import load_aoi, write_aoi
from gui.tile.aoi_tile import AoiTile
from gui.tile.dataset_tile import DatasetTile
from gui.tile.variables_tile import VariablesTile, vars_on_map
from gui.tile.derived_map import derived_on_map
from gui.tile.process_tile import ProcessTile
from gui.tile.postprocess_tile import PostProcessTile
from gui.tile.sampling_tile import SamplingTile, sampling_jobs, samples_on_map
from gui.tile.train_tile import TrainTile, train_jobs
from gui.tile.inference_tile import InferenceTile, inference_jobs, preds_on_map
from gui.tile.evaluation_tile import EvaluationTile, eval_jobs
from gui.tile.summary_tile import ProjectSummaryTile
from gui.widget.locale_select import AppLocaleSelect
from gui.widget.notification_area import NotificationArea
from gui.widget.pipeline_header import PipelineHeader
from gui.scripts.log_bridge import install_log_console_handler, clear_log_records
from gui.widget.log_console import LogConsole
from gui.i18n import t, get_translator, reset_translator

logger = setup_logging(logger_name="spatial_risk")
logger.setLevel(logging.DEBUG)
logger.debug("Spatial Risk app initialized")
logger.debug("Solara version: %s", solara.__version__)

# Surface INFO+ milestones in the on-map LogConsole (the LogConsole component
# binds this session's kernel context on mount; see gui/scripts/log_bridge.py).
install_log_console_handler()

setup_solara_server(extra_asset_locations=[])


@solara.lab.on_kernel_start
def on_kernel_start():
    reset_translator()  # re-read ~/.sepal-ui-config locale on every (re)load
    return setup_sessions()


@solara.component
def ProjectPanel(on_close=None):
    """Current-project status + New / Load / Save controls (left drawer).

    ``on_close`` (optional) closes the hosting Project step-dialog; called once a
    project finishes loading so the user lands back on the map.
    """
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

    # Saved-project count for the empty-state "Open saved" button. The empty
    # state is only reachable at session start (nothing sets project back to
    # None), so a single filesystem scan per mount is enough; None = scan failed.
    def _count_saved():
        try:
            return len(list_project_infos(DATA_DIR))
        except Exception:  # pragma: no cover - defensive
            return None

    saved_count = solara.use_memo(_count_saved, [])

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
            # Restore the saved AOI (sidecar geometry + metadata) so the map can
            # frame it and the downstream tabs unlock. Set before installing the
            # project so the load-zoom effect sees it on the same render.
            app_state.aoi_result.set(load_aoi(DATA_DIR / loaded.project_name, loaded.aoi))
            app_state.aoi_asset.set((loaded.aoi or {}).get("asset"))
            app_state.load_project_state(loaded, when)
            app_state.status_message.set(t("project.status_loaded", name=selected))
            app_state.error_message.set(None)
            set_load_open(False)
            # Dismiss the whole Project popup too, not just the inner Load
            # dialog, so a successful load returns the user to the map.
            if on_close is not None:
                on_close()
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
        # new_project_state bumps project_loaded_signal, so the shell's
        # on-switch effects clear the previous project's map overlays/tracking
        # and reset the (empty) Train/Inference job lists — no manual reset here.
        app_state.status_message.set(t("project.status_created", name=validation.cleaned))
        set_new_open(False)
        # Dismiss the whole Project popup too, not just the inner New dialog, so
        # a freshly created project returns the user to the map (mirrors do_load).
        if on_close is not None:
            on_close()

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
                t("project.error_no_project_to_save")
            )
            return
        if overwrite_needed(p.project_name, last_saved, existing_names()):
            set_overwrite_open(True)
            return
        _really_save()

    def _really_save():
        set_overwrite_open(False)
        if p is None:  # project was deleted while the overwrite dialog was open
            return
        try:
            # Persist the AOI alongside the project: geometry → aoi.geojson
            # sidecar, light metadata → project.aoi (saved into the manifest).
            p.aoi = write_aoi(
                DATA_DIR / p.project_name,
                app_state.aoi_result.value,
                asset=app_state.aoi_asset.value,
            )
            path = save_project(p)
            app_state.mark_saved(datetime.now())
            note = ""
            if not p.raw_variables:
                note = t("project.status_saved_note_no_vars")
            elif p.base_raster is None:
                note = t("project.status_saved_note_no_base")
            app_state.status_message.set(t("project.status_saved", path=path, note=note))
            app_state.error_message.set(None)
        except Exception as exc:
            app_state.error_message.set(str(exc))

    # ---- Status block ---------------------------------------------------
    with solara.Column(style="gap: 8px; padding: 8px;"):
        if p is None:
            # Empty state: one primary action (New), Load secondary, no Save —
            # a disabled Save here can never become useful, so it's omitted.
            rv.Icon(
                children=["mdi-map-marker-plus-outline"],
                style_="font-size: 40px; opacity: 0.5;",
            )
            solara.Text(t("project.empty_heading"), style="font-weight: 600;")
            solara.Text(
                t("project.empty_help"),
                classes=["text--secondary"],
            )
            solara.Button(
                t("project.button_new_project"),
                icon_name="mdi-plus",
                color="primary",
                on_click=open_new,
                style="width: 100%;",
            )
            solara.Button(
                manage_projects_label(saved_count),
                icon_name="mdi-folder-open-outline",
                color="primary",
                outlined=True,
                disabled=saved_count == 0,
                on_click=open_load,
                style="width: 100%;",
            )
        else:
            with solara.Row(style="gap: 8px; align-items: center;"):
                solara.Text(p.project_name, style="font-weight: 600;")
                rv.Chip(
                    children=[t("project.chip_unsaved") if dirty else t("project.chip_saved")],
                    color="warning" if dirty else "primary",
                    text_color="white",
                    x_small=True,
                )
            solara.Text(
                t("project.stats", raw=len(p.raw_variables),
                  processed=len(p.processed_variables), models=len(p.models)),
                classes=["text--secondary"],
                style="font-size: 12px;",
            )
            solara.Text(
                format_last_saved(last_saved, datetime.now()),
                classes=["text--secondary"],
                style="font-size: 12px;",
            )
            with solara.Row(style="gap: 8px;"):
                solara.Button(
                    t("project.button_new"),
                    icon_name="mdi-plus",
                    color="primary",
                    outlined=True,
                    small=True,
                    on_click=open_new,
                )
                solara.Button(
                    t("common.load"),
                    icon_name="mdi-folder-open-outline",
                    color="primary",
                    outlined=True,
                    small=True,
                    on_click=open_load,
                )
                solara.Button(
                    t("project.button_save"),
                    icon_name="mdi-content-save-outline",
                    color="primary",
                    outlined=True,
                    small=True,
                    on_click=do_save,
                )

    # ---- New dialog -----------------------------------------------------
    validation = validate_project_name(new_name, existing_names()) if new_open else None
    with rv.Dialog(
        v_model=new_open, on_v_model=set_new_open, max_width="400px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(t("project.dialog_new_title"))
            with rv.CardText():
                rv.TextField(
                    label=t("project.dialog_new_name_label"),
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
                        t("project.dialog_new_exists_warning", name=validation.cleaned)
                    )
                    solara.Button(
                        t("project.dialog_new_load_instead"),
                        text=True,
                        small=True,
                        on_click=load_instead,
                    )
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    t("common.cancel"), on_click=lambda: set_new_open(False), text=True, small=True
                )
                solara.Button(
                    t("common.create"),
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
                solara.Text(t("project.dialog_discard_title"))
            with rv.CardText():
                solara.Text(
                    t("project.dialog_discard_message", name=p.project_name)
                    if p is not None
                    else t("project.dialog_discard_title")
                )
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    t("common.cancel"), on_click=lambda: set_discard_open(False), text=True, small=True
                )
                solara.Button(
                    t("project.dialog_discard_confirm"),
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
                solara.Text(t("project.dialog_overwrite_title"))
            with rv.CardText():
                solara.Text(
                    t("project.dialog_overwrite_message", name=p.project_name)
                    if p is not None
                    else t("project.dialog_overwrite_title")
                )
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    t("common.cancel"),
                    on_click=lambda: set_overwrite_open(False),
                    text=True,
                    small=True,
                )
                solara.Button(
                    t("project.dialog_overwrite_confirm"), on_click=_really_save, color="error", small=True
                )

    # ---- Load dialog ----------------------------------------------------
    with rv.Dialog(
        v_model=load_open, on_v_model=set_load_open, max_width="440px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(t("project.dialog_load_title"))
            with rv.CardText():
                if not infos:
                    solara.Info(t("project.dialog_load_empty"))
                else:
                    now = datetime.now()
                    with rv.List(three_line=True):
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
                                            with rv.Row(
                                                style_="flex-wrap: wrap; gap: 4px; "
                                                "margin: 2px 0;"
                                            ):
                                                for chip in project_count_chips(info):
                                                    rv.Chip(
                                                        children=[chip.label],
                                                        x_small=True,
                                                        color="primary" if chip.accent else None,
                                                        text_color="white" if chip.accent else None,
                                                    )
                                            rv.ListItemSubtitle(
                                                children=[
                                                    t("project.dialog_load_modified",
                                                      time_ago=format_relative(info.modified, now))
                                                ]
                                            )
                                        else:
                                            rv.ListItemSubtitle(
                                                children=[
                                                    info.error or t("project.dialog_load_unreadable")
                                                ]
                                            )
                if load_busy:
                    rv.ProgressLinear(indeterminate=True)
                if load_error:
                    solara.Error(load_error)
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(
                    t("common.cancel"), on_click=lambda: set_load_open(False), text=True, small=True
                )
                solara.Button(
                    t("common.load"),
                    on_click=do_load,
                    color="primary",
                    small=True,
                    disabled=not selected or load_busy,
                )


@solara.component
def WorkflowTabs(map_, gee_interface, sepal_client=None):
    """Workflow panel: pipeline header (step map + navigation) over the
    step tiles. Step order/gating live in gui/store/workflow_steps.py."""
    active_tab, set_active_tab = solara.use_state(0)

    # The global load/save status banner shows on whatever tab the user is on
    # when they load/create/save, but it's stale once they move on. Clear it the
    # moment they navigate to another step. Keyed on active_tab: the mount run is
    # a harmless no-op (nothing set yet) and a load never changes active_tab, so
    # the message survives until the user actually switches tabs.
    def _clear_status_on_tab_switch():
        app_state.status_message.set(None)

    solara.use_effect(_clear_status_on_tab_switch, [active_tab])

    PipelineHeader(
        active_step=active_tab,
        on_navigate=set_active_tab,
        project=app_state.project,
        aoi_result=app_state.aoi_result,
    )

    with rv.TabsItems(v_model=active_tab):
        with rv.TabItem():
            AoiTile(
                map_=map_,
                gee_interface=gee_interface,
                aoi_result=app_state.aoi_result,
                aoi_asset=app_state.aoi_asset,
                on_selection=app_state.aoi_asset.set,
                restore_signal=app_state.project_loaded_signal.value,
                loading=app_state.loading,
            )
        with rv.TabItem():
            VariablesTile(
                project=app_state.project,
                process_error=app_state.process_error,
                map_=map_,
                sepal_client=sepal_client,
            )
        with rv.TabItem():
            ProcessTile(
                project=app_state.project,
                processing=app_state.processing,
                process_error=app_state.process_error,
                map_=map_,
            )
        with rv.TabItem():
            PostProcessTile(
                project=app_state.project,
                process_error=app_state.process_error,
                map_=map_,
            )
        with rv.TabItem():
            DatasetTile(project=app_state.project)

        with rv.TabItem():
            SamplingTile(project=app_state.project, map_=map_)

        with rv.TabItem():
            TrainTile(project=app_state.project)

        with rv.TabItem():
            InferenceTile(project=app_state.project, map_=map_, sepal_client=sepal_client)

        with rv.TabItem():
            EvaluationTile(project=app_state.project)

    NotificationArea(
        active_tab=active_tab,
        aoi_result=app_state.aoi_result,
        project=app_state.project,
        process_error=app_state.process_error,
        status_message=app_state.status_message,
        error_message=app_state.error_message,
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
    sepal_client = get_current_sepal_client()
    theme_toggle = solara.use_memo(lambda: ThemeToggle(), [])
    locale_select = solara.use_memo(lambda: AppLocaleSelect(translator=get_translator()), [])

    def _observe_theme():
        handler = lambda e: setattr(theme, "dark", e["new"])
        theme_toggle.observe(handler, "dark")
        return lambda: theme_toggle.unobserve(handler, "dark")

    solara.use_effect(_observe_theme, [])

    sepal_map = solara.use_memo(
        lambda: sm.SepalMap(
            zoom=3,
            min_zoom=3,
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

        name = aoi_project_name(aoi.name, datetime.now())
        app_state.project.set(Project(project_name=name))

    solara.use_effect(sync_project_from_aoi, [app_state.aoi_result.value])

    # On every project switch (load OR new), the signal below bumps so these
    # effects re-run. Read it here so Page re-renders (and the effects re-run).
    project_loaded_signal = app_state.project_loaded_signal.value

    # View reset. The SepalMap is shared across switches, so first drop the
    # previous project's overlay layers (variables, sample points, predictions,
    # old AOI; basemaps kept) and forget each tile's "on map" tracking, THEN
    # draw this project's AOI and frame it. Clearing MUST precede the redraw, so
    # this effect runs before the job-list reset effect below.
    def render_map_on_switch():
        clear_project_overlays(sepal_map)
        vars_on_map.set(set())
        derived_on_map.set(set())
        samples_on_map.set(set())
        preds_on_map.set(set())
        show_aoi_on_map(sepal_map, app_state.aoi_result.value)

    solara.use_effect(render_map_on_switch, [project_loaded_signal])

    # Session job lists are transient overlays; product rows derive from the
    # loaded project's registries at render time. Only the leftovers of the
    # previous project's runs need clearing on switch.
    def reset_jobs_on_load():
        train_jobs.set([])
        inference_jobs.set([])
        eval_jobs.set([])
        sampling_jobs.set([])

    solara.use_effect(reset_jobs_on_load, [project_loaded_signal])

    # Each project starts with a fresh process log (consistent with the map /
    # job-list resets above, keyed on the same project-switch signal).
    def reset_log_on_switch():
        clear_log_records()

    solara.use_effect(reset_log_on_switch, [project_loaded_signal])

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

    # Test model/prediction seeding removed: it injected a fake GLM model and a
    # prediction pointing at a nonexistent /tmp path into whatever real project
    # was open, which the next save then persisted.

    # Handle to the MapApp widget, captured after mount (see _capture_map_app
    # below), so we can imperatively close its step-dialog from inside a tile.
    map_app_ref = solara.use_ref(None)

    def close_project_dialog():
        """Close the Project step-dialog (mirrors MapApp's deactivate path)."""
        widget = map_app_ref.current
        if widget is not None:
            widget.step_open = False
            widget.current_step = None

    # Left drawer: Project controls + read-only Project Summary (both open as dialogs)
    steps_data = [
        {
            "id": 1,
            "name": t("app.step_project"),
            "icon": "mdi-folder-outline",
            "display": "dialog",
            "content": ProjectPanel(on_close=close_project_dialog),
        },
        {
            "id": 2,
            "name": t("app.step_project_summary"),
            "icon": "mdi-clipboard-text-outline",
            "display": "dialog",
            "content": ProjectSummaryTile(
                project=app_state.project,
                project_dirty=app_state.project_dirty,
                last_saved=app_state.last_saved,
            ),
            "width": 760,
        },
    ]

    # Right panel: workflow tabs
    right_panel_config = {
        "title": t("app.panel_workflow_title"),
        "icon": "mdi-tune",
        "width": 480,
        "toggle_icon": "mdi-chevron-right",
    }

    right_panel_content = [
        {
            "content": [WorkflowTabs(map_=sepal_map, gee_interface=gee_interface, sepal_client=sepal_client)],
        },
    ]

    solara.Title(t("app.title"))

    # pysepal's step-dialog content area sets `overflow-y: auto` with no
    # `overflow-x`, which CSS resolves to `overflow-x: auto` — producing a
    # spurious horizontal scrollbar even when the panel fits. The dialog card
    # already clips with `overflow: hidden`, so pin the content's x-axis hidden.
    solara.Style(".dialog-content { overflow-x: hidden !important; }")

    app_title = compute_app_title(
        app_state.project.value, app_state.project_dirty.value
    )

    map_app_el = MapApp.element(
        app_title=app_title,
        app_icon="mdi-tree",
        main_map=[sepal_map],
        steps_data=steps_data,
        initial_step=1,  # auto-open the Project dialog (step id 1) at startup
        theme_toggle=[theme_toggle],
        language_selector=[locale_select],
        right_panel_config=right_panel_config,
        right_panel_content=right_panel_content,
        right_panel_open=True,
        is_pinned=False,
        dialog_width=560,  # roomier Project dialog (scroll fix is the .dialog-content style above)
        repo_url="https://github.com/openforis/spatial-risk",
    )

    # Grab the realized MapApp widget so close_project_dialog() can drive its
    # step-dialog traits. Recaptured each render (cheap, element is stable).
    def _capture_map_app():
        try:
            map_app_ref.current = solara.get_widget(map_app_el)
        except Exception:  # pragma: no cover - defensive (pre-mount)
            pass

    solara.use_effect(_capture_map_app, [map_app_el])

    # Floating, collapsible process-log panel (lower-right, over the map).
    LogConsole()


routes = [
    solara.Route(path="/", component=Page, label="Spatial Risk"),
]

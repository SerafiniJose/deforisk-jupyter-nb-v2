"""Step 2 — Variables tile."""

import logging
from pathlib import Path

import ee
import reacton.ipyvuetify as rv
import solara

logger = logging.getLogger("spatial_risk")

from gui.widget.variable_list import DerivedVariableList, SourceVariableList
from gui.widget.variable_modal import VariableModal
from spatialrisk.project import Project
from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.local_raster_var import LocalRasterVar
from spatialrisk.variables.local_vector_var import LocalVectorVar

LocalRasterVar.model_rebuild()
GEEVar.model_rebuild()
LocalVectorVar.model_rebuild()

# Keys of source variables currently displayed on the map (drives the toggle state).
vars_on_map = solara.reactive(set())


def _map_layer_key(key: str) -> str:
    """Unique map-layer key for a source variable."""
    return f"var_{key}"


async def _grayscale_vis(image, var, gee_interface):
    """Grayscale palette stretched to the image's min/max over its AOI.

    Falls back to a bare grayscale palette (GEE's default 0–1 stretch) if the
    min/max can't be computed.
    """
    vis = {"palette": ["000000", "ffffff"]}
    aoi = getattr(var, "aoi", None)
    if aoi is None or gee_interface is None:
        return vis
    try:
        geom = aoi if isinstance(aoi, ee.Geometry) else aoi.geometry()
        stats = await gee_interface.get_info_async(
            image.reduceRegion(
                reducer=ee.Reducer.minMax(),
                geometry=geom,
                scale=getattr(var, "default_scale", None) or 100,
                maxPixels=1e8,
                bestEffort=True,
            )
        )
        mins = [v for k, v in stats.items() if k.endswith("_min") and v is not None]
        maxs = [v for k, v in stats.items() if k.endswith("_max") and v is not None]
        if mins and maxs:
            vis["min"], vis["max"] = min(mins), max(maxs)
    except Exception:
        logger.debug("grayscale min/max failed; using default stretch", exc_info=True)
    return vis


async def _styled_layer(image, var, gee_interface):
    """Choose visualization by raster type.

    categorical (incl. binary masks) -> ``ee.Image.randomVisualizer()`` (random RGB);
    continuous -> grayscale stretched to the image's min/max.

    Returns (image_to_add, vis_params).
    """
    rt = getattr(var, "raster_type", None)
    rt = rt.value if hasattr(rt, "value") else (str(rt) if rt is not None else "")
    if rt == "categorical":
        return image.randomVisualizer(), {}
    return image, await _grayscale_vis(image, var, gee_interface)


def _variable_to_entry(key: str, var, project) -> dict:
    """Reconstruct a modal entry dict from an existing variable object."""
    from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE

    vtype = type(var).__name__
    pp = [p.value if hasattr(p, "value") else str(p) for p in (var.post_processing or [])]

    # Predefined GEE variables hold an ee.Image (in gee_images) and carry no
    # local path / asset id — they are rebuilt from the catalogue by key. Round-
    # trip them as a predefined entry so editing re-fetches the image instead of
    # dropping gee_images and stringifying path=None into the literal "None"
    # (which then fails GEEVar validation on save).
    if vtype == "GEEVar" and not var.path and var.name in PREDEFINED_CATALOGUE:
        return {
            "source": "predefined",
            "type": "GEEVar",
            "name": var.name,
            "predefined_key": var.name,
            "year": str(var.year) if var.year else "",
        }

    entry = {
        "source": "custom",
        "type": vtype,
        "name": var.name,
        "year": str(var.year) if var.year else "",
        "post_processing": pp,
    }
    if vtype == "LocalRasterVar":
        entry["path"] = str(var.path)
        entry["raster_type"] = var.raster_type.value if hasattr(var.raster_type, "value") else str(var.raster_type)
    elif vtype == "GEEVar":
        entry["asset_id"] = str(var.path) if var.path else ""
        entry["scale"] = str(var.default_scale) if getattr(var, "default_scale", None) else ""
    elif vtype == "LocalVectorVar":
        entry["path"] = str(var.path)
        entry["rasterization_method"] = var.rasterization_method.value if hasattr(var.rasterization_method, "value") else str(var.rasterization_method)
    return entry


def _build_variable(entry: dict, project):
    """Instantiate the correct variable class from a modal entry dict."""
    if entry.get("source") == "predefined":
        return _build_predefined(entry, project)

    common = dict(
        name=entry["name"],
        year=entry.get("year"),
        post_processing=entry.get("post_processing", []),
        project=project,
    )
    vtype = entry["type"]
    if vtype == "LocalRasterVar":
        return LocalRasterVar(
            path=entry["path"],
            raster_type=entry.get("raster_type"),
            data_type=entry["data_type"],
            **common,
        )
    if vtype == "GEEVar":
        return GEEVar(
            path=entry["path"],
            default_scale=entry.get("default_scale"),
            data_type=entry["data_type"],
            **common,
        )
    if vtype == "LocalVectorVar":
        return LocalVectorVar(
            path=entry["path"],
            rasterization_method=entry.get("rasterization_method"),
            data_type=entry["data_type"],
            **common,
        )
    raise ValueError(f"Unknown variable type: {vtype}")


def _build_predefined(entry: dict, project):
    """Build a GEEVar from a predefined catalogue entry."""
    from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE, resolve_aoi_ee
    from gui.store.state_manager import app_state

    key = entry["predefined_key"]
    cat = PREDEFINED_CATALOGUE[key]

    aoi_result = app_state.aoi_result.value
    if aoi_result is None:
        raise ValueError("No AOI selected — complete the AOI step first.")

    aoi_ee = resolve_aoi_ee(aoi_result)
    year = entry.get("year")
    image = cat["get_image"](aoi_ee, year)

    return GEEVar(
        name=entry["name"],
        data_type=entry["data_type"],
        raster_type=entry.get("raster_type"),
        gee_images=[image],
        aoi=aoi_ee,
        project=project,
        year=year,
    )


def _drop_from_map(key: str, map_):
    """Remove a variable's layer from the map and forget its on-map state."""
    if map_ is not None:
        map_.remove_layer(_map_layer_key(key), none_ok=True)
    if key in vars_on_map.value:
        remaining = set(vars_on_map.value)
        remaining.discard(key)
        vars_on_map.set(remaining)


@solara.component
def VariablesTile(project, process_error, map_=None):
    """Variables step: add, inspect, and process variables.

    Args:
        project: Reactive holding the current Project (or None).
        process_error: Reactive str | None — error from last processing action.
        map_: SepalMap instance used by the per-variable "show on map" toggle.
    """
    modal_open = solara.use_reactive(False)
    editing_key, set_editing_key = solara.use_state(None)
    pending_toggle = solara.use_reactive(None)

    @solara.lab.use_task(dependencies=None, raise_error=False)
    async def _apply_map_toggle():
        """Add or remove a GEE-backed variable's layer using the async map API.

        The async path is required: the sync ``add_ee_layer`` blocks on the GEE
        interface's private event loop and hangs when called from a Solara handler.
        """
        key = pending_toggle.value
        if key is None or map_ is None:
            return
        p = project.value
        var = p.raw_variables.get(key) if p is not None else None
        images = getattr(var, "gee_images", None) if var is not None else None
        if not images:
            return
        try:
            if key in vars_on_map.value:
                _drop_from_map(key, map_)
            else:
                image, vis = await _styled_layer(images[0], var, map_.gee_interface)
                await map_.add_ee_layer_async(
                    image, vis, name=key, key=_map_layer_key(key), use_map_vis=False
                )
                vars_on_map.set(set(vars_on_map.value) | {key})
        except Exception as exc:
            logger.exception("map toggle failed for %s", key)
            process_error.set(f"Could not toggle '{key}' on map: {exc}")

    def on_toggle_map(key: str):
        """Trigger the async toggle task for one source variable."""
        if map_ is None:
            return
        pending_toggle.set(key)
        _apply_map_toggle()

    def on_add(entry: dict):
        logger.debug("on_add called: %s", entry)
        p = project.value
        if p is None:
            logger.warning("on_add: project is None")
            process_error.set("No active project — complete the AOI step first.")
            return
        try:
            var = _build_variable(entry, p)
            key = f"{var.name}_{var.year}" if var.year else var.name
            p.raw_variables[key] = var
            logger.debug("Added var '%s', raw_variables now: %s", key, list(p.raw_variables.keys()))
            project.set(p.model_copy())
            logger.debug("project.set() called, project.value.raw_variables: %s", list(project.value.raw_variables.keys()))
        except Exception as exc:
            logger.exception("on_add failed")
            process_error.set(f"Could not add variable: {exc}")

    def on_edit_open(key: str):
        set_editing_key(key)
        modal_open.set(True)

    def on_save(old_key: str, new_entry: dict):
        p = project.value
        if p is None:
            return
        try:
            old_var = p.raw_variables.pop(old_key, None)
            if old_var and p.base_raster and p.base_raster.name == old_var.name:
                p.base_raster = None
                process_error.set(
                    f"Base raster was reset because its source layer '{old_var.name}' "
                    "changed — re-set it in Step 3 — Process."
                )
            # The key may change on edit — drop the stale layer so it doesn't linger.
            _drop_from_map(old_key, map_)
            var = _build_variable(new_entry, p)
            new_key = f"{var.name}_{var.year}" if var.year else var.name
            p.raw_variables[new_key] = var
            set_editing_key(None)
            project.set(p.model_copy())
        except Exception as exc:
            logger.exception("on_save failed")
            process_error.set(f"Could not save variable: {exc}")

    def on_remove(key: str):
        p = project.value
        if p is None:
            return
        removed = p.raw_variables.pop(key, None)
        if removed and p.base_raster and p.base_raster.name == removed.name:
            p.base_raster = None
            process_error.set(
                f"Base raster was reset because its source layer '{removed.name}' "
                "was removed — re-set it in Step 3 — Process."
            )
        _drop_from_map(key, map_)
        project.set(p.model_copy())

    def on_remove_derived(key: str):
        """Remove a derived (processed) variable and delete its raster from disk."""
        p = project.value
        if p is None:
            return
        removed = p.processed_variables.pop(key, None)
        path = getattr(removed, "path", None) if removed is not None else None
        if path:
            try:
                fp = Path(path)
                if fp.is_file():
                    fp.unlink()
                    logger.debug("Deleted derived raster file: %s", fp)
            except OSError as exc:
                logger.exception("Could not delete file for derived var '%s'", key)
                process_error.set(f"Removed '{key}' but could not delete its file: {exc}")
        project.set(p.model_copy())

    fl_var, set_fl_var = solara.use_state("")
    fl_start, set_fl_start = solara.use_state(None)
    fl_end, set_fl_end = solara.use_state(None)

    def on_add_forest_loss():
        from gui.scripts.process_actions import add_forest_loss_spec
        p = project.value
        if p is None:
            return
        try:
            add_forest_loss_spec(p, fl_var, int(fl_start), int(fl_end))
            set_fl_start(None)
            set_fl_end(None)
            project.set(p.model_copy())
        except Exception as exc:
            process_error.set(f"Could not add forest-loss target: {exc}")

    def on_remove_forest_loss(name: str):
        p = project.value
        if p is None:
            return
        p.forest_loss_specs = [s for s in p.forest_loss_specs if s.name != name]
        project.set(p.model_copy())

    p = project.value

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 2 — Variables")
        solara.Text("Add input variables and deforestation targets for the risk model.")

        # Action bar
        with solara.Row(style="gap: 8px; align-items: center;"):
            solara.Button(
                "Add Variable",
                icon_name="mdi-plus",
                color="primary",
                small=True,
                on_click=lambda: modal_open.set(True),
            )

        # Source variable list
        solara.Markdown("**SOURCE VARIABLES**" + (f" ({len(p.raw_variables)})" if p else " (0)"))
        SourceVariableList(
            project=project,
            on_remove=on_remove,
            on_edit=on_edit_open,
            on_toggle_map=on_toggle_map if map_ is not None else None,
            vars_on_map=vars_on_map,
        )

        # Forest-loss target declaration (deferred; generated during Process)
        from gui.scripts.process_actions import forest_loss_candidates
        candidates = forest_loss_candidates(p) if p else {}
        with solara.Column(style="gap:8px;"):
            solara.Markdown("**FOREST-LOSS TARGETS** (generated during Process)")
            if not candidates:
                solara.Text(
                    "Add a temporal forest layer with at least two years "
                    "(e.g. forest_gfc) to create a forest-loss target.",
                    style="font-size:0.8rem;color:rgba(0,0,0,0.6);font-style:italic;",
                )
            else:
                years = candidates.get(fl_var, [])
                with solara.Row(style="gap:8px;align-items:center;"):
                    rv.Select(
                        label="Forest layer",
                        items=list(candidates.keys()),
                        v_model=fl_var,
                        on_v_model=set_fl_var,
                        dense=True, outlined=True,
                    )
                    rv.Select(
                        label="From year", items=years,
                        v_model=fl_start, on_v_model=set_fl_start,
                        dense=True, outlined=True,
                    )
                    rv.Select(
                        label="To year", items=years,
                        v_model=fl_end, on_v_model=set_fl_end,
                        dense=True, outlined=True,
                    )
                    solara.Button(
                        "Add target", icon_name="mdi-plus", small=True, color="primary",
                        on_click=on_add_forest_loss,
                        disabled=not (fl_var and fl_start and fl_end),
                    )
            for spec in (p.forest_loss_specs if p else []):
                with solara.Row(style="gap:8px;align-items:center;"):
                    rv.Chip(children=[spec.name], x_small=True, outlined=True)
                    rv.Chip(children=["pending"], color="amber", x_small=True)
                    solara.Button(
                        "", icon_name="mdi-delete-outline", icon=True, text=True, x_small=True,
                        on_click=lambda *_, n=spec.name: on_remove_forest_loss(n),
                    )

        # Derived variable list
        if p and p.processed_variables:
            DerivedVariableList(project=project, on_remove=on_remove_derived)

    editing_entry = (
        _variable_to_entry(editing_key, p.raw_variables[editing_key], p)
        if editing_key and p and editing_key in p.raw_variables
        else None
    )
    VariableModal(
        open_=modal_open,
        on_add=on_add,
        on_save=on_save,
        editing_key=editing_key,
        initial_entry=editing_entry,
    )

"""Step 2 — Variables tile."""

import asyncio
import logging

import ee
import reacton.ipyvuetify as rv
import solara

logger = logging.getLogger("spatial_risk")

from pysepal.solara.notifications import use_notifications

from gui.i18n import t
from gui.scripts import process_actions
from gui.scripts.map_helpers import add_vector_on_map, is_mappable
from gui.scripts.notify_bridge import tracked_job
from gui.scripts.solara_threads import publish_if_current, to_thread_in_context
from gui.scripts.variable_map import add_raster_var_on_map
from gui.store.project_writers import writing
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.help import InfoButton
from gui.widget.variable_list import SourceVariableList
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


def _minmax(image, var, gee_interface):
    """Compute (min, max) of the image over its AOI, or None if unavailable.

    Uses the *blocking* ``gee_interface.get_info`` so the session call is
    scheduled onto the GEE interface's own event loop (see ``_add_gee_layer``).
    Must therefore run off the Solara loop — i.e. inside a worker thread.
    """
    aoi = getattr(var, "aoi", None)
    if aoi is None or gee_interface is None:
        return None
    try:
        geom = aoi if isinstance(aoi, ee.Geometry) else aoi.geometry()
        stats = gee_interface.get_info(
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
            return min(mins), max(maxs)
    except Exception:
        logger.debug("min/max over AOI failed", exc_info=True)
    return None


def _grayscale_vis(image, var, gee_interface):
    """Grayscale palette stretched to the image's min/max over its AOI.

    Falls back to a bare grayscale palette (GEE's default 0–1 stretch) if the
    min/max can't be computed.
    """
    vis = {"palette": ["000000", "ffffff"]}
    mm = _minmax(image, var, gee_interface)
    if mm:
        vis["min"], vis["max"] = mm
    return vis


def _styled_layer(image, var, gee_interface):
    """Choose visualization for a source variable.

    Predefined catalogue variables carry their own visualization spec (keyed by
    name): a ``random_visualizer`` flag (random RGB per class) or a ``vis_params``
    dict whose palette is stretched dynamically when no min/max is given.

    Everything else falls back to by-``raster_type`` defaults: categorical (incl.
    binary masks) -> black/white palette (0=black, 1=white); continuous ->
    grayscale stretched to the image's min/max.

    Returns (image_to_add, vis_params). Synchronous — any GEE stretch it computes
    goes through the blocking interface, so call it from a worker thread.
    """
    from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE

    cat = PREDEFINED_CATALOGUE.get(getattr(var, "name", "") or "")
    if cat:
        if cat.get("random_visualizer"):
            return image.randomVisualizer(), {}
        vis = cat.get("vis_params")
        if vis:
            vis = dict(vis)
            if "min" not in vis or "max" not in vis:
                mm = _minmax(image, var, gee_interface)
                if mm:
                    vis.setdefault("min", mm[0])
                    vis.setdefault("max", mm[1])
            return image, vis

    rt = getattr(var, "raster_type", None)
    rt = rt.value if hasattr(rt, "value") else (str(rt) if rt is not None else "")
    if rt == "categorical":
        return image, {"palette": ["000000", "ffffff"], "min": 0, "max": 1}
    return image, _grayscale_vis(image, var, gee_interface)


def _add_gee_layer(map_, image, var, name: str, layer_key: str):
    """Style and add a GEE image layer to ``map_`` (blocking; run in a thread).

    Uses the GEE interface's *synchronous* API (``add_ee_layer`` / ``get_info``),
    which schedules the underlying eeclient session calls onto the interface's
    own private event loop. The async map API (``add_ee_layer_async``) cannot be
    used here: awaited on Solara's event loop it touches session locks bound to
    the interface's loop and raises "bound to a different event loop". Offloading
    this blocking call with ``asyncio.to_thread`` keeps Solara's loop free, the
    same pattern the local raster/vector branches use.
    """
    styled_image, vis = _styled_layer(image, var, map_.gee_interface)
    map_.add_ee_layer(styled_image, vis, name=name, key=layer_key, use_map_vis=False)


def _variable_to_entry(key: str, var, project) -> dict:
    """Reconstruct a modal entry dict from an existing variable object."""
    from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE

    vtype = type(var).__name__

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
    }
    if vtype == "LocalRasterVar":
        entry["path"] = str(var.path)
        entry["raster_type"] = (
            var.raster_type.value
            if hasattr(var.raster_type, "value")
            else str(var.raster_type)
        )
    elif vtype == "GEEVar":
        entry["asset_id"] = str(var.path) if var.path else ""
        entry["scale"] = (
            str(var.default_scale) if getattr(var, "default_scale", None) else ""
        )
    elif vtype == "LocalVectorVar":
        entry["path"] = str(var.path)
        entry["rasterization_method"] = (
            var.rasterization_method.value
            if hasattr(var.rasterization_method, "value")
            else str(var.rasterization_method)
        )
    return entry


def entry_key(entry: dict) -> str:
    """Storage key a modal entry will land under in raw_variables.

    Mirrors the key on_add computes from the built variable (name_year, or bare
    name when year is empty) — used to detect a duplicate BEFORE building the
    variable, which for predefined entries would already fetch the GEE image.
    """
    year = entry.get("year")
    return f"{entry['name']}_{year}" if year else entry["name"]


def _build_variable(entry: dict, project):
    """Instantiate the correct variable class from a modal entry dict."""
    if entry.get("source") == "predefined":
        return _build_predefined(entry, project)

    common = dict(
        name=entry["name"],
        year=entry.get("year"),
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
def VariablesTile(project, process_error, map_=None, sepal_client=None):
    """Variables step: add, inspect, and process variables.

    Args:
        project: Reactive holding the current Project (or None).
        process_error: Reactive str | None — error from last processing action.
        map_: SepalMap instance used by the per-variable "show on map" toggle.
    """
    modal_open = solara.use_reactive(False)
    editing_key, set_editing_key = solara.use_state(None)
    pending_toggle = solara.use_reactive(None)
    # Key of the variable being downloaded, or None for a bulk download.
    pending_download = solara.use_reactive(None)
    notifications = use_notifications()

    @solara.lab.use_task(dependencies=None, raise_error=False, prefer_threaded=True)
    async def download_task():
        """Materialize GEE-backed variables to local files (all, or one key).

        Runs on a worker thread (prefer_threaded) so the UI stays responsive;
        progress is driven by download_task.pending.
        """
        p = project.value
        if p is None:
            return
        key = pending_download.value
        keys = [key] if key is not None else None
        process_error.set(None)
        var = p.raw_variables.get(key) if key is not None else None
        title = (
            t("notifications.task_download_one", name=getattr(var, "name", key))
            if key is not None
            else t("notifications.task_download_all")
        )

        def _tracked_download():
            # Entered on the pool thread so the per-layer log lines feed this
            # tracker; to_thread_in_context supplies the kernel context the
            # tracker's bus updates need to reach the browser.
            with tracked_job(notifications, title):
                process_actions.materialize_raw_layers(p, keys)
                p.save()

        with writing(p.project_name):
            try:
                await to_thread_in_context(_tracked_download)
            except Exception as exc:
                logger.exception("download failed")
                process_error.set(t("tiles.variables.error_download", exc=exc))
            publish_if_current(project, p)

    def on_download(key=None):
        """Download one variable (key) or all pending GEE variables (None)."""
        pending_download.set(key)
        download_task()

    @solara.lab.use_task(dependencies=None, raise_error=False)
    async def _apply_map_toggle():
        """Add or remove a variable's layer on the map.

        Every layer-add is offloaded to a worker thread. GEE-backed layers use
        the GEE interface's blocking API (via ``_add_gee_layer``) so the session
        calls run on the interface's own event loop; the async map API crashes
        with "bound to a different event loop" when awaited on Solara's loop.
        Local raster/vector layers use the blocking ``add_raster_var_on_map`` /
        ``add_vector_on_map`` helpers the same way. Downloaded rasters keep the
        palette they had as a GEE layer (see ``add_raster_var_on_map``) instead of
        rendering grayscale.
        """
        key = pending_toggle.value
        if key is None or map_ is None:
            return
        p = project.value
        var = p.raw_variables.get(key) if p is not None else None
        if var is None or not is_mappable(var):
            return
        try:
            if key in vars_on_map.value:
                _drop_from_map(key, map_)
                return

            images = getattr(var, "gee_images", None)
            layer_key = _map_layer_key(key)
            if images:
                await asyncio.to_thread(
                    _add_gee_layer, map_, images[0], var, key, layer_key
                )
            elif type(var).__name__ == "LocalVectorVar":
                await asyncio.to_thread(
                    add_vector_on_map, map_, str(var.path), key, layer_key
                )
            else:  # LocalRasterVar — reuse the palette it had as a GEE layer
                await asyncio.to_thread(
                    add_raster_var_on_map,
                    map_,
                    str(var.path),
                    var=var,
                    layer_name=key,
                    key=layer_key,
                    fit_bounds=False,
                )
            vars_on_map.set(set(vars_on_map.value) | {key})
        except Exception as exc:
            logger.exception("map toggle failed for %s", key)
            process_error.set(t("tiles.variables.error_toggle_map", key=key, exc=exc))

    def on_toggle_map(key: str):
        """Trigger the async toggle task for one source variable."""
        if map_ is None:
            return
        pending_toggle.set(key)
        _apply_map_toggle()

    pending_add, set_pending_add = solara.use_state(None)

    def _do_add(entry: dict):
        p = project.value
        if p is None:
            logger.warning("on_add: project is None")
            process_error.set(t("tiles.variables.error_no_project"))
            return
        try:
            var = _build_variable(entry, p)
            key = f"{var.name}_{var.year}" if var.year else var.name
            # Replacing an existing entry needs the same cleanup as an edit:
            # drop the stale map layer and reset the base raster if this was
            # its source (the replacement starts cloud-backed again).
            old = p.raw_variables.pop(key, None)
            if old is not None:
                if p.base_raster is not None and p.base_raster.name == old.name:
                    p.base_raster = None
                    process_error.set(
                        t("tiles.variables.error_base_raster_reset", name=old.name)
                    )
                _drop_from_map(key, map_)
            p.raw_variables[key] = var
            logger.debug(
                "Added var '%s', raw_variables now: %s",
                key,
                list(p.raw_variables.keys()),
            )
            project.set(p.model_copy())
        except Exception as exc:
            logger.exception("on_add failed")
            process_error.set(t("tiles.variables.error_add_variable", exc=exc))

    def on_add(entry: dict):
        logger.debug("on_add called: %s", entry)
        p = project.value
        if p is None:
            logger.warning("on_add: project is None")
            process_error.set(t("tiles.variables.error_no_project"))
            return
        # Duplicate key (e.g. re-adding a predefined variable that was already
        # downloaded): confirm before silently clobbering it.
        if entry_key(entry) in p.raw_variables:
            set_pending_add(entry)
            return
        _do_add(entry)

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
                    t("tiles.variables.error_base_raster_reset", name=old_var.name)
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
            process_error.set(t("tiles.variables.error_save_variable", exc=exc))

    pending_remove, set_pending_remove = solara.use_state(None)

    def _do_remove(key: str):
        p = project.value
        if p is None:
            return
        removed = p.raw_variables.pop(key, None)
        if removed and p.base_raster and p.base_raster.name == removed.name:
            p.base_raster = None
            process_error.set(
                t("tiles.variables.error_base_raster_removed", name=removed.name)
            )
        _drop_from_map(key, map_)
        project.set(p.model_copy())

    p = project.value
    pending_geevars = (
        [k for k, v in p.raw_variables.items() if type(v).__name__ == "GEEVar"]
        if p
        else []
    )

    with solara.Column(style="gap: 16px;"):
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.variables.description"))
            InfoButton(t("tiles.variables.info_header"), t("tiles.variables.info_md"))

        # Action bar
        solara.Button(
            t("tiles.variables.add_variable_button"),
            icon_name="mdi-plus",
            color="primary",
            small=True,
            block=True,
            on_click=lambda: modal_open.set(True),
        )

        # Source variable list (ProductTable renders its own collapsible header)
        SourceVariableList(
            project=project,
            on_remove=set_pending_remove,
            on_edit=on_edit_open,
            on_toggle_map=on_toggle_map if map_ is not None else None,
            vars_on_map=vars_on_map,
            on_download=on_download,
            download_pending=download_task.pending,
            downloading_key=pending_download.value if download_task.pending else None,
        )

        # Download-all button, below the list
        solara.Button(
            t("tiles.variables.download_button", count=len(pending_geevars)),
            icon_name="mdi-cloud-download-outline",
            color="primary",
            outlined=True,
            small=True,
            on_click=lambda: on_download(None),
            loading=download_task.pending and pending_download.value is None,
            disabled=download_task.pending or not pending_geevars,
        )
        if download_task.pending:
            solara.ProgressLinear(True)

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
        sepal_client=sepal_client,
        existing_keys=frozenset(p.raw_variables) if p else frozenset(),
    )

    _pending_var = (
        p.raw_variables.get(pending_remove) if (p and pending_remove) else None
    )
    _pending_is_base = bool(
        _pending_var and p.base_raster and p.base_raster.name == _pending_var.name
    )
    _confirm_msg = t(
        "tiles.variables.confirm_remove_message", name=pending_remove or ""
    )
    if _pending_is_base:
        _confirm_msg += " " + t("tiles.variables.confirm_remove_base_warning")
    ConfirmDialog(
        open=pending_remove is not None,
        on_cancel=lambda: set_pending_remove(None),
        on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
        title=t("tiles.variables.confirm_remove_title"),
        message=_confirm_msg,
        confirm_label=t("common.remove"),
    )

    # Duplicate-add confirmation — warns when the replaced variable was already
    # downloaded (status resets to cloud) or backs the base raster.
    _add_key = entry_key(pending_add) if pending_add else None
    _add_old = p.raw_variables.get(_add_key) if (p and _add_key) else None
    _replace_msg = t("tiles.variables.confirm_replace_message", key=_add_key or "")
    if _add_old is not None and type(_add_old).__name__ != "GEEVar":
        _replace_msg += " " + t("tiles.variables.confirm_replace_downloaded_warning")
    if (
        _add_old is not None
        and p.base_raster is not None
        and p.base_raster.name == _add_old.name
    ):
        _replace_msg += " " + t("tiles.variables.confirm_replace_base_warning")
    ConfirmDialog(
        open=pending_add is not None,
        on_cancel=lambda: set_pending_add(None),
        on_confirm=lambda: (_do_add(pending_add), set_pending_add(None)),
        title=t("tiles.variables.confirm_replace_title"),
        message=_replace_msg,
        confirm_label=t("common.replace"),
    )

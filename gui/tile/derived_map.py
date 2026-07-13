"""Shared "show on map" toggle for derived (processed) variables.

Processing and post-processing both write into ``project.processed_variables``,
and both tiles list a slice of that registry. The on-map state is therefore kept
here — module-level, like ``vars_on_map`` — so a layer toggled on in Process
still reads as "on" in Post-process (they can show the same variable) and both
tiles share one layer-key namespace.

Rendering reuses the source-variable helpers (``add_raster_var_on_map`` /
``add_vector_on_map``): processed rasters keep the variable's ``name``, so a
downloaded catalogue layer keeps its palette after alignment.
"""

import asyncio
import logging

import solara

from gui.i18n import t
from gui.scripts.map_helpers import add_vector_on_map, is_mappable
from gui.scripts.variable_map import add_raster_var_on_map

logger = logging.getLogger("spatial_risk")

# Keys of processed variables currently displayed on the map (drives the toggle
# state in every DerivedVariableList).
derived_on_map = solara.reactive(set())


def derived_layer_key(key: str) -> str:
    """Unique map-layer key for a processed variable."""
    return f"derived_{key}"


def drop_derived_from_map(key: str, map_) -> None:
    """Remove a processed variable's layer and forget its on-map state."""
    if map_ is not None:
        map_.remove_layer(derived_layer_key(key), none_ok=True)
    if key in derived_on_map.value:
        remaining = set(derived_on_map.value)
        remaining.discard(key)
        derived_on_map.set(remaining)


def use_derived_map_toggle(project, map_, process_error):
    """Hook: an ``on_toggle_map(key)`` callback for processed variables.

    Returns None when there is no map (the caller then renders no toggle). The
    hooks below are called unconditionally, as reacton requires.

    Every layer-add is offloaded to a worker thread — the ``TileClient`` /
    geopandas reads block, exactly like the source-variable toggle.
    """
    pending_toggle = solara.use_reactive(None)

    @solara.lab.use_task(dependencies=None, raise_error=False)
    async def _apply_map_toggle():
        key = pending_toggle.value
        if key is None or map_ is None:
            return
        p = project.value
        var = p.processed_variables.get(key) if p is not None else None
        if var is None or not is_mappable(var):
            return
        try:
            if key in derived_on_map.value:
                drop_derived_from_map(key, map_)
                return

            layer_key = derived_layer_key(key)
            if type(var).__name__ == "LocalVectorVar":
                await asyncio.to_thread(
                    add_vector_on_map, map_, str(var.path), key, layer_key
                )
            else:  # LocalRasterVar — same palette resolution as source rasters
                await asyncio.to_thread(
                    add_raster_var_on_map,
                    map_,
                    str(var.path),
                    var=var,
                    layer_name=key,
                    key=layer_key,
                    fit_bounds=False,
                )
            derived_on_map.set(set(derived_on_map.value) | {key})
        except Exception as exc:
            logger.exception("map toggle failed for processed var %s", key)
            process_error.set(t("tiles.variables.error_toggle_map", key=key, exc=exc))

    def on_toggle_map(key: str):
        pending_toggle.set(key)
        _apply_map_toggle()

    return on_toggle_map if map_ is not None else None

"""Shared "show on map" toggle for derived (processed) variables.

Processing and post-processing both write into ``project.processed_variables``,
and both tiles list a slice of that registry. The on-map state is therefore kept
here — module-level, like ``vars_on_map`` — so a layer toggled on in Process
still reads as "on" in Post-process (they can show the same variable) and both
tiles share one layer-key namespace.

Rendering reuses the source-variable helpers (``add_raster_var_on_map`` /
``add_vector_on_map``): processed rasters keep the variable's ``name``, so a
downloaded catalogue layer keeps its palette after alignment. The *displayed*
name is prefixed with an origin marker (``[H]`` / ``[D]``, see
``gui.scripts.layer_labels``) because a raw variable and its harmonized
counterpart share a registry key and would otherwise be indistinguishable in
the layer control.
"""

import asyncio
import logging

import solara

from gui.i18n import t
from gui.scripts.layer_labels import processed_layer_label
from gui.scripts.map_helpers import add_vector_on_map, is_mappable
from gui.scripts.notify_bridge import ERROR_TOAST_TIMEOUT
from gui.scripts.variable_map import add_raster_var_on_map

logger = logging.getLogger("spatial_risk")

# Keys of processed variables currently displayed on the map (drives the toggle
# state in every DerivedVariableList).
derived_on_map = solara.reactive(set())


def derived_layer_key(key: str) -> str:
    """Unique map-layer key for a processed variable."""
    return f"derived_{key}"


def _derived_legend(key: str, var):
    """The legend a processed-variable raster publishes while it is on the map.

    Processed rasters are always local files, so the style resolver is the only
    source needed — post-process outputs (edge/dist/loss/gain) get their
    QGIS ramp and class labels from it.
    """
    from gui.scripts.legend_data import Label, variable_spec_from_style
    from gui.scripts.legend_registry import LayerLegend
    from gui.scripts.variable_styles import resolve_variable_style

    label = Label(literal=getattr(var, "name", "") or key)
    return LayerLegend(
        layer_id=derived_layer_key(key),
        label=label,
        spec=variable_spec_from_style(resolve_variable_style(var), var, label),
    )


def drop_derived_from_map(key: str, map_, legend_port=None) -> None:
    """Remove a processed variable's layer, legend, and on-map state.

    The single removal chokepoint for processed variables: the toggle's
    off-branch and ``process_actions``' delete path both route through it.
    ``legend_port`` may be None — that is a no-op, not a crash.
    """
    if map_ is not None:
        map_.remove_layer(derived_layer_key(key), none_ok=True)
    if legend_port is not None:
        legend_port.unregister(derived_layer_key(key))
    if key in derived_on_map.value:
        remaining = set(derived_on_map.value)
        remaining.discard(key)
        derived_on_map.set(remaining)


def use_derived_map_toggle(project, map_, notifier, legend_port=None):
    """Hook: an ``on_toggle_map(key)`` callback for processed variables.

    Returns None when there is no map (the caller then renders no toggle). The
    hooks below are called unconditionally, as reacton requires.

    Every layer-add is offloaded to a worker thread — the ``TileClient`` /
    geopandas reads block, exactly like the source-variable toggle. ``notifier``
    is passed in rather than resolved with ``use_notifications()`` so the hook
    stays usable from tests with no NotificationProvider mounted. ``legend_port``
    is a ``LegendPort`` (see ``gui/scripts/legend_registry.py``); None disables
    legend publication.
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
                drop_derived_from_map(key, map_, legend_port)
                return

            layer_key = derived_layer_key(key)
            label = processed_layer_label(p, key)
            generation = legend_port.generation() if legend_port is not None else None
            legend = None
            if type(var).__name__ == "LocalVectorVar":
                await asyncio.to_thread(
                    add_vector_on_map, map_, str(var.path), label, layer_key
                )
            else:  # LocalRasterVar — same palette resolution as source rasters
                await asyncio.to_thread(
                    add_raster_var_on_map,
                    map_,
                    str(var.path),
                    var=var,
                    layer_name=label,
                    key=layer_key,
                    fit_bounds=False,
                )
                legend = _derived_legend(key, var)

            # A project switch during the await means this layer is stale (see
            # VariablesTile's add branch for the same guard) — take it back off
            # rather than publish a legend for it. Kept outside `finally`: a
            # `return` inside `finally` would discard an in-flight exception.
            if legend_port is not None and legend_port.generation() != generation:
                map_.remove_layer(layer_key, none_ok=True)
                return

            derived_on_map.set(set(derived_on_map.value) | {key})
            if legend is not None and legend_port is not None:
                legend_port.register(legend)
        except Exception as exc:
            logger.exception("map toggle failed for processed var %s", key)
            notifier.error(
                t("tiles.variables.error_toggle_map", key=key, exc=exc),
                timeout=ERROR_TOAST_TIMEOUT,
            )

    def on_toggle_map(key: str):
        pending_toggle.set(key)
        _apply_map_toggle()

    return on_toggle_map if map_ is not None else None

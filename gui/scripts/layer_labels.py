"""Origin markers for variable layer names on the map.

A raw variable and its harmonized counterpart share a registry key by design:
``add_as_processed`` computes the storage key exactly as ``add_as_raw`` does,
and ``reproject_and_match`` leaves ``name`` unchanged. Both "show on map" paths
used to pass that bare key through as the layer label, so the layer control
showed two identical entries. These helpers prefix a two-character marker so
the origin is visible.

Markers are hard-coded ASCII and deliberately not routed through ``i18n.t()``:
layer names are data identifiers, and a name that changed with the UI locale
would be worse than an English marker.

This module must not import anything under ``gui.tile`` at module level —
``process_actions`` reaches back into ``gui.tile.derived_map`` through a lazy
in-function import, and a module-level import here would close that cycle.
"""

from gui.scripts.process_actions import postprocess_output_keys

MARKER_RAW = "[R]"
MARKER_HARMONIZED = "[H]"
MARKER_DERIVED = "[D]"


def raw_layer_label(key: str) -> str:
    """Display name for a source variable's map layer."""
    return f"{MARKER_RAW} {key}"


def processed_layer_label(project, key: str) -> str:
    """Display name for a processed variable's map layer.

    Harmonization and post-processing both write into
    ``project.processed_variables``, so the two are told apart with the same
    predicate the Derived-layers tab uses to slice that registry. Sharing it
    means the marker can never disagree with the tab a variable is listed under.

    Falls back to the harmonized marker when the project or the key is missing —
    the toggle resolves ``project.value``, which can be None mid-teardown.
    """
    if project is not None and key in postprocess_output_keys(project):
        return f"{MARKER_DERIVED} {key}"
    return f"{MARKER_HARMONIZED} {key}"

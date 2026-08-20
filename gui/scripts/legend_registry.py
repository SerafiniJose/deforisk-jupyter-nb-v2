"""Which layer legends are currently published, as pure tuple operations.

The reactive lives on ``AppState``; the transformations live here so they can be
tested without Solara. ``layer_id`` is always the map-layer key the layer was
added under (``_pred_layer_key``, ``density_layer_key``, the variable tile's
``layer_key``), so registry and map cannot drift apart.
"""

from dataclasses import dataclass
from typing import Callable, Tuple

from gui.scripts.legend_data import Label, LegendSpec


@dataclass(frozen=True)
class LayerLegend:
    """One map layer's published legend.

    Args:
        layer_id: The map-layer key; also the dropdown option value.
        label: Dropdown text — a literal for user-named layers, a catalogue
            ``label_key`` for predefined variables.
        spec: What to render, before translation.
    """

    layer_id: str
    label: Label
    spec: LegendSpec


def upsert(
    current: Tuple[LayerLegend, ...], *new: LayerLegend
) -> Tuple[LayerLegend, ...]:
    """Replace same-id entries in place and append the rest, order preserved."""
    by_id = {entry.layer_id: entry for entry in current}
    by_id.update({entry.layer_id: entry for entry in new})
    return tuple(by_id.values())


def remove(
    current: Tuple[LayerLegend, ...], *layer_ids: str
) -> Tuple[LayerLegend, ...]:
    """Drop the named entries; unknown ids are ignored."""
    dropped = set(layer_ids)
    return tuple(entry for entry in current if entry.layer_id not in dropped)


def next_selection(remaining: Tuple[LayerLegend, ...], previous: str) -> str:
    """Keep ``previous`` if it survives, else select the last entry (or none)."""
    if any(entry.layer_id == previous for entry in remaining):
        return previous
    return remaining[-1].layer_id if remaining else ""


@dataclass(frozen=True)
class LegendPort:
    """A tile's explicit handle on the legend registry.

    Tiles take this as an argument rather than importing the ``app_state``
    singleton, per the project's tile contract (tiles receive reactives and
    handles as explicit args). ``gui/solara_app.py`` owns the singleton and
    passes ``app_state.legend_port`` down.

    Args:
        register: Publish (or replace) legends; the newest becomes selected.
        unregister: Withdraw legends by map-layer key.
        generation: The current project-load generation. Toggles that add a
            layer across an ``await`` capture it before and re-check it after,
            so a project switch mid-add cannot publish a stale legend.
    """

    register: Callable[..., None]
    unregister: Callable[..., None]
    generation: Callable[[], int]

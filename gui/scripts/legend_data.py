"""Language-neutral legend specifications and their pysepal rendering.

The registry (``gui/scripts/legend_registry.py``) stores ``LegendSpec`` objects,
not translated strings: ``Page()`` converts the selected spec to pysepal's
``LegendData`` on every render, so legends re-translate live when the locale
changes. Builders in this module therefore emit i18n *keys* (``Label.key``) and
never import ``gui.i18n``.

Kept free of Solara / ipyvuetify / ipyleaflet / localtileserver so it stays
unit-testable, like its ``*_styles.py`` siblings.
"""

from dataclasses import dataclass, field
from typing import Callable, Tuple

#: How many stops a gradient is sampled into. pysepal's Legend.vue spaces the
#: supplied colours evenly ((i / (len - 1)) * 100), so a coarse sample would
#: smear the QGIS ramps, whose nodes sit far from even positions (the MW palette
#: puts three stops inside the first 3% of 1..65535). 256 mirrors the same LUT
#: size localtileserver renders the tiles from, so legend and tiles agree.
GRADIENT_STOPS = 256


@dataclass(frozen=True)
class Label:
    """A piece of legend text: either an i18n key or a ready-made literal.

    ``args`` is a tuple of ``(name, value)`` pairs rather than a dict so the
    dataclass stays hashable/frozen; it is expanded into ``t(key, **args)``.
    """

    key: str = ""
    literal: str = ""
    args: Tuple[Tuple[str, object], ...] = ()


@dataclass(frozen=True)
class LegendSpec:
    """What to draw for one layer, before translation.

    ``kind``:
        * ``"gradient"`` — a colour bar; ``colors`` are its stops and ``labels``
          its two endpoints.
        * ``"chips"``    — one colour chip per class; ``colors`` and ``labels``
          are zipped pairwise.
        * ``"note"``     — no colours at all, just ``labels`` as text rows (used
          for GEE ``randomVisualizer`` layers, whose colours are assigned
          server-side at random and cannot be shown).
    """

    kind: str
    title: Label = field(default_factory=Label)
    colors: Tuple[str, ...] = ()
    labels: Tuple[Label, ...] = ()


def resolve_label(label: Label, t: Callable[..., str]) -> str:
    """Translate one ``Label``; a key wins over a literal."""
    if label.key:
        return t(label.key, **dict(label.args))
    return label.literal


def gradient_colors(cmap, n: int = GRADIENT_STOPS) -> Tuple[str, ...]:
    """Sample a matplotlib ``Colormap`` into ``n`` evenly spaced hex stops."""
    from matplotlib.colors import to_hex

    if n < 2:
        raise ValueError("a gradient needs at least two stops")
    return tuple(to_hex(cmap(i / (n - 1))) for i in range(n))


def to_legend_data(spec: LegendSpec, t: Callable[..., str]):
    """Build a FRESH pysepal ``LegendData`` from ``spec``.

    Always constructs new objects: ``LegendData`` holds mutable lists, and a
    shared/mutated instance would trip reacton's prop-equality bailout.
    """
    from pysepal.solara.components.legend import (
        DiscreteEntry,
        GradientEntry,
        LegendData,
    )

    title = resolve_label(spec.title, t)

    if spec.kind == "gradient":
        return LegendData(
            gradients=[
                GradientEntry(
                    colors=list(spec.colors),
                    labels=[resolve_label(label, t) for label in spec.labels],
                    title=title,
                )
            ]
        )

    # chips / note — a chip-less first row carries the title, which DiscreteEntry
    # renders as a plain text row (pysepal's own "totals row" idiom).
    items = [DiscreteEntry(title, "")] if title else []
    for index, label in enumerate(spec.labels):
        color = spec.colors[index] if index < len(spec.colors) else ""
        items.append(DiscreteEntry(resolve_label(label, t), color))
    return LegendData(items=items)

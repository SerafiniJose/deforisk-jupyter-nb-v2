"""Registered sample-sets table widget."""

import logging
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t

logger = logging.getLogger("spatial_risk")

_GRID = "display:grid;grid-template-columns:1fr 1fr 1fr 70px 70px;align-items:center;width:100%;"
_HEADER_EXTRA = (
    "padding:4px 8px 6px;border-bottom:2px solid rgba(0,0,0,0.15);"
    "font-size:0.72rem;font-weight:600;color:grey;"
    "text-transform:uppercase;letter-spacing:0.05em;"
)
_ROW_EXTRA = "padding:5px 8px;border-bottom:1px solid rgba(0,0,0,0.08);"
_CELL_FLEX = "display:flex;align-items:center;gap:4px;"
_CELL_RIGHT = "display:flex;align-items:center;justify-content:flex-end;gap:0;"


@solara.component
def SampleSetList(
    project,
    on_map=frozenset(),
    on_toggle_map: Optional[Callable[[str], None]] = None,
    on_remove: Optional[Callable[[str], None]] = None,
):
    """Table of registered sample sets with add-to-map and remove actions."""
    p = project.value
    if p is None or not p.samples:
        solara.Text(t("widgets.sample_set_list.empty"), style="color:grey;")
        return

    with solara.Column(style="gap:0;width:100%;"):
        with rv.Html(tag="div", style_=_GRID + _HEADER_EXTRA):
            rv.Html(tag="span", children=[t("widgets.sample_set_list.col_name")])
            rv.Html(tag="span", children=[t("widgets.sample_set_list.col_strategy")])
            rv.Html(tag="span", children=[t("widgets.sample_set_list.col_points")])
            rv.Html(tag="span", children=[t("widgets.sample_set_list.col_map")])
            rv.Html(tag="span", children=[""])

        for key, s in p.samples.items():
            alloc = f" / {s.allocation}" if s.allocation else ""
            strat = f"{s.strategy}{alloc}"
            counts = ", ".join(f"{k}:{v}" for k, v in sorted(s.class_counts.items()))
            points = f"{s.n_total} ({counts})" if counts else str(s.n_total)
            is_on = key in on_map

            with rv.Html(tag="div", style_=_GRID + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(key)
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(strat, style="font-size:0.8rem;")
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(points, style="font-size:0.8rem;")
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    if on_toggle_map is not None:
                        solara.Button(
                            "",
                            icon_name="mdi-map-marker" if is_on else "mdi-map-marker-outline",
                            on_click=lambda *_, k=key: on_toggle_map(k),
                            icon=True, text=True, x_small=True,
                            color="primary" if is_on else None,
                        )
                with rv.Html(tag="div", style_=_CELL_RIGHT):
                    if on_remove is not None:
                        solara.Button(
                            "",
                            icon_name="mdi-delete-outline",
                            on_click=lambda *_, k=key: on_remove(k),
                            icon=True, text=True, x_small=True,
                        )

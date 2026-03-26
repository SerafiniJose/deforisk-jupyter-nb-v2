"""Registered datasets table widget."""

import logging
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

logger = logging.getLogger("spatial_risk")

_GRID = "display:grid;grid-template-columns:1fr 1fr 60px 60px 60px;align-items:center;width:100%;"
_HEADER_EXTRA = (
    "padding:4px 8px 6px;"
    "border-bottom:2px solid rgba(0,0,0,0.15);"
    "font-size:0.72rem;font-weight:600;color:grey;"
    "text-transform:uppercase;letter-spacing:0.05em;"
)
_ROW_EXTRA = "padding:5px 8px;border-bottom:1px solid rgba(0,0,0,0.08);"
_CELL_FLEX = "display:flex;align-items:center;gap:4px;"
_CELL_RIGHT = "display:flex;align-items:center;justify-content:flex-end;gap:0;"


@solara.component
def DatasetList(
    project,
    on_edit: Optional[Callable[[str], None]] = None,
    on_remove: Optional[Callable[[str], None]] = None,
):
    """Table of registered datasets with edit and remove actions."""
    p = project.value
    if p is None or not p.datasets:
        solara.Text("No datasets registered yet.", style="color:grey;")
        return

    with solara.Column(style="gap:0;width:100%;"):
        with rv.Html(tag="div", style_=_GRID + _HEADER_EXTRA):
            rv.Html(tag="span", children=["Name"])
            rv.Html(tag="span", children=["Target"])
            rv.Html(tag="span", children=["Feats"])
            rv.Html(tag="span", children=["Year"])
            rv.Html(tag="span", children=[""])

        for key, ds in p.datasets.items():
            target_name = ds.target.name if ds.target else "—"
            feat_count = str(len(ds.features))
            year_label = str(ds.year) if ds.year else "—"

            with rv.Html(tag="div", style_=_GRID + _ROW_EXTRA):
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(key)
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    rv.Chip(children=[target_name], x_small=True, outlined=True, color="error")
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    rv.Chip(children=[feat_count], x_small=True, outlined=True)
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(year_label, style="font-size:0.85rem;")
                with rv.Html(tag="div", style_=_CELL_RIGHT):
                    if on_edit is not None:
                        solara.Button(
                            "",
                            icon_name="mdi-pencil-outline",
                            on_click=lambda *_, k=key: on_edit(k),
                            icon=True,
                            x_small=True,
                        )
                    if on_remove is not None:
                        solara.Button(
                            "",
                            icon_name="mdi-delete-outline",
                            on_click=lambda *_, k=key: on_remove(k),
                            icon=True,
                            x_small=True,
                        )

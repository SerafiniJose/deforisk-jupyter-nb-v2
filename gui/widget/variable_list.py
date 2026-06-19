"""Source and Derived variable list widgets."""

import logging
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

from gui.scripts.map_helpers import is_mappable

logger = logging.getLogger("spatial_risk")

_GRID = "display:grid;grid-template-columns:1fr 90px 70px 116px;align-items:center;width:100%;column-gap:16px;"
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
def SourceVariableList(
    project,
    on_remove: Callable[[str], None],
    on_edit: Optional[Callable[[str], None]] = None,
    on_toggle_map: Optional[Callable[[str], None]] = None,
    vars_on_map=None,
):
    """Table of source (raw) variables with map-toggle, edit, and remove actions."""
    p = project.value
    logger.debug(
        "SourceVariableList render — raw_variables: %s",
        list(p.raw_variables.keys()) if p else "no project",
    )

    if p is None or not p.raw_variables:
        solara.Text("No variables added yet.", style="color: grey;")
        return

    with solara.Column(style="gap:0;width:100%;"):
        # Header row
        with rv.Html(tag="div", style_=_GRID + _HEADER_EXTRA):
            rv.Html(tag="span", children=["Name"])
            rv.Html(tag="span", children=["Type"])
            rv.Html(tag="span", children=["Year"])
            rv.Html(tag="span", children=[""])

        # Data rows
        for key, var in p.raw_variables.items():
            is_base = p.base_raster is not None and p.base_raster.name == var.name
            data_type_label = var.data_type if isinstance(var.data_type, str) else var.data_type.value

            with rv.Html(tag="div", style_=_GRID + _ROW_EXTRA):
                # Name
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(var.name)
                    if is_base:
                        rv.Chip(children=["base"], x_small=True, color="info")
                # Type
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    rv.Chip(children=[data_type_label], x_small=True, outlined=True, color="primary")
                # Year
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(str(var.year) if var.year else "—", style="color:grey;")
                # Actions — right-aligned
                with rv.Html(tag="div", style_=_CELL_RIGHT):
                    # Map toggle — GEE-backed images and local raster/vector files.
                    if on_toggle_map is not None and is_mappable(var):
                        on_map = vars_on_map.value if vars_on_map is not None else set()
                        is_on = key in on_map
                        solara.Button(
                            "",
                            icon_name="mdi-map-minus" if is_on else "mdi-map-plus",
                            on_click=lambda *_, k=key: on_toggle_map(k),
                            icon=True,
                            text=True,
                            x_small=True,
                            color="primary" if is_on else "grey darken-1",
                        )
                    if on_edit is not None:
                        solara.Button(
                            "",
                            icon_name="mdi-pencil-outline",
                            on_click=lambda *_, k=key: on_edit(k),
                            icon=True,
                            text=True,
                            x_small=True,
                        )
                    solara.Button(
                        "",
                        icon_name="mdi-delete-outline",
                        on_click=lambda *_, k=key: on_remove(k),
                        icon=True,
                        text=True,
                        x_small=True,
                    )


@solara.component
def DerivedVariableList(project, on_remove: Optional[Callable[[str], None]] = None):
    """Collapsible table of derived (processed) variables with a remove action."""
    collapsed, set_collapsed = solara.use_state(False)

    p = project.value
    if p is None or not p.processed_variables:
        return

    count = len(p.processed_variables)
    _DGRID = "display:grid;grid-template-columns:1fr 120px 80px 56px;align-items:center;width:100%;"

    with solara.Column(style="gap:0;width:100%;"):
        with solara.Row(style="align-items:center;gap:8px;padding:4px 0;"):
            solara.Text(
                f"DERIVED VARIABLES ({count})",
                style="font-weight:600;font-size:0.8rem;color:grey;",
            )
            solara.Button(
                "",
                icon_name="mdi-chevron-up" if not collapsed else "mdi-chevron-down",
                on_click=lambda: set_collapsed(not collapsed),
                icon=True,
                text=True,
                x_small=True,
            )

        if not collapsed:
            with rv.Html(tag="div", style_=_DGRID + _HEADER_EXTRA):
                rv.Html(tag="span", children=["Name"])
                rv.Html(tag="span", children=["Source"])
                rv.Html(tag="span", children=["Status"])
                rv.Html(tag="span", children=[""])

            for key, var in p.processed_variables.items():
                source_name = next(
                    (k for k, raw_var in p.raw_variables.items() if var.name.startswith(raw_var.name)),
                    "unknown",
                )
                with rv.Html(tag="div", style_=_DGRID + _ROW_EXTRA):
                    with rv.Html(tag="div"):
                        solara.Text(var.name, style="font-size:0.9rem;")
                    with rv.Html(tag="div", style_=_CELL_FLEX):
                        rv.Chip(children=[source_name], x_small=True, outlined=True)
                    with rv.Html(tag="div", style_=_CELL_FLEX):
                        rv.Chip(children=["ready"], color="success", x_small=True, outlined=True)
                    # Actions — delete (also removes the generated file from disk)
                    with rv.Html(tag="div", style_=_CELL_RIGHT):
                        if on_remove is not None:
                            solara.Button(
                                "",
                                icon_name="mdi-delete-outline",
                                on_click=lambda *_, k=key: on_remove(k),
                                icon=True,
                                text=True,
                                x_small=True,
                            )

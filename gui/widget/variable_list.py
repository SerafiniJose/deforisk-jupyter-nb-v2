"""Source and Derived variable list widgets."""

import logging
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

logger = logging.getLogger("spatial_risk")

_GRID = "display:grid;grid-template-columns:1fr 90px 90px 80px;align-items:center;width:100%;"
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
):
    """Table of source (raw) variables with edit and remove actions."""
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
            rv.Html(tag="span", children=["Status"])
            rv.Html(tag="span", children=[""])

        # Data rows
        for key, var in p.raw_variables.items():
            is_base = p.base_raster is not None and p.base_raster.name == var.name
            processed_vars = p.processed_variables
            derived = [k for k, pv in processed_vars.items() if pv.name.startswith(var.name)]
            is_processed = key in processed_vars or bool(derived)
            data_type_label = var.data_type if isinstance(var.data_type, str) else var.data_type.value
            status_label = "ready" if is_processed else "pending"
            status_color = "success" if is_processed else "warning"

            with rv.Html(tag="div", style_=_GRID + _ROW_EXTRA):
                # Name
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    solara.Text(var.name)
                    if is_base:
                        rv.Chip(children=["base"], x_small=True, color="info")
                # Type
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    rv.Chip(children=[data_type_label], x_small=True, outlined=True, color="primary")
                # Status
                with rv.Html(tag="div", style_=_CELL_FLEX):
                    rv.Chip(children=[status_label], color=status_color, x_small=True, outlined=True)
                    if derived:
                        rv.Chip(children=[f"+{len(derived)}"], x_small=True, outlined=True)
                # Actions — right-aligned
                with rv.Html(tag="div", style_=_CELL_RIGHT):
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
def DerivedVariableList(project):
    """Collapsible table of derived (processed) variables."""
    collapsed, set_collapsed = solara.use_state(False)

    p = project.value
    if p is None or not p.processed_variables:
        return

    count = len(p.processed_variables)
    _DGRID = "display:grid;grid-template-columns:1fr 120px 80px;align-items:center;width:100%;"

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

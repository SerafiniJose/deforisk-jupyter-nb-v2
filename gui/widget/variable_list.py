"""Source and Derived variable list widgets."""

import logging
from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

logger = logging.getLogger("spatial_risk")

_ROW_STYLE = (
    "display: grid;"
    "grid-template-columns: 1fr 90px 90px 72px;"
    "align-items: center;"
    "padding: 6px 8px;"
    "border-bottom: 1px solid rgba(0,0,0,0.08);"
)
_HEADER_STYLE = (
    "display: grid;"
    "grid-template-columns: 1fr 90px 90px 72px;"
    "align-items: center;"
    "padding: 4px 8px 6px;"
    "border-bottom: 2px solid rgba(0,0,0,0.15);"
    "font-size: 0.72rem;"
    "font-weight: 600;"
    "color: grey;"
    "text-transform: uppercase;"
    "letter-spacing: 0.05em;"
)
_ACTIONS_STYLE = "display: flex; justify-content: flex-end; gap: 0;"


@solara.component
def SourceVariableList(
    project,
    on_remove: Callable[[str], None],
    on_edit: Optional[Callable[[str], None]] = None,
):
    """Table of source (raw) variables with edit and remove actions.

    Args:
        project: Reactive holding the current Project (or None).
        on_remove: Callback receiving the variable key to remove.
        on_edit: Optional callback receiving the variable key to edit.
    """
    p = project.value
    logger.debug(
        "SourceVariableList render — raw_variables: %s",
        list(p.raw_variables.keys()) if p else "no project",
    )

    if p is None or not p.raw_variables:
        solara.Text("No variables added yet.", style="color: grey;")
        return

    with solara.Column(style="gap: 0; width: 100%;"):
        # Header
        solara.HTML(
            tag="div",
            style=_HEADER_STYLE,
            unsafe_innerHTML=(
                "<span>Name</span>"
                "<span>Type</span>"
                "<span>Status</span>"
                "<span></span>"
            ),
        )

        for key, var in p.raw_variables.items():
            is_base = p.base_raster is not None and p.base_raster.name == var.name
            processed_vars = p.processed_variables
            derived = [k for k, pv in processed_vars.items() if pv.name.startswith(var.name)]
            is_processed = key in processed_vars or bool(derived)
            data_type_label = var.data_type if isinstance(var.data_type, str) else var.data_type.value
            status_label = "ready" if is_processed else "pending"
            status_color = "success" if is_processed else "warning"

            with solara.Row(style=_ROW_STYLE):
                # Name + base chip
                with solara.Row(style="align-items: center; gap: 4px; flex-wrap: nowrap;"):
                    solara.Text(var.name, style="font-weight: 500;")
                    if is_base:
                        rv.Chip(children=["base"], x_small=True, color="info")

                # Type chip
                rv.Chip(children=[data_type_label], x_small=True, outlined=True, color="primary")

                # Status chip
                with solara.Row(style="align-items: center; gap: 4px;"):
                    rv.Chip(children=[status_label], color=status_color, x_small=True, outlined=True)
                    if derived:
                        rv.Chip(children=[f"+{len(derived)}"], x_small=True, outlined=True)

                # Actions — right-aligned
                with solara.Row(style=_ACTIONS_STYLE):
                    if on_edit is not None:
                        solara.Button(
                            "",
                            icon_name="mdi-pencil-outline",
                            on_click=lambda *_, k=key: on_edit(k),
                            icon=True,
                            small=True,
                        )
                    solara.Button(
                        "",
                        icon_name="mdi-delete-outline",
                        on_click=lambda *_, k=key: on_remove(k),
                        icon=True,
                        small=True,
                    )


@solara.component
def DerivedVariableList(project):
    """Collapsible table of derived (processed) variables.

    Args:
        project: Reactive holding the current Project (or None).
    """
    collapsed, set_collapsed = solara.use_state(False)

    p = project.value
    if p is None or not p.processed_variables:
        return

    count = len(p.processed_variables)

    with solara.Column(style="gap: 0; width: 100%;"):
        with solara.Row(style="align-items: center; gap: 8px; padding: 4px 0;"):
            solara.Text(
                f"DERIVED VARIABLES ({count})",
                style="font-weight: 600; font-size: 0.8rem; color: grey;",
            )
            solara.Button(
                "",
                icon_name="mdi-chevron-up" if not collapsed else "mdi-chevron-down",
                on_click=lambda: set_collapsed(not collapsed),
                icon=True,
                small=True,
            )

        if not collapsed:
            _DERIVED_ROW = (
                "display: grid;"
                "grid-template-columns: 1fr 120px 80px;"
                "align-items: center;"
                "padding: 5px 8px;"
                "border-bottom: 1px solid rgba(0,0,0,0.06);"
            )
            _DERIVED_HEADER = (
                "display: grid;"
                "grid-template-columns: 1fr 120px 80px;"
                "align-items: center;"
                "padding: 4px 8px 6px;"
                "border-bottom: 2px solid rgba(0,0,0,0.15);"
                "font-size: 0.72rem; font-weight: 600; color: grey;"
                "text-transform: uppercase; letter-spacing: 0.05em;"
            )
            solara.HTML(
                tag="div",
                style=_DERIVED_HEADER,
                unsafe_innerHTML="<span>Name</span><span>Source</span><span>Status</span>",
            )
            for key, var in p.processed_variables.items():
                source_name = next(
                    (k for k, raw_var in p.raw_variables.items() if var.name.startswith(raw_var.name)),
                    "unknown",
                )
                with solara.Row(style=_DERIVED_ROW):
                    solara.Text(var.name, style="font-size: 0.9rem;")
                    rv.Chip(children=[source_name], x_small=True, outlined=True)
                    rv.Chip(children=["ready"], color="success", x_small=True, outlined=True)

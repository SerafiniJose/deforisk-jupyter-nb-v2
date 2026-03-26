"""Source and Derived variable list widgets."""

import logging
from typing import Callable

import reacton.ipyvuetify as rv
import solara

logger = logging.getLogger("spatial_risk")


@solara.component
def SourceVariableList(project, on_remove: Callable[[str], None]):
    """List of source (raw) variables with status chips and remove button.

    Args:
        project: Reactive holding the current Project (or None).
        on_remove: Callback receiving the variable key to remove.
    """
    p = project.value
    logger.debug(
        "SourceVariableList render — raw_variables: %s",
        list(p.raw_variables.keys()) if p else "no project",
    )

    if p is None or not p.raw_variables:
        solara.Text("No variables added yet.", style="color: grey;")
        return

    with solara.Column(style="gap: 4px;"):
        for key, var in p.raw_variables.items():
            is_base = p.base_raster is not None and p.base_raster.name == var.name
            processed_vars = p.processed_variables
            derived = [k for k, pv in processed_vars.items() if pv.name.startswith(var.name)]
            is_processed = key in processed_vars or bool(derived)

            data_type_label = var.data_type if isinstance(var.data_type, str) else var.data_type.value

            with solara.Row(
                key=key,
                style="align-items: center; gap: 6px; flex-wrap: wrap; padding: 4px 0; border-bottom: 1px solid rgba(0,0,0,0.08);",
            ):
                solara.Text(var.name, style="font-weight: 500; min-width: 80px;")
                rv.Chip(children=[data_type_label], x_small=True, outlined=True, color="primary")
                if is_base:
                    rv.Chip(children=["base"], x_small=True, color="info")
                rv.Chip(
                    children=["ready" if is_processed else "pending"],
                    color="success" if is_processed else "warning",
                    x_small=True,
                    outlined=True,
                )
                if derived:
                    rv.Chip(children=[f"→ {len(derived)} derived"], x_small=True, outlined=True)
                solara.Button(
                    "",
                    icon_name="mdi-delete-outline",
                    on_click=lambda *_, k=key: on_remove(k),
                    icon=True,
                    small=True,
                    style="margin-left: auto;",
                )


@solara.component
def DerivedVariableList(project):
    """Collapsible list of derived (processed) variables.

    Args:
        project: Reactive holding the current Project (or None).
    """
    collapsed, set_collapsed = solara.use_state(False)

    p = project.value
    if p is None or not p.processed_variables:
        return

    count = len(p.processed_variables)

    with solara.Column(style="gap: 4px;"):
        with solara.Row(style="align-items: center; gap: 8px;"):
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
            with solara.Column(style="gap: 4px;"):
                for key, var in p.processed_variables.items():
                    source_name = next(
                        (k for k, raw_var in p.raw_variables.items() if var.name.startswith(raw_var.name)),
                        "unknown",
                    )
                    with solara.Row(
                        key=key,
                        style="align-items: center; gap: 6px; flex-wrap: wrap; padding: 4px 0; border-bottom: 1px solid rgba(0,0,0,0.06);",
                    ):
                        solara.Text(var.name, style="font-size: 0.9rem; min-width: 80px;")
                        rv.Chip(children=[f"from: {source_name}"], x_small=True, outlined=True)
                        rv.Chip(children=["ready"], color="success", x_small=True, outlined=True)

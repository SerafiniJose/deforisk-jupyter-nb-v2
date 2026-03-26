"""Source and Derived variable list widgets."""

from typing import Callable

import ipyvuetify as v
import reacton.ipyvuetify as rv
import solara

from spatialrisk.variables.models import DataType


def _status_chip(var) -> v.Chip:
    processed = var.name in (var.project.processed_variables if var.project else {})
    color = "success" if processed else "warning"
    label = "ready" if processed else "pending"
    return v.Chip(children=[label], color=color, x_small=True, outlined=True)


@solara.component
def SourceVariableList(project, on_remove: Callable[[str], None]):
    """List of source (raw) variables with status chips and remove button.

    Args:
        project: Reactive holding the current Project (or None).
        on_remove: Callback receiving the variable key to remove.
    """
    p = project.value
    if p is None or not p.raw_variables:
        solara.Text("No variables added yet.", style="color: grey;")
        return

    with rv.List(dense=True):
        for key, var in p.raw_variables.items():
            is_base = p.base_raster is not None and p.base_raster.name == var.name
            processed_vars = p.processed_variables
            derived = [
                k for k, pv in processed_vars.items()
                if pv.name.startswith(var.name)
            ]
            is_processed = key in processed_vars or bool(derived)

            with rv.ListItem(key=key):
                with rv.ListItemContent():
                    with solara.Row(style="align-items: center; gap: 8px; flex-wrap: wrap;"):
                        solara.Text(var.name, style="font-weight: 500;")
                        v.Chip(
                            children=[var.data_type if isinstance(var.data_type, str) else var.data_type.value],
                            x_small=True, outlined=True, color="primary",
                        )
                        if is_base:
                            v.Chip(children=["base"], x_small=True, color="info")
                        v.Chip(
                            children=["ready" if is_processed else "pending"],
                            color="success" if is_processed else "warning",
                            x_small=True, outlined=True,
                        )
                        if derived:
                            v.Chip(
                                children=[f"→ {len(derived)} derived"],
                                x_small=True, outlined=True,
                            )

                with rv.ListItemAction():
                    v.Btn(
                        icon=True, x_small=True,
                        children=[v.Icon(children=["mdi-delete-outline"])],
                        on_click=lambda *_, k=key: on_remove(k),
                    )


@solara.component
def DerivedVariableList(project):
    """Collapsible list of derived (processed) variables.

    Args:
        project: Reactive holding the current Project (or None).
    """
    collapsed, set_collapsed = solara.use_state(False)  # noqa: SH101

    p = project.value
    if p is None or not p.processed_variables:
        return

    count = len(p.processed_variables)
    with solara.Column(style="gap: 4px;"):
        with solara.Row(style="align-items: center; gap: 8px;"):
            solara.Text(f"DERIVED VARIABLES ({count})", style="font-weight: 600; font-size: 0.8rem; color: grey;")
            v.Btn(
                icon=True, x_small=True,
                children=[v.Icon(children=["mdi-chevron-up" if not collapsed else "mdi-chevron-down"])],
                on_click=lambda *_: set_collapsed(not collapsed),
            )

        if not collapsed:
            with rv.List(dense=True):
                for key, var in p.processed_variables.items():
                    # Find which raw var it came from
                    source_name = next(
                        (k for k, rv in p.raw_variables.items() if var.name.startswith(rv.name)),
                        "unknown",
                    )
                    with rv.ListItem(key=key):
                        with rv.ListItemContent():
                            with solara.Row(style="align-items: center; gap: 8px;"):
                                solara.Text(var.name, style="font-size: 0.9rem;")
                                v.Chip(children=[f"from: {source_name}"], x_small=True, outlined=True)
                                v.Chip(children=["ready"], color="success", x_small=True, outlined=True)

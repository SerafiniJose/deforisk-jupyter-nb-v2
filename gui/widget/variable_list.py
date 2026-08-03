"""Source and Derived variable list widgets."""

from typing import Callable, Optional

import solara

from gui.i18n import t
from gui.scripts.map_helpers import is_mappable
from gui.widget.product_table import ProductTable


def derived_source_key(p, var_name, fallback):
    """Raw-variable key a derived name traces back to.

    Change layers are named ``{op}_{source}_...`` — strip the operation prefix
    so they resolve to their start layer instead of "unknown".
    """
    base = var_name
    for prefix in ("loss_", "gain_"):
        if base.startswith(prefix):
            base = base[len(prefix) :]
            break
    return next(
        (k for k, raw_var in p.raw_variables.items() if base.startswith(raw_var.name)),
        fallback,
    )


@solara.component
def SourceVariableList(
    project,
    on_remove: Callable[[str], None],
    on_edit: Optional[Callable[[str], None]] = None,
    on_toggle_map: Optional[Callable[[str], None]] = None,
    vars_on_map=None,
    on_download: Optional[Callable[[str], None]] = None,
    download_pending: bool = False,
    downloading_key: Optional[str] = None,
):
    """Table of source (raw) variables with download/map/edit/remove actions.

    Cloud-backed variables (GEEVar) show a "cloud" chip and, when
    ``on_download`` is given, a per-row download button. ``downloading_key`` is
    the key currently downloading (None while a bulk download runs); every
    download button is disabled while ``download_pending``.
    """
    p = project.value
    raw_variables = (p.raw_variables if p is not None else {}) or {}
    on_map = vars_on_map.value if vars_on_map is not None else set()

    rows = []
    for key, var in raw_variables.items():
        is_base = p.base_raster is not None and p.base_raster.name == var.name
        data_type_label = (
            var.data_type if isinstance(var.data_type, str) else var.data_type.value
        )
        is_cloud = type(var).__name__ == "GEEVar"

        status_chip = (
            {
                "value": t("widgets.variable_list.chip_cloud"),
                "icon": "mdi-cloud-outline",
                "color": "warning",
            }
            if is_cloud
            else {"value": t("widgets.variable_list.chip_local"), "color": "success"}
        )

        actions = []
        if on_download is not None and is_cloud:
            actions.append(
                {
                    "kind": "download",
                    "on_click": lambda *_, k=key: on_download(k),
                    "loading": download_pending and downloading_key == key,
                    "disabled": download_pending,
                }
            )
        if on_toggle_map is not None and is_mappable(var):
            actions.append(
                {
                    "kind": "map_toggle",
                    "on_click": lambda *_, k=key: on_toggle_map(k),
                    "is_on": key in on_map,
                }
            )
        if on_edit is not None:
            actions.append({"kind": "edit", "on_click": lambda *_, k=key: on_edit(k)})
        actions.append({"kind": "delete", "on_click": lambda *_, k=key: on_remove(k)})

        name_chips = (
            [
                {
                    "value": t("widgets.variable_list.chip_base"),
                    "color": "info",
                    "outlined": False,
                }
            ]
            if is_base
            else []
        )
        rows.append(
            {
                "key": key,
                "cells": [
                    {"type": "text", "value": var.name, "chips": name_chips},
                    {
                        "type": "chips",
                        "items": [
                            {"value": data_type_label, "color": "primary"},
                            status_chip,
                        ],
                    },
                    {
                        "type": "text",
                        "value": str(var.year) if var.year else "—",
                        "muted": True,
                    },
                ],
                "actions": actions,
            }
        )

    ProductTable(
        title=t("widgets.variable_list.source_title"),
        columns=[
            {
                "label": t("widgets.variable_list.source_col_name"),
                "width": "minmax(0,2fr)",
            },
            {"label": t("widgets.variable_list.source_col_type"), "width": "150px"},
            {"label": t("widgets.variable_list.source_col_year"), "width": "44px"},
        ],
        rows=rows,
        empty_text=t("widgets.variable_list.source_empty"),
    )


@solara.component
def DerivedVariableList(
    project,
    on_remove: Optional[Callable[[str], None]] = None,
    keys: Optional[list] = None,
    on_toggle_map: Optional[Callable[[str], None]] = None,
    derived_on_map=None,
    title: Optional[str] = None,
):
    """Table of derived (processed) variables with map/remove actions.

    ``keys`` restricts the rows to those registry keys (None = all).
    ``derived_on_map`` is the reactive set of keys currently drawn on the map
    (see ``gui/tile/derived_map.py``), which drives the toggle state.
    """
    p = project.value
    if p is None:
        return
    variables = {
        k: v for k, v in p.processed_variables.items() if keys is None or k in keys
    }
    if not variables:
        return
    on_map = derived_on_map.value if derived_on_map is not None else set()

    rows = []
    for key, var in variables.items():
        source_name = derived_source_key(
            p, var.name, t("widgets.variable_list.derived_source_unknown")
        )
        actions = []
        if on_toggle_map is not None and is_mappable(var):
            actions.append(
                {
                    "kind": "map_toggle",
                    "on_click": lambda *_, k=key: on_toggle_map(k),
                    "is_on": key in on_map,
                }
            )
        if on_remove is not None:
            actions.append(
                {"kind": "delete", "on_click": lambda *_, k=key: on_remove(k)}
            )
        rows.append(
            {
                "key": key,
                "cells": [
                    {"type": "text", "value": var.name, "size": "0.9rem"},
                    {"type": "chip", "value": source_name},
                    {"type": "status", "status": "ready"},
                ],
                "actions": actions,
            }
        )

    ProductTable(
        title=title or t("widgets.variable_list.derived_title"),
        columns=[
            {
                "label": t("widgets.variable_list.derived_col_name"),
                "width": "minmax(0,2fr)",
            },
            {"label": t("widgets.variable_list.derived_col_source"), "width": "120px"},
            {"label": t("widgets.variable_list.derived_col_status"), "width": "90px"},
        ],
        rows=rows,
        empty_text="",
    )

"""Registered sample-sets table widget."""

import logging
from typing import Callable, Optional

import solara

from gui.i18n import t
from gui.widget.product_table import ProductTable

logger = logging.getLogger("spatial_risk")


@solara.component
def SampleSetList(
    project,
    on_map=frozenset(),
    on_toggle_map: Optional[Callable[[str], None]] = None,
    on_remove: Optional[Callable[[str], None]] = None,
    pending=frozenset(),
):
    """Table of registered sample sets with map-toggle and remove actions."""
    p = project.value
    samples = (p.samples if p is not None else {}) or {}

    rows = []
    for key, s in samples.items():
        alloc = f" / {s.allocation}" if s.allocation else ""
        counts = ", ".join(f"{k}:{v}" for k, v in sorted(s.class_counts.items()))
        points = f"{s.n_total} ({counts})" if counts else str(s.n_total)

        actions = []
        if on_toggle_map is not None:
            actions.append(
                {
                    "kind": "map_toggle",
                    "on_click": lambda *_, k=key: on_toggle_map(k),
                    "is_on": key in on_map,
                    "disabled": key in pending,
                }
            )
        if on_remove is not None:
            actions.append({"kind": "delete", "on_click": lambda *_, k=key: on_remove(k)})
        rows.append(
            {
                "key": key,
                "cells": [
                    {"type": "text", "value": key},
                    {"type": "text", "value": f"{s.strategy}{alloc}", "size": "0.8rem"},
                    {"type": "text", "value": points, "size": "0.8rem", "muted": True},
                ],
                "actions": actions,
            }
        )

    ProductTable(
        title=t("widgets.sample_set_list.title"),
        columns=[
            {"label": t("widgets.sample_set_list.col_name"), "width": "minmax(0,1fr)"},
            {"label": t("widgets.sample_set_list.col_strategy"), "width": "minmax(0,1fr)"},
            {"label": t("widgets.sample_set_list.col_points"), "width": "minmax(0,1fr)"},
        ],
        rows=rows,
        empty_text=t("widgets.sample_set_list.empty"),
    )

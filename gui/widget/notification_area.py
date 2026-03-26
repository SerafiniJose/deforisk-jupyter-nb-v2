"""Floating step-aware notification bar for the workflow panel."""

import reacton.ipyvuetify as rv
import solara


def _compute(tab, aoi_result, project, process_error, status_message, error_message):
    """Return (message, type_) for the current tab, or None if nothing to show."""
    # Global error always takes priority
    if error_message:
        return (error_message, "error")

    if tab == 0:  # AOI
        if aoi_result is not None:
            return (f"AOI selected: {aoi_result.name}", "success")

    elif tab == 1:  # Variables
        if process_error:
            return (process_error, "error")
        if project and project.raw_variables and not project.base_raster:
            return ("Set one variable as the base raster before processing.", "warning")

    # Global status (project load/save) visible on any step
    if status_message:
        return (status_message, "success")

    return None


@solara.component
def NotificationArea(active_tab, aoi_result, project, process_error, status_message, error_message):
    """Sticky floating notification rendered at the bottom of the workflow panel."""
    notif = _compute(active_tab, aoi_result, project, process_error, status_message, error_message)

    if notif is None:
        return

    msg, ntype = notif
    with rv.Html(
        tag="div",
        style_=(
            "position:sticky;bottom:0;left:0;right:0;"
            "padding:8px 12px 12px;"
            "z-index:10;"
            "pointer-events:none;"
        ),
    ):
        rv.Alert(
            type_=ntype,
            dense=True,
            children=[msg],
            style_="pointer-events:auto;margin:0;box-shadow:0 -2px 12px rgba(0,0,0,0.15);",
        )

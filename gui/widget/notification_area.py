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

    elif tab == 2:  # Process
        if process_error:
            return (process_error, "error")
        if project and project.raw_variables and not project.base_raster:
            return ("Set a base raster in the Process step before running processing.", "warning")

    elif tab == 3:  # Dataset
        if project and not project.processed_variables:
            return ("Run Step 3 — Process before creating datasets.", "warning")
        if project and project.datasets:
            count = len(project.datasets)
            return (f"{count} dataset(s) registered.", "success")

    elif tab == 4:  # Sampling
        if project is not None and not project.datasets:
            return ("Register a dataset (Step 4) before sampling.", "warning")
        if project is not None and project.samples:
            count = len(project.samples)
            return (f"{count} sample set(s) generated.", "success")

    elif tab == 5:  # Train
        if project is not None and not project.samples:
            return ("Generate at least one sample set (Step 5) before training.", "warning")

    elif tab == 6:  # Inference
        if project is not None and not project.models:
            return ("Train at least one model (Step 6) before inference.", "warning")

    elif tab == 7:  # Evaluation
        if project is not None and not project.predictions:
            return ("Run inference (Step 7) to produce predictions before evaluation.", "warning")

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
    rv.Alert(type_=ntype, dense=True, children=[msg])

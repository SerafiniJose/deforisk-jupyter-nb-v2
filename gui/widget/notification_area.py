"""Floating step-aware notification bar for the workflow panel."""

import reacton.ipyvuetify as rv
import solara

from gui.i18n import plural, t


def _compute(tab, aoi_result, project, process_error, status_message, error_message):
    """Return (message, type_) for the current tab, or None if nothing to show."""
    # Global error always takes priority
    if error_message:
        return (error_message, "error")

    if tab == 0:  # AOI
        if aoi_result is not None:
            return (t("notifications.aoi_selected", name=aoi_result.name), "success")

    elif tab == 1:  # Variables
        if process_error:
            return (process_error, "error")

    elif tab == 2:  # Process
        if process_error:
            return (process_error, "error")
        if project and project.raw_variables and not project.base_raster:
            return (t("notifications.process_no_base_raster"), "warning")

    elif tab == 3:  # Post-process
        if process_error:
            return (process_error, "error")

    elif tab == 4:  # Dataset
        if project and not project.processed_variables:
            return (t("notifications.dataset_run_process_first"), "warning")
        if project and project.datasets:
            count = len(project.datasets)
            return (plural(count, "notifications.dataset_count_one", "notifications.dataset_count_other"), "success")

    elif tab == 5:  # Sampling
        if project is not None and not any(
            str(getattr(v, "data_type", "")) == "raster"
            for v in project.processed_variables.values()
        ):
            return (t("notifications.sampling_no_raster_vars"), "warning")
        if project is not None and project.samples:
            return (plural(len(project.samples), "notifications.sampling_count_one", "notifications.sampling_count_other"), "success")

    elif tab == 6:  # Train
        if project is not None and not project.datasets:
            return (t("notifications.train_no_dataset"), "warning")

    elif tab == 7:  # Inference
        if project is not None and not project.models:
            return (t("notifications.inference_no_model"), "warning")

    elif tab == 8:  # Evaluation
        if project is not None and not project.predictions:
            return (t("notifications.evaluation_no_predictions"), "warning")

    # Global status (project load/save) visible on any step
    if status_message:
        return (status_message, "success")

    return None


@solara.component
def NotificationArea(active_tab, aoi_result, project, process_error, status_message, error_message):
    """Sticky floating notification rendered at the bottom of the workflow panel.

    Takes the ``app_state`` *reactives* (not their values) and reads ``.value``
    here so the component subscribes and re-renders whenever any of them change —
    including a project-only change on the current tab. Passing ``project.value``
    would trip reacton's prop-equality bailout (the shallow ``model_copy()`` makes
    the new Project compare ``==`` to the previous one) and leave a stale message
    until the next tab switch. ``active_tab`` is local ``use_state``, passed as-is.
    """
    notif = _compute(
        active_tab,
        aoi_result.value,
        project.value,
        process_error.value,
        status_message.value,
        error_message.value,
    )

    if notif is None:
        return

    msg, ntype = notif
    rv.Alert(type_=ntype, dense=True, children=[msg])

"""Floating step-aware notification bar for the workflow panel."""

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t

from gui.store.workflow_steps import STEPS


def _compute(tab, aoi_result, project, process_error, status_message, error_message):
    """Return (message, type_) for the current step, or None.

    Step identity comes from the STEPS registry (never a hardcoded index).
    Lock reasons and per-step output counts are NOT shown here anymore — the
    pipeline header owns them (jump-menu reasons, count badges).
    """
    # Global error always takes priority
    if error_message:
        return (error_message, "error")

    key = STEPS[tab].key if 0 <= tab < len(STEPS) else None

    if key == "aoi" and aoi_result is not None:
        return (t("notifications.aoi_selected", name=aoi_result.name), "success")

    if key in ("variables", "process", "postprocess") and process_error:
        return (process_error, "error")

    if (
        key == "process"
        and project
        and project.raw_variables
        and not project.base_raster
    ):
        return (t("notifications.process_no_base_raster"), "warning")

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

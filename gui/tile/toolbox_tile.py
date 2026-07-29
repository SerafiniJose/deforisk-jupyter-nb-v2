"""Toolbox — tools that sit outside the 9-step workflow.

Today it hosts one tool, deforestation allocation. New tools are added as their
own panel component plus an entry here; the workflow steps are untouched.
"""

import logging
import uuid

import solara
from pysepal.solara.notifications import use_notifications

from gui.i18n import t
from gui.scripts.allocation_runner import (
    allocation_rows,
    delete_allocation_run,
    run_allocation,
)
from gui.scripts.density_map import add_density_on_map, density_layer_key
from gui.scripts.notify_bridge import tracked_job
from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.store.project_writers import writing
from gui.widget.allocation_form import AllocationFormDialog
from gui.widget.allocation_list import AllocationList
from gui.widget.confirm_dialog import ConfirmDialog

logger = logging.getLogger("spatial_risk")

# Module-level so in-flight jobs and on-map layers survive re-renders.
# Both are cleared on project switch by gui/solara_app.py (reset_jobs_on_load /
# render_map_on_switch) — add any new reactive here to those effects too.
allocation_jobs = solara.reactive([])
density_on_map = solara.reactive(set())


def _run_allocation_job(job_id, form, project, project_reactive=None, notifier=None):
    """Worker body: entered on the pool thread, so tracked_job is entered here."""
    try:
        with tracked_job(
            notifier, t("notifications.task_allocation", name=form.name)
        ), writing(project.project_name):
            record = run_allocation(
                project,
                form,
                project_reactive=project_reactive,
                notifier=notifier,
                jobs_reactive=allocation_jobs,
                job_id=job_id,
            )
        update_job(
            allocation_jobs,
            job_id,
            status="completed",
            annual_ha=record.annual_ha,
            total_ha=record.total_ha,
        )
        logger.info(
            "Allocation completed: '%s' — %.1f ha/yr", form.name, record.annual_ha
        )
    except Exception as exc:  # surfaced on the job row
        logger.exception("Allocation failed for '%s'", form.name)
        update_job(allocation_jobs, job_id, status="failed", error=str(exc))


@solara.component
def ToolboxTile(project, map_=None, sepal_client=None):
    """Tool list + the selected tool's panel.

    Args:
        project: solara.Reactive[Project] — the live project.
        map_: SepalMap for the density-raster toggle; None disables it.
        sepal_client: passed to the form's file pickers.
    """
    notifications = use_notifications()
    p = project.value

    form_open = solara.use_reactive(False)
    pending_delete, set_pending_delete = solara.use_state(None)

    def launch(form):
        job_id = str(uuid.uuid4())[:8]
        allocation_jobs.set(
            list(allocation_jobs.value)
            + [{"id": job_id, "name": form.name, "status": "running", "error": None}]
        )
        form_open.set(False)
        spawn_in_context(_run_allocation_job, (job_id, form, p, project, notifications))
        logger.info("Allocation started: '%s' (job=%s)", form.name, job_id)

    def confirm_delete():
        key = pending_delete
        set_pending_delete(None)
        if key and p is not None:
            delete_allocation_run(p, key)
            project.set(p.model_copy())

    def toggle_density(row):
        key = density_layer_key(row["key"])
        if key in density_on_map.value:
            map_.remove_layer(key, none_ok=True)
            density_on_map.set(density_on_map.value - {key})
        else:
            add_density_on_map(
                map_, row["density_map_path"], key=key, layer_name=row["name"]
            )
            density_on_map.set(density_on_map.value | {key})

    with solara.Column(style="gap:12px;"):
        solara.Markdown(f"### {t('toolbox.title')}")
        solara.Text(t("toolbox.subtitle"))

        if p is None:
            solara.Info(t("toolbox.allocation.empty"))
            return

        solara.Markdown(f"**{t('toolbox.allocation.title')}**")
        solara.Text(t("toolbox.allocation.description"))

        AllocationList(
            rows=allocation_rows(p, allocation_jobs.value),
            on_delete=set_pending_delete,
            on_toggle_density=toggle_density if map_ is not None else None,
            density_on_map=density_on_map.value,
        )
        solara.Button(
            t("toolbox.allocation.new"),
            icon_name="mdi-plus",
            block=True,
            on_click=lambda: form_open.set(True),
        )

        ConfirmDialog(
            open=pending_delete is not None,
            title=t("toolbox.allocation.delete_title"),
            message=t("toolbox.allocation.delete_message", name=pending_delete or ""),
            on_confirm=confirm_delete,
            on_cancel=lambda: set_pending_delete(None),
        )
        AllocationFormDialog(
            open_=form_open,
            project=project,
            on_launch=launch,
            on_close=lambda: form_open.set(False),
            sepal_client=sepal_client,
        )

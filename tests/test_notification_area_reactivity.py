"""Regression: the workflow NotificationArea must refresh when the project
changes *without* a tab switch.

Same root cause as the Project Summary bug: NotificationArea was handed
``app_state.project.value`` (a plain Project). When only the project changes
(``project.set(p.model_copy())``, a shallow copy that compares ``==`` to the
previous value), reacton's prop-equality bailout skipped re-rendering it, so a
stale warning lingered until the user switched tabs (which changed ``active_tab``
and broke the bailout). The fix passes the reactives and reads ``.value`` inside
the component so it subscribes.
"""

import reacton
import solara


def _raster(name):
    from spatialrisk.variables.local_raster_var import LocalRasterVar

    return LocalRasterVar.model_construct(
        name=name, year=None, data_type="raster",
        raster_type="continuous", path=None, project=None,
    )


def test_notification_refreshes_on_project_change_same_tab(monkeypatch):
    import gui.widget.notification_area as na
    from spatialrisk.project import Project

    results = []
    orig = na._compute

    def spy(*args):
        r = orig(*args)
        results.append(r)
        return r

    monkeypatch.setattr(na, "_compute", spy)

    # Process tab (2) with a raw variable but no base raster -> warning shown.
    proj = Project(project_name="demo")
    proj.raw_variables["v"] = _raster("v")
    project = solara.reactive(proj, equals=lambda a, b: a is b)
    empty = lambda: solara.reactive(None)

    box, rc = reacton.render(
        na.NotificationArea(
            active_tab=2,
            aoi_result=empty(),
            project=project,
            process_error=empty(),
            status_message=empty(),
            error_message=empty(),
        ),
        handle_error=False,
    )
    assert results[-1] is not None and results[-1][1] == "warning"

    # Set a base raster on the SAME tab: the warning must clear without a tab switch.
    p = project.value
    p.base_raster = _raster("v")
    project.set(p.model_copy())

    assert results[-1] is None, f"notification did not refresh on project change; results={results}"
    rc.close()

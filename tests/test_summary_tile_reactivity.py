"""Regression: the Project Summary tabs must reflect the CURRENT project.

Bug: ``ProjectSummaryTile`` passed ``p = project.value`` (a plain ``Project``) to
each tab renderer. ``project.set(p.model_copy())`` (the app-wide mutate-then-
replace) produces a *shallow* copy that shares the mutated registries, so the new
Project compares ``==`` to the one reacton last rendered. Reacton's prop-equality
bailout then skips re-rendering the tab renderers, freezing them at their first
snapshot — the tile header updated but the tab tables did not.

The fix: renderers receive the ``project`` reactive and read ``.value`` (the
convention every other tile/widget follows), so they re-render on each set.
"""

import reacton
import solara


def _make_pvar(name, year):
    from spatialrisk.variables.local_raster_var import LocalRasterVar

    return LocalRasterVar.model_construct(
        name=name, year=year, data_type="raster",
        raster_type="continuous", path=None, project=None,
    )


def test_processed_tab_reflects_new_variables_after_set(monkeypatch):
    import gui.widget.summary_lists as sl
    from gui.tile.summary_tile import ProjectSummaryTile
    from spatialrisk.project import Project

    seen_totals = []
    orig = sl.processed_variable_rows

    def spy(p):
        stats, rows = orig(p)
        seen_totals.append(stats["total"])
        return stats, rows

    monkeypatch.setattr(sl, "processed_variable_rows", spy)

    project = solara.reactive(Project(project_name="demo"), equals=lambda a, b: a is b)
    project.value.processed_variables["dist_forest"] = _make_pvar("dist_forest", 2020)

    box, rc = reacton.render(
        ProjectSummaryTile(project=project), handle_error=False
    )
    assert seen_totals[-1] == 1  # first render sees the seeded variable

    # Simulate a processing run committing a new processed variable, exactly as
    # process_task does: mutate in place, then replace with a shallow model_copy.
    p = project.value
    p.processed_variables["dist_road"] = _make_pvar("dist_road", 2020)
    project.set(p.model_copy())

    # The processed-variables tab must have recomputed with the new count.
    assert 2 in seen_totals, (
        f"summary tab did not re-render on project change; totals seen={seen_totals}"
    )
    assert seen_totals[-1] == 2

    rc.close()

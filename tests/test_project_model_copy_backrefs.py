"""model_copy() must keep each variable's .project back-reference pointing at
the copy, not the original.

Regression test for the Process tile bug: clicking "Set base" did not enable
"Run processing". The GUI replaces project.value via project.set(p.model_copy())
on every action, but pydantic's shallow model_copy() left every variable's
.project pointing at the discarded original. set_base_raster() therefore set
base_raster on the stale project, so the current project.value kept
base_raster=None and the run button stayed disabled.
"""

from spatialrisk.project import Project
from spatialrisk.variables import LocalRasterVar

Project._ensure_model_schemas()


def _project_with_base():
    p = Project(project_name="t")
    raw = LocalRasterVar.model_construct(name="base", project=p)
    p.raw_variables["base"] = raw
    proc = LocalRasterVar.model_construct(name="proc", project=p)
    p.processed_variables["proc"] = proc
    p.base_raster = LocalRasterVar.model_construct(name="base", project=p)
    return p


def test_model_copy_relinks_raw_variable_backrefs():
    p = _project_with_base()
    p2 = p.model_copy()
    assert p2 is not p
    assert p2.raw_variables["base"].project is p2


def test_model_copy_relinks_processed_variable_backrefs():
    p = _project_with_base()
    p2 = p.model_copy()
    assert p2.processed_variables["proc"].project is p2


def test_model_copy_relinks_base_raster_backref():
    p = _project_with_base()
    p2 = p.model_copy()
    assert p2.base_raster.project is p2


def test_use_as_base_raster_after_copy_sets_base_on_current_project():
    """Mirrors the Process tile flow: copy first (as the Download step does),
    then set base via a variable's .project back-reference."""
    p = _project_with_base()
    p.base_raster = None
    current = p.model_copy()  # what project.value becomes after a prior action

    # set_base_raster() does: current.raw_variables[key].reproject(...).use_as_base_raster()
    # use_as_base_raster() does: self.project.base_raster = self
    reprojected = current.raw_variables["base"]
    reprojected.project.base_raster = reprojected

    assert current.base_raster is not None

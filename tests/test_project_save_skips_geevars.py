"""Regression: ``Project.save()`` must not choke on un-downloaded GEEVars.

A GEEVar created from an ``ee.Image`` holds the live image object in
``gee_images``. Those objects are not JSON-serializable, and ``load()`` only
ever reconstructs ``Local*Var`` — GEEVars are session-only. Downloading a
single variable triggers a full ``save()`` that used to serialize the *other*
still-pending GEEVars and crash with::

    PydanticSerializationError: Unable to serialize unknown type:
    <class 'ee.image.Image'>

save() must skip GEEVars entirely: a GEEVar in raw_variables is never persisted.
"""

from pathlib import Path

import spatialrisk.project as project_module
from spatialrisk import Project
from spatialrisk.variables import LocalRasterVar
from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.models import DataType, RasterType


class _Unserializable:
    """Stand-in for an ``ee.Image``: pydantic cannot dump it in json mode."""


def _project_with_mixed_raw(name: str) -> Project:
    Project._ensure_model_schemas()
    project = Project(project_name=name)

    # A downloaded, local raster var — this one must persist.
    local = LocalRasterVar(
        name="altitude",
        path=Path("/tmp/altitude.tif"),
        raster_type=RasterType.continuous,
    )
    local.project = project
    project.raw_variables["altitude"] = local

    # A still-pending GEEVar holding a non-serializable image object.
    gee = GEEVar(
        name="slope",
        data_type=DataType.raster,
        raster_type=RasterType.continuous,
        gee_images=[_Unserializable()],
    )
    gee.project = project
    project.raw_variables["slope"] = gee

    return project


def test_save_skips_geevars_and_persists_local(tmp_path, monkeypatch):
    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)

    project = _project_with_mixed_raw("gee_skip")

    # Must not raise PydanticSerializationError on the pending GEEVar.
    project.save()

    loaded = Project.load("gee_skip")
    # The local var survives; the session-only GEEVar is absent.
    assert set(loaded.raw_variables) == {"altitude"}

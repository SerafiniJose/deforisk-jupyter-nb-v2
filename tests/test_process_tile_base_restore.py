"""Regression: the Process tile must restore its "Base raster" selection after a
project is loaded.

The base raster lives in the model (``project.base_raster``) and survives a
save/load round-trip, but the Process tile's Select is driven by transient
``use_state`` that defaults empty. After a load the dropdown therefore looked
unset even though a base existed. ``base_raster_key`` maps the stored base back
to its raw-variable key so the tile can repopulate the Select; the tile wires it
through a ``use_effect`` keyed on that key.
"""

import inspect

from spatialrisk.project import Project
from spatialrisk.variables import LocalRasterVar

from gui.tile.process_tile import ProcessTile, base_raster_key

Project._ensure_model_schemas()


def _project_with_base(base_name: str = "altitude") -> Project:
    p = Project(project_name="t")
    p.raw_variables["altitude"] = LocalRasterVar.model_construct(name="altitude", project=p)
    p.raw_variables["slope"] = LocalRasterVar.model_construct(name="slope", project=p)
    p.base_raster = LocalRasterVar.model_construct(name=base_name, project=p)
    return p


def test_base_raster_key_restores_selection():
    assert base_raster_key(_project_with_base("altitude")) == "altitude"


def test_base_raster_key_empty_when_no_base():
    p = Project(project_name="t")
    p.raw_variables["altitude"] = LocalRasterVar.model_construct(name="altitude", project=p)
    assert base_raster_key(p) == ""


def test_base_raster_key_empty_when_project_none():
    assert base_raster_key(None) == ""


def test_base_raster_key_empty_when_base_unmatched():
    # A base whose name matches no raw variable yields no selection (no crash).
    assert base_raster_key(_project_with_base("ghost")) == ""


def test_process_tile_wires_base_restore():
    """Guard the wiring so the restore effect is not silently dropped."""
    src = inspect.getsource(ProcessTile)
    assert "base_raster_key" in src
    assert "use_effect" in src

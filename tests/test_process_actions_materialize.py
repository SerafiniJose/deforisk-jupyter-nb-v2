from unittest.mock import MagicMock

from gui.scripts import process_actions
from spatialrisk.project import Project  # noqa: F401 — needed for GEEVar.model_rebuild()
from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.models import DataType, RasterType

# GEEVar has a forward reference to Project; rebuild so instantiation works.
GEEVar.model_rebuild()


class _Proj:
    def __init__(self):
        self.raw_variables = {}


def test_materialize_replaces_raster_geevar(monkeypatch):
    p = _Proj()
    gv = GEEVar(
        name="altitude",
        data_type=DataType.raster,
        raster_type=RasterType.continuous,
        gee_images=["img"],
    )
    p.raw_variables["altitude"] = gv

    local = MagicMock(name="LocalRasterVar")

    def fake_to_local_raster(*a, **k):
        # mimic add_as_raw registering the local var under the same key
        p.raw_variables["altitude"] = local
        return local

    monkeypatch.setattr(GEEVar, "to_local_raster", fake_to_local_raster, raising=True)

    done = process_actions.materialize_raw_layers(p)

    assert "altitude" in done
    assert p.raw_variables["altitude"] is local


def test_materialize_skips_non_geevar():
    p = _Proj()
    p.raw_variables["x"] = MagicMock(name="LocalRasterVar")  # already local
    assert process_actions.materialize_raw_layers(p) == []

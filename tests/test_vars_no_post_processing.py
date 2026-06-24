"""The modal no longer owns post-processing; the Process tab does."""

import inspect

import gui.widget.variable_modal as vm
from gui.tile.variables_tile import _variable_to_entry
from spatialrisk.variables.local_raster_var import LocalRasterVar
from spatialrisk.variables.models import DataType, RasterType


class _FakeProject:
    base_raster = None


def test_modal_source_has_no_post_processing():
    src = inspect.getsource(vm)
    assert "PostProcessing" not in src, "PostProcessing import/use must be gone"
    assert "post_processing" not in src, "post_processing state/handlers must be gone"
    assert "Post-processing" not in src, "the post-processing Select label must be gone"


def test_local_raster_entry_omits_post_processing():
    var = LocalRasterVar(
        name="elev",
        path="/tmp/elev.tif",
        data_type=DataType.raster,
        raster_type=RasterType.continuous,
    )
    entry = _variable_to_entry("elev", var, _FakeProject())
    assert "post_processing" not in entry

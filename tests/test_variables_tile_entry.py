"""Regression tests for VariablesTile._variable_to_entry round-tripping."""

from gui.tile.variables_tile import _variable_to_entry
from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.models import DataType, RasterType


class _FakeProject:
    base_raster = None


def test_predefined_gee_roundtrips_as_predefined():
    """A catalogue-backed GEEVar (gee_images set, no path) must round-trip as a
    predefined entry so editing rebuilds the image instead of producing
    path='None' and failing GEEVar validation."""
    var = GEEVar(
        name="altitude",  # a PREDEFINED_CATALOGUE key
        data_type=DataType.raster,
        raster_type=RasterType.continuous,
        gee_images=["dummy"],
    )
    entry = _variable_to_entry("altitude", var, _FakeProject())

    assert entry["source"] == "predefined"
    assert entry["predefined_key"] == "altitude"
    # Must never stringify a missing path into the literal "None".
    assert entry.get("asset_id") != "None"


def test_gee_without_path_does_not_emit_literal_none():
    """Defensive: any GEEVar lacking a path must yield an empty asset_id, not 'None'."""
    var = GEEVar(
        name="custom_layer",  # not in the catalogue
        data_type=DataType.raster,
        gee_images=["dummy"],
    )
    entry = _variable_to_entry("custom_layer", var, _FakeProject())

    assert entry.get("asset_id", "") != "None"


def test_entry_key_matches_add_key_convention():
    """entry_key must predict the storage key on_add computes after building the
    variable (name_year, or bare name when year is empty) so the duplicate-add
    confirmation fires exactly when the add would overwrite."""
    from gui.tile.variables_tile import entry_key

    assert entry_key({"name": "forest", "year": "2020"}) == "forest_2020"
    assert entry_key({"name": "altitude", "year": ""}) == "altitude"
    assert entry_key({"name": "altitude"}) == "altitude"

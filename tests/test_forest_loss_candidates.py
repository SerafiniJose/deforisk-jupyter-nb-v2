import pytest

from gui.scripts.process_actions import add_forest_loss_spec, forest_loss_candidates
from spatialrisk.variables.models import DataType


class _Var:
    def __init__(self, name, year, data_type=DataType.raster):
        self.name = name
        self.year = year
        self.data_type = data_type


class _Proj:
    def __init__(self):
        self.raw_variables = {}
        self.forest_loss_specs = []


def _proj_with_forest():
    p = _Proj()
    p.raw_variables = {
        "forest_gfc_2015": _Var("forest_gfc", 2015),
        "forest_gfc_2020": _Var("forest_gfc", 2020),
        "forest_gfc_2024": _Var("forest_gfc", 2024),
        "altitude": _Var("altitude", None),  # static -> excluded
    }
    return p


def test_candidates_groups_temporal_rasters():
    cands = forest_loss_candidates(_proj_with_forest())
    assert cands == {"forest_gfc": [2015, 2020, 2024]}


def test_add_spec_validates_year_order():
    p = _proj_with_forest()
    with pytest.raises(ValueError):
        add_forest_loss_spec(p, "forest_gfc", 2020, 2015)


def test_add_spec_appends_unique():
    p = _proj_with_forest()
    spec = add_forest_loss_spec(p, "forest_gfc", 2015, 2020)
    assert spec.name == "forest_loss_2015_2020"
    assert spec.start_key == "forest_gfc_2015"
    assert spec.end_key == "forest_gfc_2020"
    # second identical add is a no-op
    add_forest_loss_spec(p, "forest_gfc", 2015, 2020)
    assert len(p.forest_loss_specs) == 1


def test_candidates_excludes_vectors():
    p = _Proj()
    p.raw_variables = {
        "forest_gfc_2015": _Var("forest_gfc", 2015),
        "forest_gfc_2020": _Var("forest_gfc", 2020),
        "rivers_2015": _Var("rivers", 2015, data_type=DataType.vector),
        "rivers_2020": _Var("rivers", 2020, data_type=DataType.vector),
    }
    cands = forest_loss_candidates(p)
    assert "rivers" not in cands
    assert cands == {"forest_gfc": [2015, 2020]}

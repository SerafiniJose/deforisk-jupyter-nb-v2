"""Source-guess for derived variables must see through loss_/gain_ prefixes."""

from types import SimpleNamespace

from gui.widget.variable_list import derived_source_key


class _P:
    def __init__(self):
        self.raw_variables = {
            "forest_gfc_2015": SimpleNamespace(name="forest_gfc"),
            "rivers": SimpleNamespace(name="rivers"),
        }


def test_plain_derived_name_matches_raw():
    assert derived_source_key(_P(), "rivers_dist", "?") == "rivers"


def test_loss_prefix_stripped():
    assert derived_source_key(_P(), "loss_forest_gfc_2015_2020", "?") == "forest_gfc_2015"


def test_gain_prefix_stripped():
    assert derived_source_key(_P(), "gain_forest_gfc_2015_2020", "?") == "forest_gfc_2015"


def test_unknown_falls_back():
    assert derived_source_key(_P(), "loss_mangrove_2015_2020", "?") == "?"

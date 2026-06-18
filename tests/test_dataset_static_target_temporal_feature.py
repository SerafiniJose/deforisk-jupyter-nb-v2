"""Regression test: static forest-loss target combined with a temporal feature.

Reproduces the GUI bug where selecting a static target (e.g.
``forest_loss_2015_2020``) together with a temporal feature surfaced a year
dropdown, and the selected year was wrongly forwarded to ``set_target`` —
raising "Target variable '...' is static (not temporal). Do not specify a year
parameter." This locks in the contract the dataset tile relies on.
"""

import types

from spatialrisk.dataset import Dataset


class _FakeProject:
    """Minimal project exposing the methods Dataset needs."""

    def __init__(self):
        # static target: a single instance, no year
        self._instances = {
            "forest_loss_2015_2020": [
                types.SimpleNamespace(name="forest_loss_2015_2020", year=None, path=None)
            ],
            # temporal feature: two yearly instances
            "forest_gfc": [
                types.SimpleNamespace(name="forest_gfc", year=2015, path=None),
                types.SimpleNamespace(name="forest_gfc", year=2020, path=None),
            ],
        }

    def get_all_instances(self, name, source="processed"):
        return self._instances.get(name, [])

    def is_temporal(self, name, source="processed"):
        years = {i.year for i in self.get_all_instances(name) if i.year is not None}
        return len(years) > 1

    def get_variable_years(self, name, source="processed"):
        return sorted({i.year for i in self.get_all_instances(name) if i.year is not None})

    def get_variable(self, name, year=None):
        for inst in self.get_all_instances(name):
            if inst.year == year:
                return inst
        # static variable lookup (year is None)
        return self.get_all_instances(name)[0]

    def list_unique_variable_names(self, source="processed"):
        return list(self._instances)


def test_static_target_rejects_year():
    """The buggy GUI pattern: forwarding the year to a static target raises."""
    p = _FakeProject()
    ds = Dataset(project=p, name="calib")
    try:
        ds.set_target("forest_loss_2015_2020", year=2015)
    except ValueError as exc:
        assert "is static (not temporal)" in str(exc)
    else:
        raise AssertionError("expected ValueError for year on a static target")


def test_static_target_with_temporal_feature_year_alignment():
    """The fixed GUI pattern: year goes to the constructor for feature alignment,
    and only to set_target when the target is temporal (here it is not)."""
    p = _FakeProject()
    year = 2015

    # year stored on the dataset for temporal feature alignment
    ds = Dataset(project=p, name="calib", year=year)
    target_is_temporal = p.is_temporal("forest_loss_2015_2020")
    assert target_is_temporal is False

    # only pass the year to set_target for temporal targets
    ds.set_target(
        "forest_loss_2015_2020", year=year if target_is_temporal else None
    )
    ds.set_features(["forest_gfc"])

    # target carries no year; temporal feature aligned to the selected year
    assert ds.target.year is None
    assert ds.year == year
    assert ds.features[0].year == year

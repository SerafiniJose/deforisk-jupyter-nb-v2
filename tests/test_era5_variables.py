"""ERA5-Land climate variables: recipes and catalogue entries.

The recipes are exercised against a recording stub (no Earth Engine session),
the same pattern as ``test_threshold_reaches_the_gee_expression`` in
test_predefined_params.py.
"""

import types

import gui.scripts.predefined_variables as pv
from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE


class _Rec:
    """Chainable ee stand-in that records every method call."""

    def __init__(self, calls):
        self._calls = calls

    def __getattr__(self, method):
        def _call(*args, **kwargs):
            self._calls.append((method, args))
            return self

        return _call


def _record_recipe(monkeypatch, func, *args, **kwargs):
    """Run a recipe against the recorder; return the [(method, args), ...] log."""
    calls = []

    def _image_collection(collection_id):
        calls.append(("ImageCollection", (collection_id,)))
        return _Rec(calls)

    monkeypatch.setattr(
        pv, "ee", types.SimpleNamespace(ImageCollection=_image_collection)
    )
    func(*args, **kwargs)
    return calls


def test_precipitation_sums_the_year_in_mm(monkeypatch):
    """Annual total = sum of the 12 monthly totals, converted m -> mm."""
    calls = _record_recipe(monkeypatch, pv._get_precipitation, "AOI", 2020)

    assert ("ImageCollection", ("ECMWF/ERA5_LAND/MONTHLY_AGGR",)) in calls
    assert ("filterDate", ("2020-01-01", "2021-01-01")) in calls
    assert ("select", ("total_precipitation_sum",)) in calls
    assert ("sum", ()) in calls
    assert ("multiply", (1000,)) in calls  # metres of water -> mm
    assert ("clip", ("AOI",)) in calls
    assert ("rename", ("B1",)) in calls


def test_precipitation_catalogue_entry():
    """Continuous, temporal 2001-2025, native ~11 km scale, dynamic stretch."""
    cat = PREDEFINED_CATALOGUE["precipitation"]

    assert cat["var_type"] == "GEEVar"
    assert cat["raster_type"] == "continuous"
    assert cat["temporal"] is True
    assert cat["years"] == list(range(2001, 2026))
    assert cat["default_scale"] == 11132
    assert cat["get_image"] is pv._get_precipitation
    assert "params" not in cat
    # Palette-only vis -> the AOI-dependent mm range is stretched dynamically.
    assert cat["vis_params"]["palette"]
    assert "min" not in cat["vis_params"] and "max" not in cat["vis_params"]
    assert cat["label_key"] == "vars.predefined.precipitation"
    assert cat["description_key"] == "vars.predefined_info.precipitation"

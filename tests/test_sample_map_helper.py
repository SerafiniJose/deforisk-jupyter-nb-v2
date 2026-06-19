"""Pure-logic test for the sample-points map helper."""

import geopandas as gpd
from shapely.geometry import Point

from gui.scripts.map_helpers import _split_by_target, sample_layer_keys


def test_split_by_target_separates_event_and_forest():
    gdf = gpd.GeoDataFrame(
        {"target": [1, 0, 1, 0, 0]},
        geometry=[Point(i, i) for i in range(5)],
        crs="EPSG:4326",
    )
    event, forest = _split_by_target(gdf)
    assert len(event) == 2
    assert len(forest) == 3
    assert set(event["target"]) == {1}
    assert set(forest["target"]) == {0}


def test_sample_layer_keys_are_distinct():
    e, f = sample_layer_keys("sample_s1")
    assert e == "sample_s1__event"
    assert f == "sample_s1__forest"
    assert e != f

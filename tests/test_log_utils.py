import logging

from spatialrisk.log_utils import log_progress


def test_yields_all_items_in_order():
    assert list(log_progress(["a", "b", "c"], "Doing")) == ["a", "b", "c"]


def test_logs_count_lines_with_label(caplog):
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        list(log_progress(["x", "y"], "Downloading layer", label=lambda s: s.upper()))
    messages = [r.getMessage() for r in caplog.records]
    assert messages == ["Downloading layer 1/2: X", "Downloading layer 2/2: Y"]


def test_no_suffix_without_label(caplog):
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        list(log_progress(["only"], "Reprojecting"))
    assert [r.getMessage() for r in caplog.records] == ["Reprojecting 1/1"]


def test_empty_iterable_logs_nothing(caplog):
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        assert list(log_progress([], "Rasterizing")) == []
    assert caplog.records == []


def test_label_over_tuple_pairs(caplog):
    pairs = [("forest_gfc", object()), ("dem", object())]
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        out = list(log_progress(pairs, "Reprojecting", label=lambda kv: kv[0]))
    assert out == pairs
    assert [r.getMessage() for r in caplog.records] == [
        "Reprojecting 1/2: forest_gfc",
        "Reprojecting 2/2: dem",
    ]

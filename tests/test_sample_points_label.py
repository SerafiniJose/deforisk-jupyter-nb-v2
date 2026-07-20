"""format_sample_points(): class counts only for stratified, capped at MAX_DISPLAYED_STRATA."""
import pytest

from gui.scripts.product_rows import MAX_DISPLAYED_STRATA, format_sample_points


def test_stratified_lists_class_counts():
    out = format_sample_points(1000, {"0": 600, "1": 400}, "stratified")
    assert out == "1000 (0:600, 1:400)"


@pytest.mark.parametrize("strategy", ["random", "systematic", "—", None, ""])
def test_non_stratified_shows_total_only(strategy):
    out = format_sample_points(1000, {"0": 600, "1": 400}, strategy)
    assert out == "1000"


def test_stratified_without_counts_shows_total_only():
    assert format_sample_points(50, {}, "stratified") == "50"
    assert format_sample_points(50, None, "stratified") == "50"


def test_class_keys_sort_numerically_not_lexically():
    counts = {"10": 1, "2": 2, "1": 3}
    assert format_sample_points(6, counts, "stratified") == "6 (1:3, 2:2, 10:1)"


def test_many_strata_are_truncated_with_a_more_suffix():
    counts = {str(i): 1 for i in range(25)}
    out = format_sample_points(25, counts, "stratified")
    shown = out[out.index("(") + 1: out.rindex(")")].split(", ")
    assert len(shown) == MAX_DISPLAYED_STRATA + 1
    assert shown[0] == "0:1"
    assert shown[MAX_DISPLAYED_STRATA - 1] == f"{MAX_DISPLAYED_STRATA - 1}:1"
    assert shown[-1] == f"+{25 - MAX_DISPLAYED_STRATA} more"


def test_exactly_max_strata_has_no_more_suffix():
    counts = {str(i): 1 for i in range(MAX_DISPLAYED_STRATA)}
    out = format_sample_points(MAX_DISPLAYED_STRATA, counts, "stratified")
    assert "more" not in out


def test_more_suffix_template_is_caller_supplied():
    counts = {str(i): 1 for i in range(12)}
    out = format_sample_points(12, counts, "stratified", more_fmt="+{n} más")
    assert out.endswith("+2 más)")


def test_non_numeric_class_keys_fall_back_to_string_order():
    counts = {"b": 1, "a": 2}
    assert format_sample_points(3, counts, "stratified") == "3 (a:2, b:1)"


def test_missing_total_renders_a_dash():
    assert format_sample_points(None, {}, "random") == "—"

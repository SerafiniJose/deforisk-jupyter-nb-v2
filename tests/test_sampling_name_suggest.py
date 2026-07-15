"""Unit tests for the sample-name auto-suggestion helper."""
from gui.tile.sampling_tile import _suggest_name


def test_first_suggestion_when_none_taken():
    assert _suggest_name("random", set()) == "random_1"


def test_skips_taken_names_in_order():
    assert _suggest_name("random", {"random_1"}) == "random_2"
    assert _suggest_name("random", {"random_1", "random_2"}) == "random_3"


def test_fills_lowest_available_gap():
    # random_2 is free even though random_1 and random_3 are taken
    assert _suggest_name("random", {"random_1", "random_3"}) == "random_2"


def test_strategy_name_is_used_as_prefix():
    assert _suggest_name("stratified", {"random_1"}) == "stratified_1"
    assert _suggest_name("systematic", set()) == "systematic_1"


def test_unrelated_names_do_not_block():
    assert _suggest_name("random", {"my_custom_sample", "stratified_1"}) == "random_1"

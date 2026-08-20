"""format_value(): display form of a stored parameter value in a details dialog."""

from gui.widget.details_fields import format_value


def test_none_renders_an_em_dash():
    """A missing value shows as an em dash, not the literal string "None"."""
    assert format_value(None) == "—"


def test_scalars_stringify():
    """Strings, ints, and floats pass through str() unchanged."""
    assert format_value("random") == "random"
    assert format_value(500) == "500"
    assert format_value(0.75) == "0.75"


def test_lists_join_like_the_form():
    """Lists and tuples join with ", ", matching the form's multi-value fields."""
    assert format_value(["a", "b"]) == "a, b"
    assert format_value((1, 2)) == "1, 2"


def test_booleans_are_translated_not_python_repr():
    """Sample.adapt is a bool; "True" is untranslated and looks like a leak."""
    yes, no = format_value(True), format_value(False)
    assert yes not in ("True", "1")
    assert no not in ("False", "0")
    assert yes != no


def test_booleans_use_the_shared_yes_no_keys():
    """The translation goes through the same common.yes/common.no keys as callers."""
    from gui.i18n import t

    assert format_value(True) == t("common.yes")
    assert format_value(False) == t("common.no")


def test_zero_and_empty_string_are_not_treated_as_missing():
    """Only None is missing — a seed of 0 is a real value."""
    assert format_value(0) == "0"
    assert format_value("") == ""



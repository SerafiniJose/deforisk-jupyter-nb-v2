"""Parameterised predefined layers: catalogue params, name↔params mapping.

The variable name carries its parameters (``forest_gfc_tc30``) so two forest
definitions can coexist as separate variables. ``build_predefined_name`` and
``resolve_predefined`` must stay exact inverses — everything downstream (map
styling, edit round-trip) depends on parsing the name back.
"""

import pytest

from gui.scripts.predefined_variables import (
    PREDEFINED_CATALOGUE,
    build_predefined_name,
    coerce_param_values,
    default_param_values,
    param_specs,
    resolve_predefined,
)


def test_forest_gfc_declares_a_tree_cover_param():
    """The catalogue is the single source of truth for the knob and its default."""
    specs = param_specs("forest_gfc")

    assert [s["key"] for s in specs] == ["tree_cover_threshold"]
    spec = specs[0]
    assert spec["default"] == 30
    assert (spec["min"], spec["max"]) == (1, 100)
    assert spec["suffix_prefix"] == "tc"
    assert spec["type"] == "int"


def test_get_image_default_matches_catalogue_default():
    """A caller omitting kwarg gets same forest definition as modal.

    Otherwise the two silently disagree.
    """
    import inspect

    sig = inspect.signature(PREDEFINED_CATALOGUE["forest_gfc"]["get_image"])
    assert sig.parameters["tree_cover_threshold"].default == 30


def test_layers_without_params_report_empty():
    """The other eight entries are unparameterised and must stay that way."""
    assert param_specs("altitude") == []
    assert default_param_values("altitude") == {}
    assert param_specs("not_a_layer") == []


def test_default_param_values():
    """Retrieve default param values for a catalogue entry."""
    assert default_param_values("forest_gfc") == {"tree_cover_threshold": 30}


def test_build_name_appends_suffix():
    """Build variable name by appending param suffix to catalogue key."""
    assert (
        build_predefined_name("forest_gfc", {"tree_cover_threshold": 30})
        == "forest_gfc_tc30"
    )
    # an unparameterised layer keeps its bare key
    assert build_predefined_name("altitude", {}) == "altitude"


def test_resolve_plain_catalogue_key():
    """Bare catalogue keys resolve for backward compatibility.

    Variables created before this feature are named by the bare key and must
    keep resolving.
    """
    assert resolve_predefined("altitude") == ("altitude", {})
    assert resolve_predefined("forest_gfc") == ("forest_gfc", {})


def test_resolve_parses_params_back():
    """Parse param values back from variable name."""
    assert resolve_predefined("forest_gfc_tc30") == (
        "forest_gfc",
        {"tree_cover_threshold": 30},
    )
    assert resolve_predefined("forest_gfc_tc7") == (
        "forest_gfc",
        {"tree_cover_threshold": 7},
    )


@pytest.mark.parametrize(
    "name",
    [
        "",
        "my_custom_mask",
        "forest_gfc_tcXX",  # malformed suffix
        "forest_gfc_tc30_extra",  # too many segments
        "forest_gfc_tc30_2020",  # storage key, not a variable name
        "loss_forest_gfc_tc30_2015_2020",  # post-process output
    ],
)
def test_resolve_rejects_non_catalogue_names(name):
    """Non-catalogue names fall through for custom styling.

    Anything that does not parse cleanly must fall through so custom and
    post-process variables keep their own styling.
    """
    assert resolve_predefined(name) == (None, {})


def test_roundtrip_is_exact():
    """Build and resolve are exact inverses."""
    for value in (1, 10, 30, 100):
        name = build_predefined_name("forest_gfc", {"tree_cover_threshold": value})
        assert resolve_predefined(name) == (
            "forest_gfc",
            {"tree_cover_threshold": value},
        )


def test_suffix_values_can_never_reach_four_digits():
    """Guard: a four-digit suffix would be parsed as a year downstream.

    ``spatialrisk.evaluation.interval_from_target`` pulls years out of a name
    with a four-consecutive-digits regex, so a param whose value could reach
    four digits would corrupt derived change-layer intervals. Every declared
    param's ``max`` must therefore stay below 1000.
    """
    for key in PREDEFINED_CATALOGUE:
        for spec in param_specs(key):
            assert spec["max"] < 1000, f"{key}.{spec['key']} allows a 4-digit suffix"


def test_resolve_is_not_injective_over_non_canonical_suffixes():
    """Documented limit of the inverse property (see resolve_predefined).

    The inverse holds for every name ``build_predefined_name`` emits. It is not
    injective over arbitrary strings: a zero-padded suffix parses to the same
    values. Unreachable from the UI, where values are coerced to ``int`` before
    the name is built — pinned here so the docstring stays honest.
    """
    canonical = build_predefined_name("forest_gfc", {"tree_cover_threshold": 30})
    assert canonical == "forest_gfc_tc30"
    assert resolve_predefined("forest_gfc_tc030") == resolve_predefined(canonical)


def test_coerce_accepts_valid_form_text():
    """Form fields hand back strings; the entry must carry real ints."""
    values, bad = coerce_param_values("forest_gfc", {"tree_cover_threshold": "30"})

    assert bad is None
    assert values == {"tree_cover_threshold": 30}


@pytest.mark.parametrize("text", ["", "  ", "abc", "0", "101", "12.5", None])
def test_coerce_rejects_invalid_values(text):
    """Reject invalid param values outside range or non-numeric."""
    values, bad = coerce_param_values("forest_gfc", {"tree_cover_threshold": text})

    assert values == {}
    assert bad is not None and bad["key"] == "tree_cover_threshold"


def test_coerce_reports_missing_value():
    """A param the form never filled in is an error, not a silent default."""
    values, bad = coerce_param_values("forest_gfc", {})

    assert values == {}
    assert bad["key"] == "tree_cover_threshold"


def test_coerce_on_unparameterised_layer_is_empty_and_valid():
    """Unparameterised layers require no params, coerce succeeds."""
    assert coerce_param_values("altitude", {}) == ({}, None)


def test_threshold_reaches_the_gee_expression(monkeypatch):
    """User-picked threshold reaches the GEE expression.

    ``_get_forest_gfc`` is exercised against a recording stub so no Earth
    Engine session is needed.
    """
    import types

    import gui.scripts.predefined_variables as pv

    calls = []

    class _Rec:
        """Chainable ee.Image stand-in that records every method call."""

        def __getattr__(self, method):
            def _call(*args, **kwargs):
                calls.append((method, args))
                return self

            return _call

    monkeypatch.setattr(pv, "ee", types.SimpleNamespace(Image=lambda *a, **k: _Rec()))

    pv._get_forest_gfc("AOI", 2020, tree_cover_threshold=45)

    assert ("gte", (45,)) in calls

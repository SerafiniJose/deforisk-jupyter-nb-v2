"""validate_formula: rules from the 2026-07-28 formula-field spec.

Returns None when valid, else (i18n_key, kwargs) — Solara-free so it is
unit-testable like model_registry.
"""
from gui.scripts.formula_validation import validate_formula

TARGET = "fcc"
FEATURES = ["altitude", "dist_edge", "pa"]


def _err_key(formula):
    """Extract error key from validation result."""
    res = validate_formula(formula, TARGET, FEATURES)
    return res[0] if res else None


def test_accepts_generator_output_with_levels():
    """Valid formula with generator style and C() levels."""
    assert (
        validate_formula(
            "I(fcc) + trial ~ scale(altitude) + scale(dist_edge) + "
            "C(pa, levels=[0, 1])",
            TARGET,
            FEATURES,
        )
        is None
    )


def test_accepts_intercept_only_rhs():
    """Valid formula with intercept-only RHS."""
    assert validate_formula("I(fcc) + trial ~ 1", TARGET, FEATURES) is None


def test_rejects_empty_and_missing_tilde():
    """Reject empty, missing, or malformed tilde."""
    assert _err_key("") == "tiles.train.error_formula_shape"
    assert _err_key("   ") == "tiles.train.error_formula_shape"
    assert _err_key("I(fcc) + trial") == "tiles.train.error_formula_shape"
    assert _err_key("a ~ b ~ c") == "tiles.train.error_formula_shape"
    assert _err_key("~ scale(altitude)") == "tiles.train.error_formula_shape"
    assert _err_key("I(fcc) + trial ~ ") == "tiles.train.error_formula_shape"


def test_rejects_parse_error():
    """Reject unparsable formula."""
    assert _err_key("I(fcc) + trial ~ scale(") == "tiles.train.error_formula_parse"


def test_rejects_missing_target_on_lhs():
    """Reject when target name is missing from LHS."""
    assert (
        _err_key("trial ~ scale(altitude)")
        == "tiles.train.error_formula_missing_target"
    )


def test_rejects_unknown_lhs_variable():
    """Reject unknown variables on LHS besides target and trial."""
    res = validate_formula("I(fcc) + trial + bogus ~ scale(altitude)", TARGET, FEATURES)
    assert res[0] == "tiles.train.error_formula_lhs"
    assert "bogus" in res[1]["names"]


def test_rejects_reserved_names_on_rhs():
    """Reject reserved names (cell, trial, target) on RHS."""
    # cell is appended internally by iCAR only; target/trial on the RHS
    # train but break prediction — all rejected up front.
    for bad in ("cell", "trial", "fcc"):
        res = validate_formula(
            f"I(fcc) + trial ~ scale(altitude) + {bad}", TARGET, FEATURES
        )
        assert res[0] == "tiles.train.error_formula_rhs_reserved"
        assert bad in res[1]["names"]


def test_rejects_unknown_rhs_variable():
    """Reject unknown variables on RHS."""
    res = validate_formula("I(fcc) + trial ~ scale(slope)", TARGET, FEATURES)
    assert res[0] == "tiles.train.error_formula_rhs_unknown"
    assert "slope" in res[1]["names"]

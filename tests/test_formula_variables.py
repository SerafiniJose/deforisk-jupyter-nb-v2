"""formula_variables: ast-based variable extraction for formula validation.

extract_variables() (regex) flags `levels` inside C(x, levels=[...]) as a
variable — the exact string generate_patsy_formula emits — so validation
needs this patsy+ast implementation instead.
"""

import pytest

from spatialrisk.far_helpers import formula_variables


def test_generator_style_formula_with_levels_kwarg():
    """Extract variables, excluding levels kwarg and transforms."""
    lhs, rhs = formula_variables(
        "I(fcc) + trial ~ scale(altitude) + scale(dist_edge) + C(pa, levels=[0, 1])"
    )
    assert lhs == {"fcc", "trial"}
    assert rhs == {"altitude", "dist_edge", "pa"}  # no 'levels', no 'scale', no 'C'


def test_intercept_only_rhs_is_empty_set():
    """RHS with only intercept term (1) yields empty set."""
    lhs, rhs = formula_variables("I(fcc) + trial ~ 1")
    assert lhs == {"fcc", "trial"}
    assert rhs == set()


def test_bare_variables_and_transforms():
    """Bare variables and transformed variables both extracted."""
    lhs, rhs = formula_variables("y ~ x1 + scale(x2) + C(x3)")
    assert lhs == {"y"}
    assert rhs == {"x1", "x2", "x3"}


def test_unparseable_formula_raises():
    """Malformed formula raises exception."""
    with pytest.raises(Exception):
        formula_variables("I(fcc) + trial ~ scale(")

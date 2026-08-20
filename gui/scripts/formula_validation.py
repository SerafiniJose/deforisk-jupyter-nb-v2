"""Patsy-formula validation for the New-model dialog.

Data/logic only (returns i18n *keys*); label resolution happens in the
widgets. Solara-free so dialogs and tests can both import it.
"""

from typing import Optional

from spatialrisk.far_helpers import formula_variables

# `trial` belongs on the LHS only; `cell` is appended internally by the iCAR
# model after sample preparation — a user-typed `cell` fails at fit time for
# every model, so both are rejected on the RHS up front.
_RESERVED_RHS = {"trial", "cell"}


def validate_formula(
    formula: Optional[str], target_name: str, feature_names: list
) -> Optional[tuple]:
    """None when valid, else (i18n_key, format_kwargs) for the dialog alert."""
    text = (formula or "").strip()
    if not text or text.count("~") != 1:
        return ("tiles.train.error_formula_shape", {})
    lhs_txt, rhs_txt = (s.strip() for s in text.split("~", 1))
    if not lhs_txt or not rhs_txt:
        return ("tiles.train.error_formula_shape", {})

    try:
        lhs_vars, rhs_vars = formula_variables(text)
    except Exception as exc:
        return ("tiles.train.error_formula_parse", {"error": str(exc)})

    if target_name not in lhs_vars:
        return ("tiles.train.error_formula_missing_target", {"target": target_name})
    extra_lhs = lhs_vars - {target_name, "trial"}
    if extra_lhs:
        return (
            "tiles.train.error_formula_lhs",
            {"names": ", ".join(sorted(extra_lhs))},
        )

    reserved = rhs_vars & (_RESERVED_RHS | {target_name})
    if reserved:
        return (
            "tiles.train.error_formula_rhs_reserved",
            {"names": ", ".join(sorted(reserved))},
        )
    unknown = rhs_vars - set(feature_names)
    if unknown:
        return (
            "tiles.train.error_formula_rhs_unknown",
            {"names": ", ".join(sorted(unknown))},
        )
    return None

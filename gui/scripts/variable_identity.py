"""Which raw variable is the base raster?

Kept free of Solara/ipyvuetify so the matching logic can be unit-tested without
a render harness.

Raw variables are registered under ``{name}_{year}`` (bare ``name`` when the
layer has no year), so one temporal layer contributes several entries that
share a ``name`` — forest 2000, forest 2010, … The base raster is a *reprojected
copy* of the chosen entry (``process_actions.set_base_raster``), so it cannot be
compared by object identity, but ``reproject`` preserves both ``name`` and
``year``. Those two fields together are therefore the variable's identity, and
matching on ``name`` alone tags every year of the layer as the base.
"""

from typing import Any, Optional, Tuple


def variable_identity(var: Any) -> Tuple[Optional[str], Optional[int]]:
    """``(name, year)`` — what makes a raw variable distinct from its siblings.

    Duck-typed on purpose: it is handed model instances, reprojected copies and
    the plain namespaces the summary helpers are tested with.
    """
    return (getattr(var, "name", None), getattr(var, "year", None))


def is_base_raster(project: Any, var: Any) -> bool:
    """True when ``var`` is the raw variable backing the project's base raster."""
    base = getattr(project, "base_raster", None)
    if base is None or var is None:
        return False
    return variable_identity(base) == variable_identity(var)


def base_raster_key(project: Any) -> str:
    """Raw-variable key backing the current base raster ('' if none/unmatched).

    The Process-tile Select is keyed by raw-variable key, so mapping the stored
    base back to a key is what lets the Select be restored after a project is
    loaded (the base lives in the model, the Select's state is transient
    ``use_state``). Name-only matching restored whichever year of a temporal
    layer happened to come first.
    """
    if project is None or getattr(project, "base_raster", None) is None:
        return ""
    return next(
        (
            key
            for key, var in getattr(project, "raw_variables", {}).items()
            if is_base_raster(project, var)
        ),
        "",
    )

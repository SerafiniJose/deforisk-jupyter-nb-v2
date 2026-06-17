"""Shared GEE recipe catalogue.

A registry mapping ``catalogue_key -> resolver(aoi_ee, **params) -> ee.Image |
ee.FeatureCollection``. Promoted out of ``gui/scripts/predefined_variables.py``
and ``notebooks/1.variables_factory.ipynb`` so both the GUI and notebooks resolve
against one source of truth. ``GEEAdapter`` is the only runtime caller.
"""

from typing import Any, Callable, Dict

import ee  # noqa: F401  (module-level so tests can patch spatialrisk.gee.catalogue.ee)

Resolver = Callable[..., Any]

CATALOGUE: Dict[str, Resolver] = {}


def register(key: str) -> Callable[[Resolver], Resolver]:
    """Decorator: register ``fn`` under ``key`` in the shared catalogue."""

    def _decorator(fn: Resolver) -> Resolver:
        if key in CATALOGUE:
            raise ValueError(f"catalogue key already registered: {key!r}")
        CATALOGUE[key] = fn
        return fn

    return _decorator


def get_resolver(key: str) -> Resolver:
    """Return the resolver registered under ``key``."""
    if key not in CATALOGUE:
        raise KeyError(f"unknown catalogue key: {key!r}")
    return CATALOGUE[key]

"""Backward-compatibility namespace for the legacy ``component.script`` package.

The project package was renamed ``component/script`` -> ``spatialrisk`` (a clean
``git mv``; see ``component/script/__init__.py`` for the redirect shim). This
top-level package exists only so that ``import component.script`` resolves; all
real code lives in ``spatialrisk``.
"""

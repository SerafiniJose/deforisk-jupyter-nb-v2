"""Stateless prediction collaborators (offload seams).

These are pure / self-contained functions and classes extracted from the
former ``BaseRiskModel.apply()`` family. They never import ``spatialrisk``
at package-import time, so they stay importable while the top-level package
__init__ is mid-migration.
"""

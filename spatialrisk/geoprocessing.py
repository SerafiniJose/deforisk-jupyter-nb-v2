"""Stateless variable-geoprocessing functions.

Pure offload seams extracted from ``variables/local_raster_var.py`` and
``variables/local_vector_var.py``. Each function takes an EXPLICIT ``out_path``
plus an input spec plus a base/geobox, RETURNS a new ``LocalRasterSpec``, and
NEVER reaches into a live ``Project`` (no live-Project attribute access, no
implicit save).
Numeric/geospatial behavior is the verbatim current path: these wrap the
existing primitives in ``geo_utils`` / ``processing`` only.
"""

from __future__ import annotations

from spatialrisk.document import LocalRasterSpec
from spatialrisk.variables.models import PostProcessing, RasterizationMethod, RasterType


def reproject_and_match(*args, **kwargs):  # implemented in F2
    raise NotImplementedError


def rasterize_vector(*args, **kwargs):  # implemented in F3
    raise NotImplementedError


def apply_post_processing(*args, **kwargs):  # implemented in F4
    raise NotImplementedError

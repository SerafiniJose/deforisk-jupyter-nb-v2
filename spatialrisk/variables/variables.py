"""Backward-compatible imports for legacy `component.script.variables.variables`.

This module intentionally re-exports the canonical classes and enums defined in
the modular variables package. The source of truth lives in:

- models.py
- variable.py
- local_raster_var.py
- local_vector_var.py
- gee_var.py
"""

from spatialrisk.variables.gee_var import GEEVar
from spatialrisk.variables.local_raster_var import LocalRasterVar
from spatialrisk.variables.local_vector_var import LocalVectorVar
from spatialrisk.variables.models import (
    DataType,
    PostProcessing,
    RasterizationMethod,
    RasterType,
)
from spatialrisk.variables.variable import Variable

__all__ = [
    "DataType",
    "GEEVar",
    "LocalRasterVar",
    "LocalVectorVar",
    "PostProcessing",
    "RasterType",
    "RasterizationMethod",
    "Variable",
]

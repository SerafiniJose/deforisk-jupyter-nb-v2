"""Backward-compatible imports for legacy `component.script.variables.variables`.

This module intentionally re-exports the canonical classes and enums defined in
the modular variables package. The source of truth lives in:

- models.py
- variable.py
- local_raster_var.py
- local_vector_var.py
- gee_var.py
"""

from component.script.variables.gee_var import GEEVar
from component.script.variables.local_raster_var import LocalRasterVar
from component.script.variables.local_vector_var import LocalVectorVar
from component.script.variables.models import (
    DataType,
    PostProcessing,
    RasterType,
    RasterizationMethod,
)
from component.script.variables.variable import Variable

__all__ = [
    "Variable",
    "DataType",
    "RasterizationMethod",
    "RasterType",
    "PostProcessing",
    "LocalVectorVar",
    "LocalRasterVar",
    "GEEVar",
]

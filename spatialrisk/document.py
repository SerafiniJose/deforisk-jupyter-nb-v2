"""Document layer: frozen, references-only Project specs (Pydantic v2).

Leaf module — imports only the enums from variables/models.py. No Session
import, no import-time model_rebuild, no forward references. Paths are str.
"""

from typing import Literal, Union

import pydantic
from pydantic import BaseModel, ConfigDict, Field

from spatialrisk.variables.models import (  # enums only
    DataType,
    PostProcessing,
    RasterizationMethod,
    RasterType,
)

JsonValue = pydantic.JsonValue
GeoJSONGeometry = dict[str, JsonValue]


class VariableId(BaseModel):
    """Canonical, unambiguous variable reference (source, name, year)."""

    model_config = ConfigDict(frozen=True)

    source: Literal["raw", "processed"]
    name: str
    year: int | None = None


VarRef = VariableId

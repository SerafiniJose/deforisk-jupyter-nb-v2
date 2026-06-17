from enum import Enum


class DataType(str, Enum):
    vector = "vector"
    raster = "raster"


class RasterizationMethod(str, Enum):
    binary = "binary"
    unique = "unique"


class RasterType(str, Enum):
    continuous = "continuous"
    categorical = "categorical"


class PostProcessing(str, Enum):
    edge = "edge"
    dist = "dist"


from typing import List

from pydantic import BaseModel, Field


class ForestLossSpec(BaseModel):
    """Deferred forest-loss target recipe.

    Declared in the Variables tile and generated during the Process run from
    two forest layers (start year -> end year). Persisted with the project.
    """

    name: str  # forest_loss_{start_year}_{end_year}
    start_key: str  # raw_variables key of the start-year forest layer
    end_key: str  # raw_variables key of the end-year forest layer
    start_year: int
    end_year: int
    tags: List[str] = Field(default_factory=lambda: ["deforestation", "forest_loss"])

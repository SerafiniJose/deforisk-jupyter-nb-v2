"""Document layer: frozen, references-only Project specs (Pydantic v2).

Leaf module — imports only the enums from variables/models.py. No Session
import, no import-time model_rebuild, no forward references. Paths are str.
"""

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeVar, Union, get_args

import pydantic
from pydantic import BaseModel, ConfigDict, Field, GetCoreSchemaHandler, computed_field
from pydantic_core import core_schema

from spatialrisk.sampling import Sampling
from spatialrisk.variables.models import (  # enums only
    DataType,
    PostProcessing,
    RasterizationMethod,
    RasterType,
)

JsonValue = pydantic.JsonValue
GeoJSONGeometry = dict[str, JsonValue]

V = TypeVar("V")


class FrozenDict(Mapping[str, V]):
    """Immutable, hashable str-keyed mapping for the Document registries.

    Copies its input on construction; __setitem__/__delitem__ raise TypeError.
    Carries a Pydantic core schema that validates values against V.
    """

    __slots__ = ("_data", "_hash")

    def __init__(self, data: Mapping[str, V] | None = None):
        object.__setattr__(self, "_data", dict(data) if data is not None else {})
        object.__setattr__(self, "_hash", None)

    def __getitem__(self, key: str) -> V:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __setitem__(self, key, value):
        raise TypeError("FrozenDict is immutable")

    def __delitem__(self, key):
        raise TypeError("FrozenDict is immutable")

    def __setattr__(self, name, value):
        raise TypeError("FrozenDict is immutable")

    def __eq__(self, other) -> bool:
        if isinstance(other, FrozenDict):
            return self._data == other._data
        if isinstance(other, Mapping):
            return self._data == dict(other)
        return NotImplemented

    def __hash__(self) -> int:
        if self._hash is None:
            object.__setattr__(self, "_hash", hash(frozenset(self._data.items())))
        return self._hash

    def __repr__(self) -> str:
        return f"FrozenDict({self._data!r})"

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        args = get_args(source)
        value_schema = handler.generate_schema(args[1]) if len(args) == 2 else core_schema.any_schema()
        dict_schema = core_schema.dict_schema(
            keys_schema=core_schema.str_schema(),
            values_schema=value_schema,
        )

        def _validate(value: Any) -> "FrozenDict":
            if isinstance(value, FrozenDict):
                value = dict(value)
            return cls(value)

        return core_schema.no_info_after_validator_function(
            _validate,
            dict_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: dict(v),
                info_arg=False,
                return_schema=dict_schema,
            ),
        )


class VariableId(BaseModel):
    """Canonical, unambiguous variable reference (source, name, year)."""

    model_config = ConfigDict(frozen=True)

    source: Literal["raw", "processed"]
    name: str
    year: int | None = None


VarRef = VariableId


class CatalogueRecipe(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: Literal["catalogue"] = "catalogue"
    catalogue_key: str
    params: dict[str, JsonValue] = Field(default_factory=dict)
    aoi: GeoJSONGeometry | None = None
    scale: float | None = None
    crs: str | None = None
    export_kind: Literal["raster", "vector"]
    vector_selectors: tuple[str, ...] | None = None
    unmask_value: int | None = 255
    nodata_value: int | None = 255


class AssetRecipe(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: Literal["asset"] = "asset"
    asset_id: str
    band: str | None = None
    aoi: GeoJSONGeometry | None = None
    scale: float | None = None
    crs: str | None = None
    export_kind: Literal["raster", "vector"]
    vector_selectors: tuple[str, ...] | None = None
    unmask_value: int | None = 255
    nodata_value: int | None = 255


GEERecipe = Annotated[
    Union[CatalogueRecipe, AssetRecipe], Field(discriminator="source")
]


class LocalRasterSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["local_raster"] = "local_raster"
    name: str
    year: int | None = None
    active: bool = True
    tags: tuple[str, ...] = ()
    path: str
    raster_type: RasterType
    post_processing: tuple[PostProcessing, ...] = ()
    processing_history: tuple[str, ...] = ()
    default_crs: str | None = None
    default_resolution: float | None = None
    derived_from: str | None = None

    @computed_field
    @property
    def data_type(self) -> DataType:
        return DataType.raster


class LocalVectorSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["local_vector"] = "local_vector"
    name: str
    year: int | None = None
    active: bool = True
    tags: tuple[str, ...] = ()
    path: str
    rasterization_method: RasterizationMethod
    default_crs: str | None = None
    derived_from: str | None = None

    @computed_field
    @property
    def data_type(self) -> DataType:
        return DataType.vector


class GEESpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["gee"] = "gee"
    name: str
    year: int | None = None
    active: bool = True
    tags: tuple[str, ...] = ()
    data_type: DataType
    raster_type: RasterType | None = None
    rasterization_method: RasterizationMethod | None = None
    post_processing: tuple[PostProcessing, ...] = ()
    recipe: GEERecipe
    materialized_key: str | None = None


VariableSpec = Annotated[
    Union[LocalRasterSpec, LocalVectorSpec, GEESpec], Field(discriminator="kind")
]


class DatasetSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    year: int | None = None
    target_ref: VarRef | None = None
    feature_refs: tuple[VarRef, ...] = ()
    sampling: Sampling | None = None


class _ModelSpecBase(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str | None = None
    project_name: str | None = None
    dataset_name: str | None = None
    target_name: str | None = None
    feature_names: tuple[str, ...] = ()
    year: int | None = None
    formula: str | None = None
    parameters: dict[str, JsonValue] = Field(default_factory=dict)
    sampling: Sampling | None = None
    samples_path: str | None = None
    trained: bool = False
    trained_at: str | None = None
    n_samples: int | None = None
    deviance: float | None = None


class GLMSpec(_ModelSpecBase):
    model_type: Literal["glm"]
    estimator_pickle: str | None = None


class RFSpec(_ModelSpecBase):
    model_type: Literal["rf"]
    estimator_pickle: str | None = None


class ICARSpec(_ModelSpecBase):
    model_type: Literal["icar"]
    estimator_pickle: str | None = None
    rho_path: str | None = None


class JNRSpec(_ModelSpecBase):
    model_type: Literal["jnr"]
    dist_thresh: float | None = None
    dist_bins: tuple[float, ...] = ()
    defrate_files: dict[str, str] = Field(default_factory=dict)


class MWSpec(_ModelSpecBase):
    model_type: Literal["mw"]
    dist_thresh: float | None = None
    win_size_list: tuple[int, ...] = ()
    ldefrate_files: dict[str, str] = Field(default_factory=dict)


ModelSpec = Annotated[
    Union[GLMSpec, RFSpec, ICARSpec, JNRSpec, MWSpec],
    Field(discriminator="model_type"),
]

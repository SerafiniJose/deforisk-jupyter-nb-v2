"""ProjectSession: the runtime aggregate root over an inert ProjectDocument.

Never serialized. The only thing that crosses save/load/worker boundaries is
the frozen ProjectDocument reached via snapshot().
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Optional

from box import Box
from pydantic import BaseModel, ConfigDict

from spatialrisk.document import (
    ProjectDocument,
    LocalRasterSpec,
    LocalVectorSpec,
    GEESpec,
    VariableId,
    DatasetSpec,
    PredictionSpec,
    GEERecipe,
)


class FolderResolver:
    """Resolves project folder structure; preserves the iteration-suffix mechanic.

    `folders(it_name="run1")` returns a Box where the per-iteration folders
    carry a `<it_name>_` prefix, mirroring Project.initialize_folders. Creating
    folders lazily — no filesystem side effect at import time.
    """

    def __init__(self, project_name: str, data_root: Path):
        self.project_name = project_name
        self.data_root = Path(data_root)

    def folders(self, step: Optional[str] = None, it_name: str = "") -> Box:
        if step and not it_name:
            raise ValueError("A suffix must be provided when a specific step is specified.")

        prefix = f"{it_name}_" if it_name else ""
        project_folder = self.data_root / self.project_name
        project_folder.mkdir(parents=True, exist_ok=True)

        folders = {
            "data_raw_folder": project_folder / "data_raw",
            "processed_data_folder": project_folder / "data",
            "sampling_folder": project_folder / "far_samples",
            "rmj_mw": project_folder / "rmj_mw",
            "plots_folder": project_folder / "plots",
            "rmj_bm": project_folder / f"{prefix}rmj_bm",
            "glm_model": project_folder / f"{prefix}far_glm",
            "icar_model": project_folder / f"{prefix}far_icar",
            "rf_model": project_folder / f"{prefix}far_rf",
        }

        if step:
            folders[step].mkdir(parents=True, exist_ok=True)
        else:
            for folder in folders.values():
                folder.mkdir(parents=True, exist_ok=True)

        folders.update({
            "data_root": self.data_root,
            "project_folder": project_folder,
        })
        return Box(folders)


class VariableHandle:
    """Thin, never-serialized handle: a (session, source, key) pointer + spec access.

    The geoprocessing methods are the Phase F6 bridge: each derives an explicit
    ``out_path`` from the session's processed-data folder using the legacy
    name/history/year convention, delegates to the matching stateless function
    in :mod:`spatialrisk.geoprocessing`, registers the returned spec into
    ``processed_variables``, and returns a handle to the new processed spec.
    No ``self.project`` reach-through, no implicit save.
    """

    def __init__(self, session: "ProjectSession", source: str, key: str):
        self._session = session
        self.source = source
        self.key = key

    @property
    def spec(self):
        return self._session._collection(self.source).get(self.key)

    def to_ref(self) -> VariableId:
        spec = self.spec
        return VariableId(source=self.source, name=spec.name, year=spec.year)

    # ------------------------------------------------------------------ #
    # Output-path convention (mirrors LocalRasterVar processed filenames)
    # ------------------------------------------------------------------ #
    def _processed_out_path(self, suffix: str) -> Path:
        spec = self.spec
        folder = self._session.folders().processed_data_folder
        history = tuple(getattr(spec, "processing_history", ()) or ())
        filename_suffix = "_".join([*history, suffix]) if history else suffix
        year_suffix = f"_{spec.year}" if spec.year else ""
        return folder / f"{spec.name}_{filename_suffix}{year_suffix}.tif"

    def _register_processed(self, new_spec) -> "VariableHandle":
        self._session.add_local_raster(new_spec, processed=True)
        return self._session.get_variable_handle(
            new_spec.name, year=new_spec.year, source="processed"
        )

    # ------------------------------------------------------------------ #
    # Geoprocessing bridge (delegates to stateless seams)
    # ------------------------------------------------------------------ #
    def reproject_and_match(
        self, resampling: Optional[str] = None, output_suffix: str = "reprojected_matched"
    ) -> "VariableHandle":
        from spatialrisk import geoprocessing

        out_path = self._processed_out_path(output_suffix)
        geobox = self._session._base_geobox()
        new_spec = geoprocessing.reproject_and_match(
            self.spec, geobox=geobox, out_path=str(out_path), resampling=resampling
        )
        return self._register_processed(new_spec)

    def rasterize(self, rasterization_method=None) -> "VariableHandle":
        from spatialrisk import geoprocessing

        out_path = self._processed_out_path("rasterized")
        base_geobox = self._session._base_geobox()
        new_spec = geoprocessing.rasterize_vector(
            self.spec,
            base_geobox=base_geobox,
            out_path=str(out_path),
            rasterization_method=rasterization_method,
        )
        return self._register_processed(new_spec)

    def apply_post_processing(self, post_process) -> "VariableHandle":
        from spatialrisk import geoprocessing

        suffix = getattr(post_process, "value", str(post_process))
        out_path = self._processed_out_path(suffix)
        new_spec = geoprocessing.apply_post_processing(
            self.spec, post_process, out_path=str(out_path)
        )
        return self._register_processed(new_spec)


@dataclass(frozen=True)
class ResolvedFeature:
    name: str
    path: str
    raster_type: Any
    levels: Optional[list] = None


@dataclass(frozen=True)
class ResolvedTarget:
    name: str
    path: str
    raster_type: Any


@dataclass(frozen=True)
class ResolvedDataset:
    name: str
    year: Optional[int]
    target: Optional[ResolvedTarget]
    features: tuple = field(default_factory=tuple)


class DatasetHandle:
    """Thin handle resolving a DatasetSpec's VarRefs into paths + raster_type + levels."""

    def __init__(self, session: "ProjectSession", key: str):
        self._session = session
        self.key = key

    @property
    def spec(self):
        return self._session._doc.datasets.get(self.key)

    def _resolve_ref(self, ref):
        return self._session.get_variable(ref.name, year=ref.year, source=ref.source)

    def resolve(self) -> ResolvedDataset:
        from spatialrisk.variables.models import RasterType
        from spatialrisk.far_helpers import get_categorical_levels

        spec = self.spec
        # --- target (temporal/static year rules ported from Dataset.set_target) ---
        target = None
        if spec.target_ref is not None:
            name, source = spec.target_ref.name, spec.target_ref.source
            is_temporal = self._session.is_temporal(name, source=source)
            if is_temporal and spec.target_ref.year is None:
                years = self._session.get_variable_years(name, source=source)
                raise ValueError(
                    f"Target variable '{name}' is multitemporal. "
                    f"You must specify a year. Available years: {years}"
                )
            tspec = self._resolve_ref(spec.target_ref)
            if tspec is None:
                raise ValueError(f"Target variable '{name}' not found.")
            target = ResolvedTarget(name=name, path=tspec.path, raster_type=getattr(tspec, "raster_type", None))

        # --- features: path + raster_type + lazy categorical levels ---
        features = []
        for ref in spec.feature_refs:
            fspec = self._resolve_ref(ref)
            if fspec is None:
                # preserve "feature not found -> warn + skip"
                import warnings
                warnings.warn(f"Feature '{ref.name}' not found; skipping.", UserWarning, stacklevel=2)
                continue
            rtype = getattr(fspec, "raster_type", None)
            levels = None
            if rtype == RasterType.categorical:
                levels = get_categorical_levels(fspec)   # reads fspec.path
            features.append(ResolvedFeature(name=ref.name, path=fspec.path, raster_type=rtype, levels=levels))

        return ResolvedDataset(name=spec.name, year=spec.year, target=target, features=tuple(features))


class ModelHandle:
    """Thin handle delegating fit/apply to a predictor collaborator (Phase E)."""

    def __init__(self, session: "ProjectSession", key: str, predictor: Any = None):
        self._session = session
        self.key = key
        self._predictor = predictor

    @property
    def spec(self):
        return self._session._doc.models.get(self.key)

    def fit(self, **kw):
        if self._predictor is None:
            raise ValueError("No predictor injected for ModelHandle.fit().")
        return self._predictor.fit(self._session, self.key, **kw)

    def apply(self, out_path: str, mask: Optional[str] = None, **kw):
        if self._predictor is None:
            raise ValueError("No predictor injected for ModelHandle.apply().")
        return self._predictor.apply(self._session, self.key, out_path, mask=mask, **kw)


class ProjectSession:
    """Ergonomic, mutable-feeling wrapper over an immutable ProjectDocument.

    Mutations never touch the document in place: each goes through the
    validated `_replace` primitive, which round-trips the document through
    `model_validate` and bumps `doc_version`.
    """

    def __init__(
        self,
        doc: ProjectDocument,
        *,
        store: Any = None,
        estimator_store: Any = None,
        gee: Any = None,
    ) -> None:
        self._doc = doc
        self.doc_version: int = 0
        self.store = store
        self.estimator_store = estimator_store
        self.gee = gee
        # Driver-side ONLY; keyed by model_key; NEVER shipped to workers.
        self.estimator_cache: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    @classmethod
    def from_document(
        cls,
        doc: ProjectDocument,
        *,
        store: Any = None,
        estimator_store: Any = None,
        gee: Any = None,
    ) -> "ProjectSession":
        return cls(doc, store=store, estimator_store=estimator_store, gee=gee)

    @classmethod
    def create(cls, name: str, *, store: Any = None, estimator_store: Any = None, gee: Any = None) -> "ProjectSession":
        doc = ProjectDocument(project_name=name)
        return cls(doc, store=store, estimator_store=estimator_store, gee=gee)

    @classmethod
    def open(cls, name: str, *, store: Any, estimator_store: Any = None, gee: Any = None) -> "ProjectSession":
        doc = store.load(name)
        return cls(doc, store=store, estimator_store=estimator_store, gee=gee)

    def save(self) -> str:
        if self.store is None:
            raise ValueError("No store injected; pass store= to create/open.")
        return self.store.save(self._doc)

    def snapshot(self) -> ProjectDocument:
        """Return the current inert document (the crossing-boundary artifact)."""
        return self._doc

    @property
    def project_name(self) -> str:
        return self._doc.project_name

    # ------------------------------------------------------------------ #
    # Mutation primitive (validated — never bypass via model_copy)
    # ------------------------------------------------------------------ #
    def _replace(self, **changes: Any) -> ProjectDocument:
        """Replace the document wholesale through full validation.

        Validates FIRST, then commits, so a rejected mutation leaves `_doc`
        and `doc_version` untouched. Re-runs every validator so the JSON-only /
        no-`ee` type boundary holds on the mutation path.
        """
        merged = self._doc.model_dump() | changes
        validated = ProjectDocument.model_validate(merged)  # raises before commit
        self._doc = validated
        self.doc_version += 1
        return self._doc

    # ------------------------------------------------------------------ #
    # Storage-key helper + variable mutators
    # ------------------------------------------------------------------ #
    @staticmethod
    def _storage_key(spec) -> str:
        year = getattr(spec, "year", None)
        return f"{spec.name}_{year}" if year else spec.name

    def _add_raw(self, key: str, spec) -> ProjectDocument:
        new_raw = dict(self._doc.raw_variables)
        new_raw[key] = spec
        return self._replace(raw_variables=new_raw)

    def _add_processed(self, key: str, spec) -> ProjectDocument:
        new_proc = dict(self._doc.processed_variables)
        new_proc[key] = spec
        return self._replace(processed_variables=new_proc)

    def add_local_raster(
        self, spec: LocalRasterSpec, key: Optional[str] = None, processed: bool = False
    ) -> ProjectDocument:
        storage_key = key or self._storage_key(spec)
        if processed:
            return self._add_processed(storage_key, spec)
        return self._add_raw(storage_key, spec)

    def add_local_vector(self, spec: LocalVectorSpec, key: Optional[str] = None) -> ProjectDocument:
        return self._add_raw(key or self._storage_key(spec), spec)

    def add_gee_variable(self, spec: GEESpec, key: Optional[str] = None) -> ProjectDocument:
        return self._add_raw(key or self._storage_key(spec), spec)

    def set_aoi(self, geojson: dict) -> ProjectDocument:
        return self._replace(aoi=geojson)

    def set_base_raster(self, ref: VariableId) -> ProjectDocument:
        return self._replace(base_raster_ref=ref.model_dump())

    # ------------------------------------------------------------------ #
    # Folder structure + base-raster geobox (Phase F6 bridge support)
    # ------------------------------------------------------------------ #
    def _data_root(self) -> Path:
        """Resolve the data root from the injected store, else the legacy default."""
        root = getattr(self.store, "data_root", None)
        if root is not None:
            return Path(root)
        from spatialrisk.project import downloads_folder

        return Path(downloads_folder)

    def folders(self, step: Optional[str] = None, it_name: str = "") -> Box:
        """Resolve this project's folder structure (delegates to FolderResolver).

        Works with ``store=None`` by falling back to the legacy
        ``spatialrisk.project.downloads_folder``.
        """
        resolver = FolderResolver(self.project_name, self._data_root())
        return resolver.folders(step=step, it_name=it_name)

    def _base_geobox(self):
        """Open the base raster and return its odc geobox (mirrors get_base_geobox)."""
        ref = self._doc.base_raster_ref
        if ref is None:
            raise ValueError(
                "base_raster_ref is unset; call set_base_raster() before "
                "reprojecting/rasterizing against the base grid."
            )
        spec = self.get_variable(ref.name, year=ref.year, source=ref.source)
        if spec is None:
            raise ValueError(
                f"base_raster_ref points at a variable that does not exist: {ref!r}"
            )
        path = getattr(spec, "path", None)
        if path is None:
            raise ValueError(f"base raster spec has no path: {spec!r}")

        import rioxarray
        import odc.geo.xr  # noqa: F401  (registers the .odc accessor)

        if not Path(path).exists():
            raise FileNotFoundError(f"Base raster file not found: {path}")

        raster_array = rioxarray.open_rasterio(
            str(path), chunks="auto", cache=False, lock=False
        )
        return raster_array.odc.geobox

    # ------------------------------------------------------------------ #
    # Registry mutators (dataset / model / prediction)
    # ------------------------------------------------------------------ #
    def register_dataset(self, spec: DatasetSpec, key: Optional[str] = None) -> ProjectDocument:
        new = dict(self._doc.datasets)
        new[key or spec.name] = spec
        return self._replace(datasets=new)

    def register_model(self, spec, key: Optional[str] = None) -> ProjectDocument:
        storage_key = key or (
            f"{spec.model_type}_{spec.name}" if spec.name else spec.model_type
        )
        new = dict(self._doc.models)
        new[storage_key] = spec
        return self._replace(models=new)

    def register_prediction(self, spec: PredictionSpec, key: Optional[str] = None) -> ProjectDocument:
        storage_key = key or (spec.name or f"{spec.model_key}_{spec.year}")
        new = dict(self._doc.predictions)
        new[storage_key] = spec
        return self._replace(predictions=new)

    # ------------------------------------------------------------------ #
    # Registry / query (over _doc, honoring derived_from provenance)
    # ------------------------------------------------------------------ #
    def _collection(self, source: str):
        return (
            self._doc.processed_variables
            if source == "processed"
            else self._doc.raw_variables
        )

    def get_variable(self, name: str, year: Optional[int] = None, source: str = "processed"):
        variables = self._collection(source)
        storage_key = f"{name}_{year}" if year else name
        return variables.get(storage_key)

    def get_all_instances(self, name: str, source: str = "processed") -> list:
        variables = self._collection(source)
        out = []
        for spec in variables.values():
            if spec.name != name:
                continue
            # provenance: skip a GEESpec whose product is already materialized
            if getattr(spec, "kind", None) == "gee" and getattr(spec, "materialized_key", None):
                continue
            out.append(spec)
        return out

    def is_temporal(self, name: str, source: str = "processed") -> bool:
        instances = self.get_all_instances(name, source)
        unique_years = {s.year for s in instances if s.year is not None}
        return len(unique_years) > 1

    def get_variable_years(self, name: str, source: str = "processed") -> list:
        instances = self.get_all_instances(name, source)
        return sorted({s.year for s in instances if s.year is not None})

    def list_sources(self, source: str = "processed") -> list:
        """Keys of GEE recipe descriptors (the source side of provenance)."""
        variables = self._collection(source)
        return sorted(
            key for key, spec in variables.items()
            if getattr(spec, "kind", None) == "gee"
        )

    def list_materialized(self, source: str = "processed") -> list:
        """Keys of on-disk products: local specs + GEE sources NOT yet materialized.

        A GEESpec whose `materialized_key` is set is skipped (its product is
        listed instead) so a recipe and its product never double-count.
        """
        variables = self._collection(source)
        out = []
        for key, spec in variables.items():
            if getattr(spec, "kind", None) == "gee" and getattr(spec, "materialized_key", None):
                continue
            out.append(key)
        return sorted(out)

    def list_variables(self, source: str = "processed", **filters) -> dict:
        if source == "both":
            candidates = {**dict(self._doc.raw_variables), **dict(self._doc.processed_variables)}
        else:
            candidates = dict(self._collection(source))

        if not filters:
            return candidates

        def matches(spec) -> bool:
            for attr, expected in filters.items():
                if not hasattr(spec, attr):
                    return False
                value = getattr(spec, attr)
                if callable(expected):
                    if not expected(value):
                        return False
                elif isinstance(expected, Iterable) and not isinstance(expected, (str, bytes, bytearray)):
                    if value not in expected:
                        return False
                else:
                    if value != expected:
                        return False
            return True

        return {k: v for k, v in candidates.items() if matches(v)}

    def filter_by_tags(self, tags, match_all: bool = False, look_up_in: Optional[str] = None, **filters) -> dict:
        if isinstance(tags, str):
            tags = [tags]
        if look_up_in is None:
            look_up_in = filters.pop("source", "processed")
        variables = self.list_variables(source=look_up_in, **filters)
        result = {}
        for k, spec in variables.items():
            spec_tags = getattr(spec, "tags", ())
            if match_all:
                if all(t in spec_tags for t in tags):
                    result[k] = spec
            else:
                if any(t in spec_tags for t in tags):
                    result[k] = spec
        return result

    def filter_by_attrs(self, source: str = "processed", **attrs) -> dict:
        if "tags" in attrs:
            tags_filter = attrs.pop("tags")
            result = self.filter_by_tags(tags_filter, look_up_in=source)
            if not attrs:
                return result
            return {k: v for k, v in result.items()
                    if all(getattr(v, a, None) == e for a, e in attrs.items())}
        return self.list_variables(source=source, **attrs)

    def get_variable_handle(self, name: str, year: Optional[int] = None, source: str = "processed") -> Optional["VariableHandle"]:
        key = f"{name}_{year}" if year else name
        if key not in self._collection(source):
            return None
        return VariableHandle(self, source, key)

    def base_raster_handle(self) -> Optional["VariableHandle"]:
        ref = self._doc.base_raster_ref
        if ref is None:
            return None
        return self.get_variable_handle(ref.name, year=ref.year, source=ref.source)

    def get_dataset_handle(self, key: str) -> Optional["DatasetHandle"]:
        if key not in self._doc.datasets:
            return None
        return DatasetHandle(self, key)

    def get_model_handle(self, key: str, predictor: Any = None) -> Optional["ModelHandle"]:
        if key not in self._doc.models:
            return None
        return ModelHandle(self, key, predictor=predictor)

    # ------------------------------------------------------------------ #
    # Spec builders (picklable payload factories — Phase G)
    # ------------------------------------------------------------------ #
    def materialize_spec(self, var_key: str) -> "MaterializeSpec":
        """Build a picklable MaterializeSpec for a GEE source variable.

        Resolves the variable's recipe + resolved output path so the worker can
        download it with GEEAdapter.build_image/materialize without the Session.
        """
        from spatialrisk.document import GEESpec

        var = self.get_variable(var_key)
        if not isinstance(var, GEESpec):
            raise TypeError(
                f"materialize_spec requires a GEESpec source variable, "
                f"got {type(var).__name__} for key {var_key!r}."
            )
        recipe = var.recipe
        out_path = self._materialize_out_path(var_key)
        return MaterializeSpec(
            var_key=var_key,
            recipe=recipe,
            out_path=out_path,
            scale=getattr(recipe, "scale", None),
            crs=getattr(recipe, "crs", None),
            export_kind=recipe.export_kind,
            vector_selectors=recipe.vector_selectors,
        )

    # ------------------------------------------------------------------ #
    # Orchestration (delegates to handles/collaborators; real geoprocessing
    # lands in Phase G — these wire ordering + iteration only)
    # ------------------------------------------------------------------ #
    def _materialize_one(self, key: str, spec, **kw):  # pragma: no cover - Phase G fills this in
        raise NotImplementedError("materialize is wired in Phase E/G (GEEAdapter).")

    def materialize_all(self, source: str = "raw", **kw):
        """Materialize each un-materialized GEE source descriptor (runs FIRST)."""
        for key, spec in dict(self._collection(source)).items():
            if getattr(spec, "kind", None) != "gee":
                continue
            if getattr(spec, "materialized_key", None):
                continue
            self._materialize_one(key, spec, **kw)

    def reproject_and_match_all(self, source: str = "raw", **kw):  # pragma: no cover - Phase G
        raise NotImplementedError("reproject_and_match is wired in Phase G.")

    def rasterize_all(self, source: str = "raw", **kw):  # pragma: no cover - Phase G
        raise NotImplementedError("rasterize is wired in Phase G.")

    def process_all(self, source: str = "raw", **kw):
        """materialize (GEE downloads first) -> reproject -> rasterize."""
        self.materialize_all(source=source, **kw)
        self.reproject_and_match_all(source=source, **kw)
        self.rasterize_all(source=source, **kw)


class MaterializeSpec(BaseModel):
    """Picklable, worker-sized recipe for downloading one GEE variable.

    Mirrors §10: (recipe, out_path, scale, crs, export_kind, vector_selectors).
    Carries only the catalogue/asset recipe (no live ``ee`` object) plus the
    resolved output path; the worker rebuilds ``ee`` via GEEAdapter.build_image.
    """

    model_config = ConfigDict(frozen=True)

    var_key: str
    recipe: GEERecipe
    out_path: str
    scale: Optional[float] = None
    crs: Optional[str] = None
    export_kind: Literal["raster", "vector"]
    vector_selectors: Optional[tuple[str, ...]] = None


from typing import Any, Dict
from spatialrisk.sampling import Sampling
from pydantic import JsonValue


class FeatureMeta(BaseModel):
    """Per-feature categorical metadata needed for in-worker formula handling."""

    model_config = ConfigDict(frozen=True)

    name: str
    raster_type: Literal["continuous", "categorical"]
    levels: Optional[tuple[int, ...]] = None


class SupervisedFitSpec(BaseModel):
    """Self-contained fit job for GLM/RF (§10).

    Sampling happens inside the worker (base._prepare_samples -> to_dataframe),
    so the spec carries the raster paths + Sampling + categorical metadata +
    formula + the output CSV destination, NOT a pre-built CSV. The worker
    samples, fits, and emits ``output_sample_path`` + ``estimator_pickle``.
    """

    model_config = ConfigDict(frozen=True)

    model_key: str
    model_type: Literal["glm", "rf"]
    target_path: str
    feature_paths: Dict[str, str]
    feature_meta: tuple[FeatureMeta, ...] = ()
    formula: str
    sampling: Sampling
    output_sample_path: str
    parameters: Dict[str, JsonValue] = {}
    estimator_pickle: Optional[str] = None

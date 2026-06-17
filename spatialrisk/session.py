"""ProjectSession: the runtime aggregate root over an inert ProjectDocument.

Never serialized. The only thing that crosses save/load/worker boundaries is
the frozen ProjectDocument reached via snapshot().
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Optional

from box import Box

from spatialrisk.document import (
    ProjectDocument,
    LocalRasterSpec,
    LocalVectorSpec,
    GEESpec,
    VariableId,
    DatasetSpec,
    PredictionSpec,
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

    def add_local_raster(self, spec: LocalRasterSpec, key: Optional[str] = None) -> ProjectDocument:
        return self._add_raw(key or self._storage_key(spec), spec)

    def add_local_vector(self, spec: LocalVectorSpec, key: Optional[str] = None) -> ProjectDocument:
        return self._add_raw(key or self._storage_key(spec), spec)

    def add_gee_variable(self, spec: GEESpec, key: Optional[str] = None) -> ProjectDocument:
        return self._add_raw(key or self._storage_key(spec), spec)

    def set_aoi(self, geojson: dict) -> ProjectDocument:
        return self._replace(aoi=geojson)

    def set_base_raster(self, ref: VariableId) -> ProjectDocument:
        return self._replace(base_raster_ref=ref.model_dump())

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

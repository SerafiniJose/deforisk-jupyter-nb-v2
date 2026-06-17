"""Persistence layer for spatial-risk projects and models.

Extracts JSON project (de)serialization and model pickle persistence out of the
``Project`` aggregate and ``BaseRiskModel``, behind two small ports:

- :class:`ProjectRepository` -- save/load/list projects as JSON.
- :class:`ModelStore` -- save/load a trained model's pickled estimator.

The behaviour is a verbatim move of the previous in-class logic (so the
notebooks and ``Project.save``/``load`` shims keep working). Entity classes are
imported *inside* methods to keep module import order acyclic.
"""

import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# spatialrisk.persistence is only ever imported lazily (by Project.save/load and
# BaseRiskModel.save/load_model), so by the time this module is first imported the
# entity modules below are already fully initialised -- there is no import cycle,
# and these belong at module top rather than inside the methods.
from spatialrisk.dataset import Dataset
from spatialrisk.mlmodels import (
    GLMModel,
    ICARModel,
    JNRBenchmarkModel,
    MWModel,
    RFModel,
)
from spatialrisk.mlmodels.base import BaseRiskModel
from spatialrisk.project import Project
from spatialrisk.variables import LocalRasterVar, LocalVectorVar


class ProjectRepository:
    """Save, load and list :class:`~spatialrisk.project.Project` instances as JSON.

    Parameters
    ----------
    data_root : Path, optional
        Root directory under which each project lives in ``<data_root>/<name>/``.
        Defaults (lazily, at call time) to ``spatialrisk.project.downloads_folder``
        so production code is unchanged and tests can inject a temp directory.
    """

    def __init__(self, data_root: Optional[Union[str, Path]] = None) -> None:
        self._data_root = Path(data_root) if data_root is not None else None

    @property
    def data_root(self) -> Path:
        if self._data_root is not None:
            return self._data_root
        # Resolved lazily so monkeypatching the module global is honoured.
        from spatialrisk.project import downloads_folder

        return downloads_folder

    def project_dir(self, project_name: str) -> Path:
        return self.data_root / project_name

    # ------------------------------------------------------------------ save
    def save(self, project: "Project", filename: Optional[str] = None) -> Path:
        """Serialize ``project`` to ``<data_root>/<name>/<filename>`` and return the path."""
        type(project)._ensure_model_schemas()

        if filename is None:
            filename = f"{project.project_name}_project.json"

        project_folder = self.project_dir(project.project_name)
        project_folder.mkdir(parents=True, exist_ok=True)
        save_path = project_folder / filename

        data: Dict[str, Any] = {
            "project_name": project.project_name,
            "raw_variables": {},
            "processed_variables": {},
        }

        # Only include years if explicitly set (backward compatibility)
        if project.years is not None:
            data["years"] = project.years

        for var_name, var in project.raw_variables.items():
            data["raw_variables"][var_name] = var.model_dump(mode="json")
        for var_name, var in project.processed_variables.items():
            data["processed_variables"][var_name] = var.model_dump(mode="json")

        if project.base_raster is not None:
            data["base_raster"] = project.base_raster.model_dump(mode="json")

        if project.models:
            data["models"] = {
                key: model.model_dump(mode="json")
                for key, model in project.models.items()
            }

        # Datasets are stored by reference (names) to avoid duplicating the
        # variable payloads they point at.
        if project.datasets:
            data["datasets"] = {}
            for key, dataset in project.datasets.items():
                data["datasets"][key] = {
                    "name": dataset.name,
                    "year": dataset.year,
                    "target_name": dataset.target.name if dataset.target else None,
                    "target_year": dataset.target.year if dataset.target else None,
                    "feature_names": [f.name for f in dataset.features],
                }

        # Serialize registered predictions
        if project.predictions:
            data["predictions"] = {}
            for key, prediction in project.predictions.items():
                data["predictions"][key] = prediction.model_dump(mode="json")

        save_path.write_text(
            json.dumps(data, indent=4, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"Project saved to: {save_path}")
        return save_path

    # ------------------------------------------------------------------ load
    def load(self, project_name: str, filename: Optional[str] = None) -> "Project":
        """Load and reconstruct a project from JSON."""
        Project._ensure_model_schemas()

        if filename is None:
            filename = f"{project_name}_project.json"

        load_path = self.project_dir(project_name) / filename
        if not load_path.exists():
            raise FileNotFoundError(f"Project file not found: {load_path}")

        data = json.loads(load_path.read_text(encoding="utf-8"))

        project = Project(project_name=data["project_name"], years=data.get("years"))

        # str -> Path is coerced by pydantic on construction, so no manual coercion.
        for var_name, var_data in data.get("raw_variables", {}).items():
            var = self._build_variable(var_name, var_data)
            var.project = project
            project.raw_variables[var_name] = var

        for var_name, var_data in data.get("processed_variables", {}).items():
            var = self._build_variable(var_name, var_data)
            var.project = project
            project.processed_variables[var_name] = var

        if data.get("base_raster"):
            project.base_raster = LocalRasterVar(**data["base_raster"])
            project.base_raster.project = project

        if data.get("models"):
            self._load_models(project, data["models"])

        if data.get("datasets"):
            self._load_datasets(project, data["datasets"])

        # Reconstruct registered predictions
        if "predictions" in data and data["predictions"]:
            from spatialrisk.predictions.prediction import Prediction

            for key, pred_data in data["predictions"].items():
                if pred_data.get("path"):
                    pred_data["path"] = Path(pred_data["path"])
                prediction = Prediction(**pred_data)
                prediction.project = project
                project.predictions[key] = prediction
            print(f"Loaded {len(project.predictions)} prediction(s)")

        print(f"Project loaded from: {load_path}")
        print(f"Loaded {len(project.processed_variables)} processed variables")
        return project

    # ------------------------------------------------------------------ list
    def list(self) -> List[str]:
        """Return sorted names of all saved projects under ``data_root``."""
        root = self.data_root
        if not root.exists():
            return []
        return sorted(
            p.name
            for p in root.iterdir()
            if p.is_dir() and (p / f"{p.name}_project.json").exists()
        )

    # --------------------------------------------------------------- helpers
    @staticmethod
    def _build_variable(var_name, var_data):
        data_type = var_data.get("data_type")
        if data_type == "vector":
            return LocalVectorVar(**var_data)
        if data_type == "raster":
            return LocalRasterVar(**var_data)
        raise ValueError(f"Unknown data_type for variable {var_name}: {data_type}")

    @staticmethod
    def _load_models(project: "Project", models_data: Dict[str, Any]) -> None:
        registry = {
            "glm": GLMModel,
            "rf": RFModel,
            "icar": ICARModel,
            "jnr": JNRBenchmarkModel,
            "mw": MWModel,
        }
        for key, model_data in models_data.items():
            model_cls = registry.get(model_data.get("model_type", ""))
            if model_cls is None:
                print(
                    f"  Warning: unknown model_type "
                    f"'{model_data.get('model_type')}' for key '{key}' — skipped"
                )
                continue
            model = model_cls(**model_data)
            model.project = project
            project.models[key] = model
        print(f"Loaded {len(project.models)} model(s)")

    @staticmethod
    def _load_datasets(project: "Project", datasets_data: Dict[str, Any]) -> None:
        for key, ds_data in datasets_data.items():
            ds = Dataset(
                project=project,
                name=ds_data.get("name"),
                year=ds_data.get("year"),
            )
            target_name = ds_data.get("target_name")
            feature_names = ds_data.get("feature_names", [])
            if target_name:
                # The dataset's stored year applies to temporal features (already
                # restored above). Only pass it to set_target when the target is
                # temporal, since set_target rejects a year for static targets.
                target_is_temporal = project.is_temporal(target_name)
                ds.set_target(
                    target_name,
                    year=ds_data.get("year") if target_is_temporal else None,
                )
            if feature_names:
                missing = [n for n in feature_names if not project.get_all_instances(n)]
                valid_names = [n for n in feature_names if project.get_all_instances(n)]
                if missing:
                    print(
                        f"  ⚠ Dataset '{key}': feature(s) not found in processed "
                        f"variables, skipped: {missing}"
                    )
                if valid_names:
                    ds.set_features(valid_names)
            project.datasets[key] = ds
        print(f"Loaded {len(project.datasets)} dataset(s)")


class ModelStore:
    """Persist and restore a trained model's pickled estimator payload."""

    @staticmethod
    def save(model: "BaseRiskModel", folder: Optional[Union[str, Path]] = None) -> Path:
        """Pickle the trained estimator to a date-stamped file and return its path."""
        if model._ml_model is None:
            raise RuntimeError("Model has not been trained. Call fit() first.")

        if folder is not None:
            out_dir = Path(folder)
        else:
            default = model._default_folder()
            if default is None:
                raise RuntimeError(
                    "Cannot determine output folder: no project is attached. "
                    "Set model.project first or pass folder= explicitly."
                )
            out_dir = default

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / model._pickle_filename()

        payload = {
            "ml_model": model._ml_model,
            "design_sample": model._design_sample,
            "formula": model.formula,
            "samples_path": model.samples_path,
        }
        with open(out_path, "wb") as fh:
            pickle.dump(payload, fh)

        model.model_path = out_path
        print(f"  Model saved to: {out_path}")
        return out_path

    @staticmethod
    def load(model: "BaseRiskModel") -> None:
        """Unpickle ``model.model_path`` into the model's private attributes."""
        if model.model_path is None:
            raise RuntimeError("model_path is not set. Train and save first.")
        if not Path(model.model_path).exists():
            raise FileNotFoundError(f"Pickle not found: {model.model_path}")

        with open(model.model_path, "rb") as fh:
            payload = pickle.load(fh)

        model._ml_model = payload["ml_model"]
        model._design_sample = payload.get("design_sample")
        if payload.get("formula") is not None:
            model.formula = payload["formula"]
        if payload.get("samples_path") is not None:
            model.samples_path = payload["samples_path"]


# ---------------------------------------------------------------------------
# v1 persistence layer (ProjectDocument-based) — successor to the classes above.
# Added alongside the legacy ProjectRepository/ModelStore, which stay until the
# Project god object is retired.
# ---------------------------------------------------------------------------
from typing import Protocol, runtime_checkable

from spatialrisk.document import ProjectDocument


@runtime_checkable
class ProjectStorePort(Protocol):
    """Port for persisting/retrieving a :class:`ProjectDocument`."""

    def save(self, doc: "ProjectDocument") -> str: ...
    def load(self, name: str) -> "ProjectDocument": ...
    def list(self) -> List[str]: ...
    def exists(self, name: str) -> bool: ...


@runtime_checkable
class EstimatorStorePort(Protocol):
    """Port for persisting/retrieving a curated estimator pickle payload."""

    def save(self, payload: dict, dest: str) -> str: ...
    def load(self, ref: str) -> dict: ...


class LocalFSEstimatorStore:
    """Persist the curated estimator pickle payload to the local filesystem.

    Payload is the existing GLM/RF/iCAR shape:
    ``{ml_model, design_sample, formula, samples_path}``. JNR/MW have no pickle.
    ``ref``/``dest`` are absolute path strings (see PredictionSpec/ModelSpec
    ``estimator_pickle``).
    """

    def save(self, payload: dict, dest: str) -> str:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as fh:
            pickle.dump(payload, fh)
        return str(dest_path)

    def load(self, ref: str) -> dict:
        ref_path = Path(ref)
        if not ref_path.exists():
            raise FileNotFoundError(f"Estimator pickle not found: {ref_path}")
        with open(ref_path, "rb") as fh:
            return pickle.load(fh)


_V0_RASTER_DROP = {"data_type", "multi_year"}
_V0_VECTOR_DROP = {"data_type", "multi_year"}


def _migrate_v0_variable(var_data: dict) -> dict:
    """Inject the ``kind`` discriminator and strip v0-only fields."""
    data_type = var_data.get("data_type")
    out = {k: v for k, v in var_data.items()}
    if data_type == "raster":
        out["kind"] = "local_raster"
        for k in _V0_RASTER_DROP:
            out.pop(k, None)
    elif data_type == "vector":
        out["kind"] = "local_vector"
        for k in _V0_VECTOR_DROP:
            out.pop(k, None)
        # v0 stored a nullable rasterization_method; v1 requires one.
        if out.get("rasterization_method") is None:
            out["rasterization_method"] = "binary"
    else:
        raise ValueError(f"Unknown v0 data_type for variable: {data_type!r}")
    return out


def _resolve_base_raster_ref(
    base_raster: dict, raw_vars: dict, processed_vars: dict
) -> Optional[dict]:
    """Resolve a v0 embedded base_raster dict to a VariableId dict.

    Matches by (name, year), preferring processed over raw (base rasters are
    products of reprojection). Returns a plain dict for VariableId, or None.
    """
    name = base_raster.get("name")
    year = base_raster.get("year")

    def _match(registry: dict) -> bool:
        return any(
            v.get("name") == name and v.get("year") == year
            for v in registry.values()
        )

    if _match(processed_vars):
        source = "processed"
    elif _match(raw_vars):
        source = "raw"
    else:
        # No registry match — default to processed (base rasters live there).
        source = "processed"
    return {"source": source, "name": name, "year": year}


_MODEL_COMMON = (
    "name", "model_type", "project_name", "dataset_name", "target_name",
    "feature_names", "year", "formula", "parameters", "sampling",
    "samples_path", "trained", "trained_at", "n_samples", "deviance",
)
_KNOWN_MODEL_TYPES = {"glm", "rf", "icar", "jnr", "mw"}


def _migrate_v0_model(model_data: dict) -> Optional[dict]:
    """Convert a v0 model dict to a typed v1 ModelSpec dict, or None to skip."""
    mtype = model_data.get("model_type", "")
    if mtype not in _KNOWN_MODEL_TYPES:
        print(
            f"  Warning: unknown model_type {mtype!r} — skipped during migration"
        )
        return None

    out = {k: model_data[k] for k in _MODEL_COMMON if k in model_data}

    if mtype in ("glm", "rf"):
        out["estimator_pickle"] = model_data.get("model_path")
    elif mtype == "icar":
        out["estimator_pickle"] = model_data.get("model_path")
        out["rho_path"] = model_data.get("rho_path")
    elif mtype == "jnr":
        out["dist_thresh"] = model_data.get("dist_thresh")
        out["dist_bins"] = tuple(model_data.get("dist_bins") or ())
        out["defrate_files"] = dict(model_data.get("defrate_files") or {})
    elif mtype == "mw":
        out["dist_thresh"] = model_data.get("dist_thresh")
        out["win_size_list"] = tuple(model_data.get("win_size_list") or ())
        out["ldefrate_files"] = dict(model_data.get("ldefrate_files") or {})
    return out


def _migrate_v0_to_v1(data: dict) -> dict:
    """Convert a pre-``schema_version`` (v0) project dict to a v1 dict."""
    out: dict = {
        "schema_version": 1,
        "project_name": data["project_name"],
        "raw_variables": {
            key: _migrate_v0_variable(v)
            for key, v in data.get("raw_variables", {}).items()
        },
        "processed_variables": {
            key: _migrate_v0_variable(v)
            for key, v in data.get("processed_variables", {}).items()
        },
    }
    models_data = data.get("models")
    if models_data:
        migrated_models = {}
        for key, model_data in models_data.items():
            spec = _migrate_v0_model(model_data)
            if spec is not None:
                migrated_models[key] = spec
        out["models"] = migrated_models

    base_raster = data.get("base_raster")
    if base_raster:
        out["base_raster_ref"] = _resolve_base_raster_ref(
            base_raster,
            data.get("raw_variables", {}),
            data.get("processed_variables", {}),
        )
    return out


class LocalFSProjectStore:
    """Persist a :class:`ProjectDocument` to ``<data_root>/<name>/<name>_project.json``.

    Pure ``model_dump_json``/``model_validate_json`` — the discriminated unions
    dispatch automatically. ``data_root`` is injectable (tests + future remote
    adapter). When omitted, it resolves lazily to ``project.downloads_folder``
    so production code is unchanged.
    """

    def __init__(self, data_root: Optional[Union[str, Path]] = None) -> None:
        self._data_root = Path(data_root) if data_root is not None else None

    @property
    def data_root(self) -> Path:
        if self._data_root is not None:
            return self._data_root
        from spatialrisk.project import downloads_folder

        return downloads_folder

    def _project_file(self, name: str) -> Path:
        return self.data_root / name / f"{name}_project.json"

    def save(self, doc: "ProjectDocument") -> str:
        save_path = self._project_file(doc.project_name)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_text(doc.model_dump_json(indent=2), encoding="utf-8")
        return str(save_path)

    def load(self, name: str) -> "ProjectDocument":
        load_path = self._project_file(name)
        if not load_path.exists():
            raise FileNotFoundError(f"Project file not found: {load_path}")
        raw = load_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if "schema_version" not in data:
            data = _migrate_v0_to_v1(data)
            return ProjectDocument.model_validate(data)
        return ProjectDocument.model_validate_json(raw)

    def exists(self, name: str) -> bool:
        return self._project_file(name).exists()

    def list(self) -> List[str]:
        root = self.data_root
        if not root.exists():
            return []
        return sorted(
            p.name
            for p in root.iterdir()
            if p.is_dir() and (p / f"{p.name}_project.json").exists()
        )

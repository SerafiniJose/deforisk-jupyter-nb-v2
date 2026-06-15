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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:  # avoid import cycles at module load
    from spatialrisk.mlmodels.base import BaseRiskModel
    from spatialrisk.project import Project


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

        save_path.write_text(
            json.dumps(data, indent=4, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"Project saved to: {save_path}")
        return save_path

    # ------------------------------------------------------------------ load
    def load(self, project_name: str, filename: Optional[str] = None) -> "Project":
        """Load and reconstruct a project from JSON."""
        from spatialrisk.project import Project
        from spatialrisk.variables import LocalRasterVar, LocalVectorVar

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
            var = self._build_variable(var_name, var_data, LocalRasterVar, LocalVectorVar)
            var.project = project
            project.raw_variables[var_name] = var

        for var_name, var_data in data.get("processed_variables", {}).items():
            var = self._build_variable(var_name, var_data, LocalRasterVar, LocalVectorVar)
            var.project = project
            project.processed_variables[var_name] = var

        if data.get("base_raster"):
            project.base_raster = LocalRasterVar(**data["base_raster"])
            project.base_raster.project = project

        if data.get("models"):
            self._load_models(project, data["models"])

        if data.get("datasets"):
            self._load_datasets(project, data["datasets"])

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
    def _build_variable(var_name, var_data, LocalRasterVar, LocalVectorVar):
        data_type = var_data.get("data_type")
        if data_type == "vector":
            return LocalVectorVar(**var_data)
        if data_type == "raster":
            return LocalRasterVar(**var_data)
        raise ValueError(f"Unknown data_type for variable {var_name}: {data_type}")

    @staticmethod
    def _load_models(project: "Project", models_data: Dict[str, Any]) -> None:
        from spatialrisk.mlmodels import (
            GLMModel,
            ICARModel,
            JNRBenchmarkModel,
            MWModel,
            RFModel,
        )

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
        from spatialrisk.dataset import Dataset

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

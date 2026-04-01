"""Base class for risk probability models.

Provides a generic Pydantic-based foundation for ML models that:
- Own a Dataset and Sampling object; generate training samples internally
- Store dataset metadata, formula, parameters, and training date
- Generate raster predictions from a Dataset object
- Serialize to/from JSON for project persistence
"""

import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from spatialrisk.sampling import Sampling


class BaseRiskModel(BaseModel):
    """Generic base class for risk probability ML models.

    Stores all model metadata in serializable fields. The actual trained
    ML object is held in a private attribute and persisted as a pickle file
    whose name includes a timestamp to prevent overwriting.

    Attributes:
    ----------
    name : str, optional
        Base name for the model, e.g. "calibration". Used in pickle filename.
    model_type : str
        Model type identifier: "glm", "rf", or "icar".
    project_name : str, optional
        Name of the parent project. Used for path reconstruction after load.
    dataset_name : str, optional
        Name of the dataset used for training, e.g. "calibration_2020".
    target_name : str, optional
        Name of the target variable, e.g. "forest_loss_2015_2020".
    feature_names : list of str
        Names of the feature variables used in training.
    year : int, optional
        Year associated with the training dataset.
    formula : str, optional
        Patsy formula string used for training.
    parameters : dict
        Model-specific hyperparameters (solver, n_trees, mcmc iterations, …).
    sampling : Sampling, optional
        Sampling configuration used to generate the training samples.
    model_path : Path, optional
        Path to the saved pickle file.
    samples_path : Path, optional
        Path to the CSV file used for training (if samples came from a file).
    trained : bool
        Whether the model has been fitted.
    trained_at : str, optional
        ISO-format datetime string set when fit() completes.
    n_samples : int, optional
        Number of samples used during training.
    deviance : float, optional
        Model deviance (2 × log-loss × n_samples) from training.
    project : any
        Live Project reference. Excluded from serialization.
    dataset : any
        Live Dataset reference. Excluded from serialization.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Identity
    name: Optional[str] = None
    model_type: str = ""

    # Dataset metadata (serializable)
    project_name: Optional[str] = None
    dataset_name: Optional[str] = None
    target_name: Optional[str] = None
    feature_names: List[str] = Field(default_factory=list)
    year: Optional[int] = None

    # Formula and parameters
    formula: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    sampling: Optional[Sampling] = None

    # File paths
    model_path: Optional[Path] = None
    samples_path: Optional[Path] = None

    # Training results
    trained: bool = False
    trained_at: Optional[str] = None
    n_samples: Optional[int] = None
    deviance: Optional[float] = None

    # Live references — excluded from serialization
    project: Optional[Any] = Field(default=None, exclude=True, repr=False)
    dataset: Optional[Any] = Field(default=None, exclude=True, repr=False)

    # In-memory ML objects — not serialized
    _ml_model: Any = PrivateAttr(default=None)
    _x_design_info: Any = PrivateAttr(default=None)
    # Small training sample used to reconstruct DesignInfo after loading
    _design_sample: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _prepare_samples(
        self,
        formula: Optional[str] = None,
        output_csv: Optional[Union[str, Path]] = None,
    ):
        """Generate samples DataFrame and resolve formula.

        Called internally by fit(). Populates target_name, feature_names,
        year, and formula fields from the attached dataset.

        Parameters
        ----------
        formula : str, optional
            Override formula. Falls back to self.formula, then auto-generates
            using generate_patsy_formula(self.dataset).
        output_csv : str or Path, optional
            If provided, saves the full training DataFrame to this CSV path
            and sets self.samples_path.

        Returns
        -------
        df : pd.DataFrame
            Sampled training data from dataset.to_dataframe().
        resolved_formula : str
            The formula to use for training.
        """
        from spatialrisk.far_helpers import generate_patsy_formula

        if self.dataset is None:
            raise ValueError("dataset must be set before calling fit().")
        if self.sampling is None:
            raise ValueError("sampling must be set before calling fit().")

        df = self.dataset.to_dataframe(sampling=self.sampling, output_csv=output_csv)
        if output_csv is not None:
            self.samples_path = Path(output_csv)

        # Populate serializable metadata from dataset
        self.target_name = self.dataset.target.name
        self.feature_names = [v.name for v in self.dataset.features]
        if self.dataset.year is not None:
            self.year = self.dataset.year

        # Resolve formula: argument > self.formula > auto-generate
        resolved = formula or self.formula or generate_patsy_formula(self.dataset)
        self.formula = resolved
        return df, resolved

    def _resolve_dataset(self, dataset: Optional[Any]) -> Any:
        """Return dataset to use for apply(), validating feature compatibility."""
        active = dataset if dataset is not None else self.dataset
        if active is None:
            raise ValueError(
                "No dataset available. Pass dataset= or set model.dataset"
                " before calling apply()."
            )
        # Validate features when a different dataset is provided
        if dataset is not None and dataset is not self.dataset and self.feature_names:
            available = {v.name for v in dataset.features}
            missing = [f for f in self.feature_names if f not in available]
            if missing:
                raise ValueError(
                    f"Provided dataset is missing required feature(s): {missing}"
                )
        return active

    def _stamp_now(self) -> str:
        """Return current datetime as ISO string and set trained_at."""
        self.trained_at = datetime.now().isoformat()
        return self.trained_at

    def _pickle_filename(self) -> str:
        """Build a date-stamped pickle filename to avoid overwriting."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = self.name or "model"
        return f"{self.model_type}_{base}_{ts}.pickle"

    def _default_folder(self) -> Optional[Path]:
        """Return the project model folder for this model type, if available."""
        if self.project is None:
            return None
        folders = self.project.folders
        key_map = {
            "glm": "glm_model",
            "rf": "rf_model",
            "icar": "icar_model",
        }
        folder_key = key_map.get(self.model_type)
        if folder_key and hasattr(folders, folder_key):
            return getattr(folders, folder_key)
        return None

    # ------------------------------------------------------------------
    # Fit / Apply (implemented in subclasses)
    # ------------------------------------------------------------------

    def fit(
        self,
        formula: Optional[str] = None,
        folder: Optional[Union[str, Path]] = None,
    ) -> "BaseRiskModel":
        """Train the model using the attached dataset and sampling.

        Parameters
        ----------
        formula : str, optional
            Patsy formula. If omitted, falls back to self.formula or
            auto-generates via generate_patsy_formula(self.dataset).
        folder : str or Path, optional
            Folder for saving the model pickle.

        Returns
        -------
        self
        """
        raise NotImplementedError("Subclasses must implement fit()")

    def apply(
        self,
        output_file: Union[str, Path],
        dataset: Optional[Any] = None,
        mask: Optional[Union[str, Path]] = None,
        mask_value: Union[int, float, list] = 0,
    ) -> Path:
        """Generate a probability raster from a Dataset object.

        Parameters
        ----------
        output_file : str or Path
            Path for the output GeoTIFF.
        dataset : Dataset, optional
            Dataset with target and features configured. If omitted, uses
            self.dataset. When provided, must contain all features in
            self.feature_names.
        mask : str or Path, optional
            Path to a mask raster. Pixels matching ``mask_value`` (or the
            raster's nodata) are set to nodata (65535) in the output.
            If omitted, prediction runs over the full raster stack.
        mask_value : int, float, or list of int/float, optional
            Value(s) in the mask raster that identify pixels to suppress.
            Defaults to 0. Ignored when ``mask`` is None.

        Returns
        -------
        Path
            Path to the written GeoTIFF.
        """
        raise NotImplementedError("Subclasses must implement apply()")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, folder: Optional[Union[str, Path]] = None) -> Path:
        """Save the trained ML object as a date-stamped pickle file.

        Parameters
        ----------
        folder : str or Path, optional
            Target folder. Falls back to the project model folder, then cwd.

        Returns
        -------
        Path
            Path to the written pickle file.
        """
        if self._ml_model is None:
            raise RuntimeError("Model has not been trained. Call fit() first.")

        # Resolve output folder
        if folder is not None:
            out_dir = Path(folder)
        else:
            default = self._default_folder()
            if default is None:
                raise RuntimeError(
                    "Cannot determine output folder: no project is attached. "
                    "Set model.project first or pass folder= explicitly."
                )
            out_dir = default

        out_dir.mkdir(parents=True, exist_ok=True)
        filename = self._pickle_filename()
        out_path = out_dir / filename

        payload = {
            "ml_model": self._ml_model,
            "design_sample": self._design_sample,
            "formula": self.formula,
            "samples_path": self.samples_path,
        }
        with open(out_path, "wb") as fh:
            pickle.dump(payload, fh)

        self.model_path = out_path
        print(f"  Model saved to: {out_path}")
        return out_path

    def load_model(self) -> None:
        """Load the ML object from self.model_path into memory."""
        if self.model_path is None:
            raise RuntimeError("model_path is not set. Train and save first.")
        if not Path(self.model_path).exists():
            raise FileNotFoundError(f"Pickle not found: {self.model_path}")
        with open(self.model_path, "rb") as fh:
            payload = pickle.load(fh)
        self._ml_model = payload["ml_model"]
        self._design_sample = payload.get("design_sample")
        if payload.get("formula") is not None:
            self.formula = payload["formula"]
        if payload.get("samples_path") is not None:
            self.samples_path = payload["samples_path"]

    # ------------------------------------------------------------------
    # Project registration
    # ------------------------------------------------------------------

    def register(
        self,
        project: Any,
        key: Optional[str] = None,
        auto_save: bool = True,
    ) -> None:
        """Register this model in the project's models dict.

        Parameters
        ----------
        project : Project
            Parent project instance.
        key : str, optional
            Storage key. Defaults to "{model_type}_{name}" or "{model_type}".
        auto_save : bool
            If True, calls project.save() after registering.
        """
        self.project = project
        self.project_name = project.project_name

        storage_key = key or (
            f"{self.model_type}_{self.name}" if self.name else self.model_type
        )
        project.models[storage_key] = self
        print(f"  Model registered as project.models['{storage_key}']")

        if auto_save:
            project.save()

    # ------------------------------------------------------------------
    # Serialization override
    # ------------------------------------------------------------------

    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Exclude live references (project, dataset) from serialization."""
        kwargs.setdefault("exclude", set())
        if isinstance(kwargs["exclude"], set):
            kwargs["exclude"] = kwargs["exclude"] | {"project", "dataset"}
        return super().model_dump(**kwargs)

"""Base class for risk probability models.

Provides a generic Pydantic-based foundation for ML models that:
- Own a Dataset and Sampling object; generate training samples internally
- Store dataset metadata, formula, parameters, and training date
- Generate raster predictions from a Dataset object
- Serialize to/from JSON for project persistence
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import rasterio
from patsy import dmatrices
from patsy.highlevel import build_design_matrices
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

        Returns:
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

        Returns:
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

        Returns:
        -------
        Path
            Path to the written GeoTIFF.
        """
        # forestatrisk is an optional heavy dependency -- keep it lazy so the
        # package imports (and models construct/serialize) without it installed.
        import forestatrisk as far

        if self._ml_model is None:
            self.load_model()

        self._check_apply_preconditions()

        active_dataset = self._resolve_dataset(dataset)

        if self._x_design_info is None:
            if self.samples_path is not None and Path(self.samples_path).exists():
                _df = pd.read_csv(self.samples_path).dropna()
                _, x_ref = dmatrices(self.formula, _df, NA_action="drop")
                self._x_design_info = x_ref.design_info
            else:
                raise RuntimeError(
                    "Cannot reconstruct design info: samples_path not set or "
                    "file missing. Re-run fit() to regenerate samples."
                )

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        feature_paths = {var.name: var.path for var in active_dataset.features}
        label = self.model_type.upper() or type(self).__name__
        print(f"\n🗺  Predicting {label} raster → {output_file}")

        with rasterio.open(active_dataset.target.path) as ref:
            profile = ref.profile.copy()
            target_transform = ref.transform
        profile.update(dtype="uint16", count=1, nodata=0)

        _mask_values = (
            (mask_value if isinstance(mask_value, (list, tuple)) else [mask_value])
            if mask is not None
            else None
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            blockinfo = far.misc.makeblock(str(active_dataset.target.path))
            nblock, nblock_x = blockinfo[0], blockinfo[1]
            x_off, y_off, nx, ny = blockinfo[3], blockinfo[4], blockinfo[5], blockinfo[6]

            for b in range(nblock):
                px = b % nblock_x
                py = b // nblock_x
                col_start, row_start = x_off[px], y_off[py]
                n_cols, n_rows = nx[px], ny[py]
                window = rasterio.windows.Window(col_start, row_start, n_cols, n_rows)
                block_bounds = rasterio.windows.bounds(window, target_transform)

                # Mask block (read by geographic bounds so a differently-gridded
                # mask is resampled onto the target grid before suppression).
                mask_invalid = np.zeros(n_rows * n_cols, dtype=bool)
                if mask is not None:
                    with rasterio.open(mask) as mask_src:
                        mask_win = rasterio.windows.from_bounds(
                            *block_bounds, mask_src.transform
                        )
                        mask_block = mask_src.read(
                            1,
                            window=mask_win,
                            out_shape=(n_rows, n_cols),
                            resampling=rasterio.enums.Resampling.nearest,
                        )
                        mask_nodata = mask_src.nodata
                    mask_invalid = np.isin(mask_block.ravel(), _mask_values)
                    if mask_nodata is not None:
                        mask_invalid |= mask_block.ravel() == mask_nodata

                # Read feature data for this block, replacing nodata with NaN.
                block_dict = {}
                for name, path in feature_paths.items():
                    with rasterio.open(path) as src:
                        arr = src.read(1, window=window).astype(float)
                        if src.nodata is not None:
                            arr[arr == src.nodata] = np.nan
                    block_dict[name] = arr.ravel()

                block_df_full = pd.DataFrame(block_dict)
                valid_mask = (
                    ~block_df_full.isnull().any(axis=1).to_numpy() & ~mask_invalid
                )
                block_df = block_df_full[valid_mask]

                out_arr = np.zeros(n_rows * n_cols, dtype=np.uint16)
                if not block_df.empty:
                    (x_block,) = build_design_matrices(
                        [self._x_design_info], block_df, NA_action="drop"
                    )
                    x_arr = np.asarray(x_block)
                    proba = self._predict_block(
                        x_arr, valid_mask, window, block_bounds, n_rows, n_cols
                    )
                    out_arr[valid_mask] = far.misc.rescale(
                        np.asarray(proba, dtype=float)
                    ).astype(np.uint16)

                dst.write(out_arr.reshape(n_rows, n_cols), 1, window=window)

        print(f"✓ {label} raster written: {output_file}")
        self._register_prediction(output_file, dataset=active_dataset)
        return output_file

    def _predict_block(self, x_arr, valid_mask, window, block_bounds, n_rows, n_cols):
        """Return P(event) for the valid rows of one block. Override per model.

        Default: a supervised classifier exposing ``predict_proba`` (GLM, RF).
        """
        return self._ml_model.predict_proba(x_arr)[:, 1]

    def _check_apply_preconditions(self) -> None:
        """Hook for model-specific apply() preconditions (default: none)."""
        return None

    def _model_key(self) -> str:
        """Return this model's key in ``project.models``.

        Prefers an identity reverse-lookup (honors custom keys passed to
        ``register``/``add_model``); falls back to the default key formula.
        """
        if self.project is not None:
            for key, model in self.project.models.items():
                if model is self:
                    return key
        return f"{self.model_type}_{self.name}" if self.name else self.model_type

    def _register_prediction(
        self,
        path: Union[str, Path],
        dataset: Optional[Any] = None,
        year: Optional[int] = None,
        window: Optional[int] = None,
        auto_save: bool = True,
    ) -> Optional[Any]:
        """Build and register a Prediction for an output raster.

        No-ops (returns None) when the model has no project reference, so direct
        ``apply()`` calls outside a project context keep working unchanged.
        """
        if self.project is None:
            return None

        from spatialrisk.predictions.prediction import (
            Prediction,
            build_dataset_snapshot,
        )

        ds = dataset if dataset is not None else self.dataset
        prediction = Prediction(
            path=Path(path),
            model_key=self._model_key(),
            dataset_name=(getattr(ds, "name", None) or self.dataset_name or "unknown"),
            year=year if year is not None else self.year,
            window=window,
            model_snapshot=self.model_dump(mode="json"),
            dataset_snapshot=build_dataset_snapshot(ds),
        )
        prediction.add_to_project(self.project, auto_save=auto_save)
        return prediction

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, folder: Optional[Union[str, Path]] = None) -> Path:
        """Save the trained ML object as a date-stamped pickle file.

        Parameters
        ----------
        folder : str or Path, optional
            Target folder. Falls back to the project model folder, then cwd.

        Returns:
        -------
        Path
            Path to the written pickle file.
        """
        from spatialrisk.persistence import ModelStore

        return ModelStore.save(self, folder)

    def load_model(self) -> None:
        """Load the ML object from self.model_path into memory."""
        from spatialrisk.persistence import ModelStore

        ModelStore.load(self)

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

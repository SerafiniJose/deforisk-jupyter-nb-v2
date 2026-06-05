"""JNR Benchmark unsupervised deforestation risk model.

Implements the Jurisdictional and Nested REDD+ (JNR) benchmark approach:
stratifies the landscape by distance-to-forest-edge bins × subjurisdictions
and assigns historical deforestation rates as vulnerability scores.

All processing functions live in ``spatialrisk.rmj`` and work with
generic binary (0/1) rasters.  This class is a thin orchestration layer
that ties datasets and project metadata to those functions.

Workflow
--------
1. ``fit()``  — calls ``rmj.dist_edge_threshold`` and
               ``rmj.compute_dist_bins`` for a training period.
2. ``apply()`` — calls ``rmj.vulnerability_map`` and
                ``rmj.defrate_per_class`` for any period.

Variable naming
---------------
Features are looked up by exact name in the Dataset.  Defaults are
``forest_edge``, ``forest``, and ``subj``.  Override on the instance::

    jnr = JNRBenchmarkModel(
        name="calibration",
        forest_edge_var="forest_gfc_edge",
        forest_var="forest_gfc",
    )
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from spatialrisk.mlmodels.base import BaseRiskModel


class JNRBenchmarkModel(BaseRiskModel):
    """JNR Benchmark deforestation risk model.

    Thin orchestration layer over the functions in ``spatialrisk.rmj``.
    All raster processing logic lives there; this class manages datasets,
    Pydantic metadata, and project registration.

    Attributes
    ----------
    blk_rows : int
        Number of raster rows per processing block (default: 128).
    defor_threshold : float
        Distance percentile for the forest-edge threshold (default: 99.5).
        Can be overridden per ``fit()`` call.
    max_dist : int
        Maximum distance (m) for the bin arange (default: 5000).
        Can be overridden per ``fit()`` call.
    forest_value : int
        Pixel value meaning "forest" in the binary forest raster (default: 1).
    defor_value : int
        Pixel value meaning "deforested" in the binary deforestation raster
        (default: 1).
    dist_thresh : float, optional
        Distance-to-edge threshold in metres. Populated by ``fit()``.
    dist_bins : list of float
        Distance bin edges for vulnerability stratification.
        Populated by ``fit()``.
    forest_edge_var : str
        Dataset feature name for the distance-to-edge raster
        (default: ``"forest_edge"``).
    forest_var : str
        Dataset feature name for the binary forest raster, used by
        ``apply()`` (default: ``"forest"``).
    subj_var : str
        Dataset feature name for the subjurisdiction raster, used by
        ``apply()`` (default: ``"subj"``).
    defrate_files : dict
        Paths to the defrate CSV files written by ``apply()``, keyed by
        period name.  Pass ``defrate_files.get("calibration")`` as
        ``deforate_model`` when running the validation or forecast period.
    """

    model_type: str = "jnr"
    blk_rows: int = 128

    # Training parameters — set on the model or override per fit() call
    defor_threshold: float = 99.5
    max_dist: int = 5000

    # Binary raster pixel-value conventions
    forest_value: int = 1
    defor_value: int = 1

    # State persisted after fit()
    dist_thresh: Optional[float] = None
    dist_bins: List[float] = Field(default_factory=list)

    # Configurable feature-variable name mappings
    forest_edge_var: str = "forest_edge"
    forest_var: str = "forest"
    subj_var: str = "subj"

    # Defrate CSV paths produced by apply(), keyed by period name
    defrate_files: Dict[str, Path] = Field(default_factory=dict)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _default_folder(self) -> Optional[Path]:
        """Return project rmj_bm folder if a project is attached."""
        if self.project is None:
            return None
        folders = self.project.folders
        if hasattr(folders, "rmj_bm"):
            return Path(getattr(folders, "rmj_bm"))
        return None

    def _get_feature(self, dataset: Any, var_name: str) -> Path:
        """Return the path of a named feature from a dataset.

        Parameters
        ----------
        dataset : Dataset
        var_name : str
            Exact name to look for in ``dataset.features``.

        Returns
        -------
        Path

        Raises
        ------
        ValueError
            If the feature is not found, listing available names.
        """
        for var in dataset.features:
            if var.name == var_name:
                return var.path
        available = [v.name for v in dataset.features]
        raise ValueError(
            f"JNRBenchmarkModel requires feature '{var_name}' but it was not "
            f"found in the dataset features.\n"
            f"  Available: {available}\n"
            f"  Set model.forest_edge_var / forest_var / subj_var to match "
            f"your variable names."
        )

    # ------------------------------------------------------------------
    # Fit
    # ------------------------------------------------------------------

    def fit(
        self,
        dataset: Optional[Any] = None,
        defor_values: Union[int, List[int]] = 1,
        defor_threshold: Optional[float] = None,
        max_dist: Optional[int] = None,
        folder: Optional[Union[str, Path]] = None,
    ) -> "JNRBenchmarkModel":
        """Compute distance threshold and bin edges for a training period.

        Parameters
        ----------
        dataset : Dataset, optional
            Dataset with:

            * **target** — binary deforestation raster (``self.defor_value``
              where deforested)
            * **feature** named ``self.forest_edge_var`` — distance-to-edge
              raster (metres)

            The dataset's ``name`` is used as the period sub-folder.
            Falls back to ``self.dataset`` if not provided.
        defor_values : int or list of int
            Pixel values in the deforestation raster that count as deforested.
            Default: ``1``.
        defor_threshold : float, optional
            Distance percentile for the forest-edge cutoff.  Overrides
            ``self.defor_threshold`` and persists on the model.
        max_dist : int, optional
            Maximum distance (m) for the bin arange.  Overrides
            ``self.max_dist`` and persists on the model.
        folder : str or Path, optional
            Root output folder.  Defaults to the project ``rmj_bm`` folder,
            then the current working directory.

        Returns
        -------
        self
        """
        from spatialrisk.rmj import dist_edge_threshold, compute_dist_bins

        # Resolve dataset
        active = dataset if dataset is not None else self.dataset
        if active is None:
            raise ValueError(
                "No dataset available. Pass dataset= or set model.dataset "
                "before calling fit()."
            )
        if active.target is None:
            raise ValueError(
                "Dataset has no target set. Call dataset.set_target() before fit()."
            )

        # Validate target is a deforestation variable (by tag)
        target_tags = getattr(active.target, "tags", []) or []
        if "deforestation" not in target_tags:
            raise ValueError(
                f"JNRBenchmarkModel requires a target variable tagged 'deforestation', "
                f"but '{active.target.name}' has tags {target_tags}. "
                f"Ensure the variable was created/processed with the 'deforestation' tag."
            )

        period = active.name or self.name
        if period is None:
            raise ValueError(
                "Cannot determine period name: neither dataset.name nor "
                "model.name is set."
            )

        # Persist training parameter overrides
        if defor_threshold is not None:
            self.defor_threshold = defor_threshold
        if max_dist is not None:
            self.max_dist = max_dist

        # Extract file paths from dataset
        deforestation_file = active.target.path
        forest_edge_file = self._get_feature(active, self.forest_edge_var)

        out_root = (
            Path(folder)
            if folder is not None
            else (self._default_folder() or Path.cwd())
        )
        period_dir = out_root / period
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n🔧 JNR fit — period='{period}'")
        print(deforestation_file, forest_edge_file)

        # Step 1: Distance-to-edge threshold
        result = dist_edge_threshold(
            deforestation_file=deforestation_file,
            forest_edge_file=forest_edge_file,
            defor_values=defor_values,
            defor_threshold=self.defor_threshold,
            max_dist=self.max_dist,
            blk_rows=self.blk_rows,
            tab_file=period_dir / "tab_dist.csv",
            fig_file=period_dir / f"perc_dist_{period}.png",
            verbose=False,
        )
        self.dist_thresh = float(result["dist_thresh"])
        print(f"  dist_thresh={self.dist_thresh:.1f} m")

        # Step 2: Distance bins for vulnerability stratification
        bins = compute_dist_bins(
            forest_edge_file=forest_edge_file,
            dist_thresh=self.dist_thresh,
        )
        self.dist_bins = [float(b) for b in bins]
        print(f"  dist_bins: {len(self.dist_bins)} edges")

        # Populate serialisable metadata (mirrors _prepare_samples() pattern)
        self.target_name = active.target.name
        self.feature_names = [v.name for v in active.features]
        self.dataset_name = active.name
        if active.year is not None:
            self.year = active.year
        self.dataset = active

        self._stamp_now()
        self.trained = True
        print(f"✓ JNR fit complete — trained_at={self.trained_at}")
        return self

    # ------------------------------------------------------------------
    # Apply
    # ------------------------------------------------------------------

    def apply(
        self,
        output_file: Union[str, Path],
        dataset: Optional[Any] = None,
        time_interval: Optional[int] = None,
        deforate_model: Optional[Union[str, Path]] = None,
    ) -> Path:
        """Generate a vulnerability map and deforestation-rate table.

        Dataset features required:

        * **target** — binary deforestation raster
        * ``self.forest_var`` — binary forest at initial year
        * ``self.forest_edge_var`` — distance-to-edge (metres)
        * ``self.subj_var`` — subjurisdiction integer IDs

        Parameters
        ----------
        output_file : str or Path
            Output vulnerability GeoTIFF path.
        dataset : Dataset, optional
            Falls back to ``self.dataset`` if not provided.  The dataset's
            ``name`` is used as the period name.
        time_interval : int
            Number of years in the period (required).
        deforate_model : str or Path, optional
            Calibration / historical defrate CSV.  When provided, model rates
            are applied with quantity-adjustment correction.  Pass
            ``model.defrate_files.get("calibration")`` for validation or
            ``model.defrate_files.get("historical")`` for forecast.

        Returns
        -------
        Path
            Path to the written vulnerability GeoTIFF.
        """
        from spatialrisk.rmj import vulnerability_map, defrate_per_class

        if not self.dist_bins:
            raise RuntimeError("Model has not been fitted. Call fit() first.")
        if self.dist_thresh is None:
            raise RuntimeError("dist_thresh not set. Call fit() first.")

        # Resolve dataset
        active = dataset if dataset is not None else self.dataset
        if active is None:
            raise ValueError(
                "No dataset available. Pass dataset= or set model.dataset "
                "before calling apply()."
            )
        if time_interval is None:
            raise ValueError(
                "time_interval is required for apply(). "
                "Provide the number of years in the period, e.g. time_interval=5."
            )

        # Extract file paths from dataset
        deforestation_file = active.target.path
        forest_file = self._get_feature(active, self.forest_var)
        forest_edge_file = self._get_feature(active, self.forest_edge_var)
        subj_file = self._get_feature(active, self.subj_var)

        period = active.name or self.name

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        defrate_tab = output_file.parent / f"defrate_cat_bm_{period}.csv"

        print(f"\n🗺  JNR apply — period='{period}' → {output_file.name}")

        # Step 1: Vulnerability map
        vulnerability_map(
            forest_file=forest_file,
            forest_edge_file=forest_edge_file,
            dist_bins=self.dist_bins,
            subj_file=subj_file,
            output_file=output_file,
            blk_rows=self.blk_rows,
            verbose=False,
        )

        # Step 2: Deforestation rate per class
        defrate_per_class(
            forest_file=forest_file,
            deforestation_file=deforestation_file,
            vulnerability_file=output_file,
            time_interval=time_interval,
            tab_file_defrate=defrate_tab,
            deforate_model=(
                Path(deforate_model) if deforate_model is not None else None
            ),
            forest_value=self.forest_value,
            defor_value=self.defor_value,
            blk_rows=self.blk_rows,
        )

        # Store defrate path for use in subsequent apply() calls
        self.defrate_files[period] = defrate_tab

        print(f"✓ JNR apply complete — {output_file}")
        return output_file

    # ------------------------------------------------------------------
    # Persistence — no pickle, state is in Pydantic fields
    # ------------------------------------------------------------------

    def save(self, folder: Optional[Union[str, Path]] = None) -> None:
        """No-op: JNR model state lives in Pydantic fields (dist_thresh, dist_bins).

        Call ``model.register(project)`` to persist via project JSON.
        """
        print(
            "  JNR model state persisted via Pydantic fields "
            "(dist_thresh, dist_bins). "
            "Use register(project) to save to project JSON."
        )
        return None

    def load_model(self) -> None:
        """Verify model state is populated (dist_bins and dist_thresh)."""
        if not self.dist_bins:
            raise RuntimeError(
                "dist_bins is empty. Ensure fit() was called and "
                "the model was registered."
            )
        if self.dist_thresh is None:
            raise RuntimeError(
                "dist_thresh is None. Ensure fit() was called and "
                "the model was registered."
            )
        print(
            f"  JNR model OK — dist_thresh={self.dist_thresh:.1f} m, "
            f"{len(self.dist_bins)} bin edges."
        )

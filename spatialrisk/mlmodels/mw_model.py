"""Moving Window (MW) unsupervised risk model using riskmapjnr.

Computes local event rates within spatial moving windows of specified sizes
to produce a probability/risk raster.  No machine-learning training is
required — the model is a spatial heuristic based on neighbourhood event
density.  Although the defaults use deforestation terminology, the model
is generic and works with any binary target variable.

All processing functions live in ``spatialrisk.rmj`` and work with two explicit
binary (0/1) rasters — a deforestation layer and a forest-at-start layer. This
class is a thin orchestration layer that ties datasets and project metadata to
those functions.

Workflow
--------
1. ``fit()``  — calls ``rmj.deforrate.dist_edge_threshold`` and
               ``rmj.deforrate.local_defor_rate`` (one per window size) for a
               training period (typically "calibration" or "historical").
2. ``apply()`` — for any period, calls ``rmj.set_defor_cat_zero`` and
                ``rmj.deforrate.defrate_per_cat`` using the ldefrate rasters
                produced in ``fit()``.

Variable naming
---------------
Features are looked up by exact name in the Dataset.  Defaults are
``forest_edge`` and ``forest``.  Override on the instance::

    mw = MWModel(
        name="calibration",
        forest_edge_var="forest_gfc_edge",
        forest_var="forest_gfc",
    )
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from spatialrisk.mlmodels.base import BaseRiskModel


class MWModel(BaseRiskModel):
    """Moving Window risk model.

    Thin orchestration layer over the functions in ``spatialrisk.rmj``.
    All raster processing logic lives there; this class manages datasets,
    Pydantic metadata, and project registration.

    Although field names use "forest" / "defor" conventions, the model is
    generic — any binary (0/1) target raster and any distance raster can
    be used by overriding ``forest_edge_var``, ``forest_var``,
    ``forest_value``, and ``defor_value``.

    Attributes
    ----------
    win_size_list : list of int
        Moving window sizes in pixels (default: [5, 11, 21]).
    blk_rows : int
        Number of raster rows per processing block (default: 256).
    defor_threshold : float
        Distance percentile used to define the edge threshold
        (default: 99.5).  Can be overridden per ``fit()`` call.
    max_dist : int
        Maximum distance (m) for the bin arange (default: 5000).
        Can be overridden per ``fit()`` call.
    rescale_max_val : int
        Maximum value for ldefrate rescaling (default: 65535 to match
        the uint16 scale used by GLM/RF/iCAR models).
    forest_value : int
        Pixel value meaning "background" / "no event" in the binary
        reference raster (default: 1).
    defor_value : int
        Pixel value meaning "event" in the binary target raster
        (default: 1).
    dist_thresh : float, optional
        Distance-to-edge threshold in metres.  Populated by ``fit()``.
    ldefrate_files : dict
        Mapping of ``{"win_size": path_to_ldefrate.tif}``.  Populated by
        ``fit()``.  Keys are strings to ensure JSON serialisability.
    forest_edge_var : str
        Dataset feature name for the distance-to-edge raster
        (default: ``"forest_edge"``).
    forest_var : str
        Dataset feature name for the binary reference raster, used by
        ``apply()`` (default: ``"forest"``).
    defor_var : str
        Dataset layer name for the binary forest-loss (deforestation) raster.
        When set, this named layer (a feature or the target) is used as the
        event raster; when empty, the dataset target is used (default: ``""``).
    """

    model_type: str = "mw"
    win_size_list: List[int] = Field(default_factory=lambda: [5, 11, 21])
    blk_rows: int = 256
    defor_threshold: float = 99.5
    max_dist: int = 5000
    rescale_max_val: int = 65535

    # Binary raster pixel-value conventions
    forest_value: int = 1
    defor_value: int = 1

    # State persisted after fit() — no pickle, paths to raster files
    dist_thresh: Optional[float] = None
    ldefrate_files: Dict[str, Path] = Field(default_factory=dict)

    # Configurable feature-variable name mappings
    forest_edge_var: str = "forest_edge"
    forest_var: str = "forest"
    # Forest-loss (deforestation) layer. When set, this named dataset layer is
    # used as the binary event raster instead of the dataset target, letting the
    # user pick which variable in their dataset is the forest-loss layer. Empty
    # falls back to the dataset target (backward-compatible default).
    defor_var: str = ""

    def output_files(self) -> List[Path]:
        """MW persists one deforestation-rate raster per window size."""
        files = super().output_files()
        files.extend(Path(p) for p in self.ldefrate_files.values() if p)
        return files

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _default_folder(self) -> Optional[Path]:
        """Return project rmj_mw folder if a project is attached."""
        if self.project is None:
            return None
        folders = self.project.folders
        if hasattr(folders, "rmj_mw"):
            return Path(getattr(folders, "rmj_mw"))
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
            f"MWModel requires feature '{var_name}' but it was not "
            f"found in the dataset features.\n"
            f"  Available: {available}\n"
            f"  Set model.forest_edge_var / forest_var to match "
            f"your variable names."
        )

    def _resolve_defor_file(self, dataset: Any) -> Path:
        """Resolve the binary forest-loss (deforestation) raster path.

        If ``defor_var`` is set, look it up by exact name among the dataset's
        features or its target, so the user controls which layer is the
        forest-loss input.  When empty, fall back to the dataset target
        (backward-compatible default).
        """
        if self.defor_var:
            for var in dataset.features:
                if var.name == self.defor_var:
                    return var.path
            if dataset.target is not None and dataset.target.name == self.defor_var:
                return dataset.target.path
            available = [v.name for v in dataset.features]
            if dataset.target is not None:
                available.append(dataset.target.name)
            raise ValueError(
                f"MWModel forest-loss variable '{self.defor_var}' was not found "
                f"in the dataset.\n"
                f"  Available: {available}\n"
                f"  Set model.defor_var to match a layer in your dataset."
            )
        if dataset.target is None:
            raise ValueError(
                "Dataset has no target set and defor_var is not configured. "
                "Set the forest-loss variable (defor_var) or call "
                "dataset.set_target() before fit()."
            )
        return dataset.target.path

    # ------------------------------------------------------------------
    # Fit
    # ------------------------------------------------------------------

    def fit(
        self,
        dataset: Optional[Any] = None,
        defor_threshold: Optional[float] = None,
        time_interval: Optional[int] = None,
        folder: Optional[Union[str, Path]] = None,
    ) -> "MWModel":
        """Compute local event rates for a training period.

        Runs ``rmj.dist_edge_threshold`` to determine the distance-to-edge
        cutoff, then ``rmj.local_defor_rate`` for each window size.

        Parameters
        ----------
        dataset : Dataset, optional
            Dataset with:

            * **target** — binary event raster (``self.defor_value`` where
              the event occurred).
            * **feature** named ``self.forest_edge_var`` — distance-to-edge
              raster (metres).

            The dataset's ``name`` is used as the period sub-folder.
            Falls back to ``self.dataset`` if not provided.
        defor_threshold : float, optional
            Distance percentile for the forest-edge cutoff.  Overrides
            ``self.defor_threshold`` and persists on the model.
        time_interval : int
            Number of years covered by the period (required).
        folder : str or Path, optional
            Root output folder.  Defaults to the project ``rmj_mw`` folder,
            then the current working directory.

        Returns
        -------
        self
        """
        import numpy as np

        from spatialrisk.rmj import deforrate

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

        if time_interval is None:
            raise ValueError(
                "time_interval is required for fit(). "
                "Provide the number of years in the period, e.g. time_interval=5."
            )

        period = active.name or self.name
        if period is None:
            raise ValueError(
                "Cannot determine period name: neither dataset.name nor "
                "model.name is set."
            )

        # Persist training parameter override
        if defor_threshold is not None:
            self.defor_threshold = defor_threshold

        # Extract file paths from dataset. The forest-at-start layer is now an
        # explicit input to local_defor_rate (the moving-window denominator),
        # so it is required at fit() time as well as apply() time.
        deforestation_file = self._resolve_defor_file(active)
        forest_edge_file = self._get_feature(active, self.forest_edge_var)
        forest_file = self._get_feature(active, self.forest_var)

        out_root = (
            Path(folder) if folder is not None else (self._default_folder() or Path.cwd())
        )
        period_dir = out_root / period
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n🔧 MW fit — period='{period}', windows={self.win_size_list}")

        # Step 1: Distance-to-edge threshold (native two-layer; deforrate treats
        # ``defor == 1`` as the event under the 1=event convention).
        result = deforrate.dist_edge_threshold(
            defor_file=deforestation_file,
            dist_file=forest_edge_file,
            dist_bins=np.arange(0, self.max_dist, step=30),
            defor_threshold=self.defor_threshold,
            blk_rows=self.blk_rows,
            tab_file_dist=period_dir / "tab_dist.csv",
            fig_file_dist=period_dir / f"perc_dist_{period}.png",
            verbose=False,
        )
        self.dist_thresh = float(result["dist_thresh"])
        print(f"  dist_thresh={self.dist_thresh:.1f} m")

        # Step 2: Local deforestation rate per window size
        ldefrate_files: Dict[str, Path] = {}
        for win_size in self.win_size_list:
            ldefrate_file = period_dir / f"ldefrate_mw_{win_size}.tif"
            print(f"  local_defor_rate — window {win_size}×{win_size} px...")
            deforrate.local_defor_rate(
                defor_file=deforestation_file,
                forest_file=forest_file,
                ldefrate_file=ldefrate_file,
                win_size=win_size,
                time_interval=time_interval,
                rescale_min_val=2,
                rescale_max_val=self.rescale_max_val,
                blk_rows=self.blk_rows,
                verbose=False,
            )
            ldefrate_files[str(win_size)] = ldefrate_file

        self.ldefrate_files = ldefrate_files

        # Populate serialisable metadata (mirrors JNRBenchmarkModel pattern)
        self.target_name = active.target.name
        self.feature_names = [v.name for v in active.features]
        self.dataset_name = active.name
        if active.year is not None:
            self.year = active.year
        self.dataset = active

        self._stamp_now()
        self.trained = True
        print(
            f"✓ MW fit complete — {len(ldefrate_files)} ldefrate files, "
            f"trained_at={self.trained_at}"
        )
        return self

    # ------------------------------------------------------------------
    # Apply
    # ------------------------------------------------------------------

    def apply(
        self,
        dataset: Optional[Any] = None,
        time_interval: Optional[int] = None,
        output_folder: Optional[Union[str, Path]] = None,
        output_file: Optional[Union[str, Path]] = None,
        mask: Optional[Union[str, Path]] = None,
        mask_value: Union[int, float, list] = 0,
    ) -> Dict[str, Path]:
        """Generate probability maps for a given period.

        Uses the ``ldefrate`` rasters from ``fit()`` to run
        ``rmj.set_defor_cat_zero`` and ``rmj.defrate_per_cat``.

        Dataset features required:

        * **target** — binary event raster
        * ``self.forest_edge_var`` — distance-to-edge (metres) at period start
        * ``self.forest_var`` — binary reference raster at period start

        Parameters
        ----------
        dataset : Dataset, optional
            Falls back to ``self.dataset`` if not provided.  The dataset's
            ``name`` is used as the period name.
        time_interval : int
            Number of years in the period (required).
        output_folder : str or Path, optional
            Root output folder.  Defaults to project ``rmj_mw`` folder.
        output_file : optional
            Unused; kept for API consistency with supervised models.
        mask : optional
            Unused; kept for API consistency with supervised models.
        mask_value : optional
            Unused; kept for API consistency.

        Returns
        -------
        dict
            ``{win_size_str: Path}`` for each probability raster produced.
        """
        from spatialrisk.rmj import set_defor_cat_zero, deforrate

        if not self.ldefrate_files:
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
                "Provide the number of years in the period, e.g. time_interval=4."
            )

        period = active.name or self.name

        # Extract file paths from dataset
        deforestation_file = self._resolve_defor_file(active)
        forest_edge_file = self._get_feature(active, self.forest_edge_var)
        forest_file = self._get_feature(active, self.forest_var)

        out_root = (
            Path(output_folder)
            if output_folder is not None
            else (self._default_folder() or Path.cwd())
        )
        period_dir = out_root / period
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n🗺  MW apply — period='{period}', windows={self.win_size_list}")

        output_files: Dict[str, Path] = {}
        for win_size_str, ldefrate_file in self.ldefrate_files.items():
            ldefrate_file = Path(ldefrate_file)
            if not ldefrate_file.exists():
                raise FileNotFoundError(
                    f"ldefrate raster not found: {ldefrate_file}. "
                    "Re-run fit() or restore the file."
                )

            prob_file = period_dir / f"prob_mw_{win_size_str}_{period}.tif"
            defrate_tab = period_dir / f"defrate_cat_mw_{win_size_str}_{period}.csv"

            set_defor_cat_zero(
                ldefrate_file=ldefrate_file,
                forest_edge_file=forest_edge_file,
                dist_thresh=self.dist_thresh,
                output_file=prob_file,
                blk_rows=self.blk_rows,
                verbose=False,
            )

            deforrate.defrate_per_cat(
                defor_file=deforestation_file,
                forest_file=forest_file,
                riskmap_file=prob_file,
                time_interval=time_interval,
                tab_file_defrate=defrate_tab,
                blk_rows=self.blk_rows,
            )

            output_files[win_size_str] = prob_file
            self._register_prediction(
                prob_file,
                dataset=active,
                window=int(win_size_str) if str(win_size_str).isdigit() else None,
            )
            print(f"  window {win_size_str} → {prob_file.name}")

        print(f"✓ MW apply complete — {len(output_files)} probability maps written")
        return output_files

    # ------------------------------------------------------------------
    # Persistence — no pickle, state is in raster files + Pydantic fields
    # ------------------------------------------------------------------

    def save(self, folder: Optional[Union[str, Path]] = None) -> None:
        """No-op: MW model state is in raster files referenced by ldefrate_files.

        Call ``model.register(project)`` to persist metadata via project JSON.
        """
        print(
            "  MW model state persisted via Pydantic fields "
            "(dist_thresh, ldefrate_files). "
            "Use register(project) to save to project JSON."
        )
        return None

    def load_model(self) -> None:
        """Verify that ldefrate raster files referenced by ldefrate_files exist."""
        if not self.ldefrate_files:
            raise RuntimeError(
                "ldefrate_files is empty. Ensure fit() was called and "
                "the model was registered."
            )
        missing = [
            str(p)
            for p in self.ldefrate_files.values()
            if not Path(p).exists()
        ]
        if missing:
            raise FileNotFoundError(
                f"ldefrate raster(s) not found:\n" + "\n".join(missing)
            )
        print(
            f"  MW model OK — {len(self.ldefrate_files)} ldefrate files verified."
        )

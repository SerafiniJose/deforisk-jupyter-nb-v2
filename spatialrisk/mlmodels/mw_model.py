"""Moving Window (MW) unsupervised deforestation risk model using riskmapjnr.

Computes local deforestation rates within spatial moving windows of specified
sizes to produce a probability/risk raster. No machine-learning training is
required — the model is a spatial heuristic based on neighbourhood event density.

Workflow
--------
1. fit()  — computes dist_edge_threshold and local_defor_rate for a training
            period (typically "calibration" or "historical").
2. apply() — for any period, calls set_defor_cat_zero and defrate_per_cat
             using the ldefrate rasters produced in fit().
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from component.script.mlmodels.base import BaseRiskModel


class MWModel(BaseRiskModel):
    """Moving Window deforestation risk model.

    Wraps the ``riskmapjnr`` moving-window workflow:
    ``dist_edge_threshold`` → ``local_defor_rate`` (fit), then
    ``set_defor_cat_zero`` → ``defrate_per_cat`` (apply).

    Attributes
    ----------
    win_size_list : list of int
        Moving window sizes in pixels (default: [5, 11, 21]).
    blk_rows : int
        Number of raster rows per processing block (default: 256).
    defor_threshold : float
        Distance percentile used to define the forest-edge threshold
        (default: 99.5).
    rescale_max_val : int
        Maximum value for ldefrate rescaling (default: 65535 to match
        the uint16 scale used by GLM/RF/iCAR models).
    dist_thresh : float, optional
        Distance-to-edge threshold in metres. Populated by fit().
    ldefrate_files : dict
        Mapping of ``{"win_size": path_to_ldefrate.tif}``. Populated by fit().
        Keys are strings to ensure JSON serialisability.
    """

    model_type: str = "mw"
    win_size_list: List[int] = Field(default_factory=lambda: [5, 11, 21])
    blk_rows: int = 256
    defor_threshold: float = 99.5
    rescale_max_val: int = 65535

    # State persisted after fit() — no pickle, paths to raster files
    dist_thresh: Optional[float] = None
    ldefrate_files: Dict[str, Path] = Field(default_factory=dict)

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

    # ------------------------------------------------------------------
    # Fit
    # ------------------------------------------------------------------

    def fit(
        self,
        fcc_file: Union[str, Path],
        forest_edge_file: Union[str, Path],
        defor_values: Union[int, List[int]],
        time_interval: int,
        period: str,
        folder: Optional[Union[str, Path]] = None,
    ) -> "MWModel":
        """Compute local deforestation rates for a training period.

        Runs ``rmj.dist_edge_threshold`` to determine the distance-to-edge
        cutoff, then ``rmj.local_defor_rate`` for each window size.

        Parameters
        ----------
        fcc_file : str or Path
            Forest change raster (fcc123 convention: 0=non-forest, 1=deforested
            in calibration period, 2=deforested in later period, 3=remained forest).
        forest_edge_file : str or Path
            Distance-to-forest-edge raster for the training period's initial year.
        defor_values : int or list of int
            Pixel values that encode deforestation (e.g., 1 or [1, 2]).
        time_interval : int
            Number of years covered by the period.
        period : str
            Period name used for output sub-folder, e.g. ``"calibration"``
            or ``"historical"``.
        folder : str or Path, optional
            Root output folder. Defaults to the project ``rmj_mw`` folder,
            then the current working directory.

        Returns
        -------
        self
        """
        import numpy as np
        import riskmapjnr as rmj

        fcc_file = Path(fcc_file)
        forest_edge_file = Path(forest_edge_file)

        out_root = (
            Path(folder) if folder is not None else (self._default_folder() or Path.cwd())
        )
        period_dir = out_root / period
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n🔧 MW fit — period='{period}', windows={self.win_size_list}")

        # Step 1: Distance-to-edge threshold
        tab_file = period_dir / "tab_dist.csv"
        fig_file = period_dir / f"perc_dist_{period}.png"

        result = rmj.dist_edge_threshold(
            fcc_file=str(fcc_file),
            defor_values=defor_values,
            defor_threshold=self.defor_threshold,
            dist_file=str(forest_edge_file),
            dist_bins=np.arange(0, 5000, step=30),
            tab_file_dist=str(tab_file),
            fig_file_dist=str(fig_file),
            blk_rows=self.blk_rows,
            dist_file_available=True,
            check_fcc=False,
            verbose=False,
        )
        self.dist_thresh = float(result["dist_thresh"])
        print(f"  dist_thresh={self.dist_thresh:.1f} m")

        # Step 2: Local deforestation rate per window size
        ldefrate_files: Dict[str, Path] = {}
        for win_size in self.win_size_list:
            ldefrate_file = period_dir / f"ldefrate_mw_{win_size}.tif"
            print(f"  local_defor_rate — window {win_size}×{win_size} px...")
            rmj.local_defor_rate(
                fcc_file=str(fcc_file),
                defor_values=defor_values,
                ldefrate_file=str(ldefrate_file),
                win_size=win_size,
                time_interval=time_interval,
                rescale_min_val=2,
                rescale_max_val=self.rescale_max_val,
                blk_rows=self.blk_rows,
                verbose=False,
            )
            ldefrate_files[str(win_size)] = ldefrate_file

        self.ldefrate_files = ldefrate_files
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
        period: str,
        forest_edge_file: Union[str, Path],
        fcc_file: Union[str, Path],
        defor_values: Union[int, List[int]],
        time_interval: int,
        output_folder: Optional[Union[str, Path]] = None,
        mask: Optional[Union[str, Path]] = None,
        mask_value: Union[int, float, list] = 0,
        dataset: Optional[Any] = None,
        output_file: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Path]:
        """Generate probability maps for a given period.

        Uses the ``ldefrate`` rasters from ``fit()`` to run
        ``rmj.set_defor_cat_zero`` and ``rmj.defrate_per_cat``.

        Parameters
        ----------
        period : str
            Target period name, e.g. ``"validation"`` or ``"forecast"``.
        forest_edge_file : str or Path
            Distance-to-forest-edge raster for this period's initial year.
        fcc_file : str or Path
            Forest change raster for deforestation-rate tabulation.
        defor_values : int or list of int
            Deforestation pixel values for this period.
        time_interval : int
            Number of years in the period.
        output_folder : str or Path, optional
            Root output folder. Defaults to project ``rmj_mw`` folder.
        mask : str or Path, optional
            Unused; kept for API consistency with supervised models.
        mask_value : int, float, or list of int/float, optional
            Unused; kept for API consistency. Defaults to 0.
        dataset : optional
            Unused; kept for API consistency with supervised models.
        output_file : optional
            Unused; output paths are derived from ``output_folder / period /
            prob_mw_{win_size}_{period}.tif``.

        Returns
        -------
        dict
            ``{win_size_str: Path}`` for each probability raster produced.
        """
        import riskmapjnr as rmj

        if not self.ldefrate_files:
            raise RuntimeError("Model has not been fitted. Call fit() first.")
        if self.dist_thresh is None:
            raise RuntimeError("dist_thresh not set. Call fit() first.")

        forest_edge_file = Path(forest_edge_file)
        fcc_file = Path(fcc_file)

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

            rmj.set_defor_cat_zero(
                ldefrate_file=str(ldefrate_file),
                dist_file=str(forest_edge_file),
                dist_thresh=self.dist_thresh,
                ldefrate_with_zero_file=str(prob_file),
                blk_rows=self.blk_rows,
                verbose=False,
            )

            rmj.defrate_per_cat(
                fcc_file=str(fcc_file),
                riskmap_file=str(prob_file),
                time_interval=time_interval,
                period=period,
                tab_file_defrate=str(defrate_tab),
                blk_rows=self.blk_rows,
                verbose=False,
            )

            output_files[win_size_str] = prob_file
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

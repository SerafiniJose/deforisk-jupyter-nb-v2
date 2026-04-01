"""JNR Benchmark unsupervised deforestation risk model using riskmapjnr.

Implements the Jurisdictional and Nested REDD+ (JNR) benchmark approach:
stratifies the landscape by distance-to-forest-edge bins × subjurisdictions
and assigns historical deforestation rates as vulnerability scores.

Workflow
--------
1. fit()  — computes dist_edge_threshold and benchmark.compute_dist_bins for a
            training period (typically "calibration" or "historical").
2. apply() — for any period, calls benchmark.vulnerability_map and
             benchmark.defrate_per_class using the computed dist_bins.
"""

from pathlib import Path
from typing import Any, List, Optional, Union

from pydantic import Field, field_validator

from spatialrisk.mlmodels.base import BaseRiskModel

_JNR_TARGET = "deforestation"


class JNRBenchmarkModel(BaseRiskModel):
    """JNR Benchmark deforestation risk model.

    Wraps the ``riskmapjnr.benchmark`` workflow:
    ``dist_edge_threshold`` → ``benchmark.compute_dist_bins`` (fit), then
    ``benchmark.vulnerability_map`` → ``benchmark.defrate_per_class`` (apply).

    Attributes
    ----------
    blk_rows : int
        Number of raster rows per processing block (default: 128).
    defor_threshold : float
        Distance percentile used to define the forest-edge threshold
        (default: 99.5).
    dist_thresh : float, optional
        Distance-to-edge threshold in metres. Populated by fit().
    dist_bins : list of float
        Distance bin edges used for vulnerability stratification.
        Populated by fit().
    """

    model_type: str = "jnr"
    blk_rows: int = 128
    defor_threshold: float = 99.5

    @field_validator("target_name")
    @classmethod
    def _validate_target(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v != _JNR_TARGET:
            raise ValueError(
                f"JNRBenchmarkModel only supports target '{_JNR_TARGET}', got '{v}'."
            )
        return v

    # State persisted after fit()
    dist_thresh: Optional[float] = None
    dist_bins: List[float] = Field(default_factory=list)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _default_folder(self) -> Optional[Path]:
        """Return project rmj_bm folder if a project is attached."""
        if self.project is None:
            return None
        folders = self.project.folders
        if hasattr(folders, "rmj_bm"):
            return Path(getattr(folders, "rmj_bm"))
        return None

    # ------------------------------------------------------------------
    # Fit
    # ------------------------------------------------------------------

    def fit(
        self,
        fcc_file: Union[str, Path],
        forest_edge_file: Union[str, Path],
        defor_values: Union[int, List[int]],
        period: str,
        folder: Optional[Union[str, Path]] = None,
    ) -> "JNRBenchmarkModel":
        """Compute distance threshold and bin edges for a training period.

        Runs ``rmj.dist_edge_threshold`` to determine the forest-edge
        distance cutoff, then ``rmj.benchmark.compute_dist_bins`` to derive
        the bin edges used for vulnerability stratification.

        Parameters
        ----------
        fcc_file : str or Path
            Forest change raster (fcc123 convention).
        forest_edge_file : str or Path
            Distance-to-forest-edge raster for the training period's initial year.
        defor_values : int or list of int
            Pixel values that encode deforestation (e.g., 1 or [1, 2]).
        period : str
            Period name used for output sub-folder, e.g. ``"calibration"``
            or ``"historical"``.
        folder : str or Path, optional
            Root output folder. Defaults to the project ``rmj_bm`` folder,
            then the current working directory.

        Returns
        -------
        self
        """
        import numpy as np
        import riskmapjnr as rmj

        if self.dataset is not None and getattr(self.dataset, "target", None) is not None:
            if self.dataset.target.name != _JNR_TARGET:
                raise ValueError(
                    f"JNRBenchmarkModel only supports target '{_JNR_TARGET}', "
                    f"got '{self.dataset.target.name}'."
                )

        fcc_file = Path(fcc_file)
        forest_edge_file = Path(forest_edge_file)

        out_root = (
            Path(folder) if folder is not None else (self._default_folder() or Path.cwd())
        )
        period_dir = out_root / period
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n🔧 JNR fit — period='{period}'")

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
            check_fcc=True,
            verbose=False,
        )
        self.dist_thresh = float(result["dist_thresh"])
        print(f"  dist_thresh={self.dist_thresh:.1f} m")

        # Step 2: Distance bins for vulnerability stratification
        bins = rmj.benchmark.compute_dist_bins(
            str(forest_edge_file),
            self.dist_thresh,
        )
        self.dist_bins = [float(b) for b in bins]
        print(f"  dist_bins: {len(self.dist_bins)} edges")

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
        period: str,
        forest_file: Union[str, Path],
        forest_edge_file: Union[str, Path],
        subj_file: Union[str, Path],
        fcc_file: Union[str, Path],
        time_interval: int,
        defor_values: Optional[Union[int, List[int]]] = None,
        deforate_model: Optional[Union[str, Path]] = None,
        mask: Optional[Union[str, Path]] = None,
        mask_value: Union[int, float, list] = 0,
        dataset: Optional[Any] = None,
    ) -> Path:
        """Generate a vulnerability map and deforestation-rate table.

        Calls ``rmj.benchmark.vulnerability_map`` to produce the risk raster,
        then ``rmj.benchmark.defrate_per_class`` to tabulate rates.

        Parameters
        ----------
        output_file : str or Path
            Path for the output vulnerability GeoTIFF (e.g.
            ``rmj_bm/calibration/prob_bm_calibration.tif``).
        period : str
            Period name, e.g. ``"calibration"``, ``"validation"``,
            ``"historical"``, or ``"forecast"``.
        forest_file : str or Path
            Binary forest raster for the period's initial year
            (1 = forest, 0 = non-forest).
        forest_edge_file : str or Path
            Distance-to-forest-edge raster for the period's initial year.
        subj_file : str or Path
            Subjurisdiction raster (integer zones used for stratification).
        fcc_file : str or Path
            Forest change raster for deforestation-rate tabulation.
        time_interval : int
            Number of years in the period.
        defor_values : int or list of int, optional
            Unused; kept for API consistency.
        deforate_model : str or Path, optional
            Path to a calibration-period defrate CSV. Required for
            ``"validation"`` and ``"forecast"`` periods so that the same
            per-class rates computed during calibration are applied.
        mask : optional
            Unused; kept for API consistency.
        mask_value : int, float, or list of int/float, optional
            Unused; kept for API consistency. Defaults to 0.
        dataset : optional
            Unused; kept for API consistency.

        Returns
        -------
        Path
            Path to the written vulnerability GeoTIFF.
        """
        import riskmapjnr as rmj

        if not self.dist_bins:
            raise RuntimeError("Model has not been fitted. Call fit() first.")
        if self.dist_thresh is None:
            raise RuntimeError("dist_thresh not set. Call fit() first.")

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        forest_file = Path(forest_file)
        forest_edge_file = Path(forest_edge_file)
        subj_file = Path(subj_file)
        fcc_file = Path(fcc_file)

        defrate_tab = output_file.parent / f"defrate_cat_bm_{period}.csv"

        print(f"\n🗺  JNR apply — period='{period}' → {output_file.name}")

        # Step 1: Vulnerability map
        rmj.benchmark.vulnerability_map(
            forest_file=str(forest_file),
            dist_file=str(forest_edge_file),
            dist_bins=self.dist_bins,
            subj_file=str(subj_file),
            output_file=str(output_file),
            blk_rows=self.blk_rows,
            verbose=False,
        )

        # Step 2: Deforestation rate per class
        rmj.benchmark.defrate_per_class(
            fcc_file=str(fcc_file),
            vulnerability_file=str(output_file),
            time_interval=time_interval,
            period=period,
            deforate_model=(
                str(deforate_model) if deforate_model is not None else None
            ),
            tab_file_defrate=str(defrate_tab),
            blk_rows=self.blk_rows,
            verbose=False,
        )

        print(f"✓ JNR apply complete — {output_file}")
        return output_file

    # ------------------------------------------------------------------
    # Persistence — no pickle, state is in Pydantic fields (dist_bins, dist_thresh)
    # ------------------------------------------------------------------

    def save(self, folder: Optional[Union[str, Path]] = None) -> None:
        """No-op: JNR model state is in dist_thresh and dist_bins Pydantic fields.

        Call ``model.register(project)`` to persist metadata via project JSON.
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

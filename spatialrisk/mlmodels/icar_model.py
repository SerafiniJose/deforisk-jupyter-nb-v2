"""iCAR Bayesian risk model using forestatrisk with Patsy formulas.

The intrinsic Conditional Auto-Regressive (iCAR) model accounts for
spatial autocorrelation through a latent spatial random effect (rho).
Training uses MCMC via forestatrisk.model_binomial_iCAR.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import numpy as np
import rasterio

from spatialrisk.mlmodels.base import BaseRiskModel


def compute_cell_indices(
    cell_id: "np.ndarray",
    raster_path: str,
    csize_km: float,
) -> "np.ndarray":
    """Convert pixel-based cell_id values to spatial cell indices.

    ``dataset.to_dataframe()`` stores ``cell_id = row * ncols + col`` (a flat
    pixel index).  forestatrisk's ``model_binomial_iCAR`` expects the ``cell``
    column to contain the index into the spatial-cell grid produced by
    ``cellneigh(raster, csize, rank=1)``, i.e. values in ``[0, ncell)``.

    This function performs that conversion using the same algorithm as the
    sampling notebook::

        bigJ = floor((pts_x - Xmin) / csize_m)
        bigI = floor((Ymax  - pts_y) / csize_m)
        cell = bigI * ncol_cells + bigJ

    Parameters
    ----------
    cell_id : array-like of int
        Flat pixel indices (row * ncols + col) from the samples DataFrame.
    raster_path : str
        Path to the reference raster (same one passed to ``cellneigh``).
    csize_km : float
        Spatial cell size in kilometres (must match the value used in
        ``cellneigh``).

    Returns:
    -------
    np.ndarray of int
        Spatial cell indices aligned with the ``cellneigh`` output.
    """
    import rasterio

    with rasterio.open(raster_path) as src:
        gt = src.transform
        ncols_r = src.width
        Xmin = gt.c
        Xmax = gt.c + gt.a * src.width
        Ymax = gt.f

    csize_m = csize_km * 1000
    ncol_cells = int(np.ceil((Xmax - Xmin) / csize_m))

    pixel_ids = np.asarray(cell_id, dtype=int)
    pixel_row = pixel_ids // ncols_r
    pixel_col = pixel_ids % ncols_r
    pts_x = (pixel_col + 0.5) * gt.a + gt.c
    pts_y = (pixel_row + 0.5) * gt.e + gt.f
    bigJ = ((pts_x - Xmin) / csize_m).astype(int)
    bigI = ((Ymax - pts_y) / csize_m).astype(int)
    return bigI * ncol_cells + bigJ


class ICARModel(BaseRiskModel):
    """Bayesian iCAR spatial risk model.

    Requires the ``cell_id`` column present in DataFrames produced by
    ``dataset.to_dataframe()``, which encodes the raster cell index and
    enables construction of the spatial neighbourhood graph.

    Attributes
    ----------
    csize : float
        Cell size (km) for building the spatial neighbourhood (default: 10).
    mcmc : int
        Total MCMC iterations (default: 6000).
    burnin : int
        Number of burn-in iterations (default: 4000).
    thin : int
        Thinning factor (default: 1).
    prior_vrho : float
        Prior variance for rho. -1 uses a uniform prior (default: -1).
    beta_start : float
        Starting value for betas. -99 triggers automatic initialisation
        (default: -99).
    random_seed : int, optional
        Random seed for reproducibility.
    rho_path : Path, optional
        Path to the interpolated rho GeoTIFF saved after training.
    """

    model_type: str = "icar"
    csize: float = 10.0
    mcmc: int = 4000
    burnin: int = 4000
    thin: int = 1
    prior_vrho: float = -1.0
    beta_start: float = -99.0
    random_seed: Optional[int] = None
    rho_path: Optional[Path] = None
    csize_interpolate: float = 0.1

    def fit(
        self,
        formula: Optional[str] = None,
        folder: Optional[Union[str, Path]] = None,
    ) -> "ICARModel":
        """Train the iCAR model via MCMC.

        Parameters
        ----------
        formula : str, optional
            Patsy formula. If omitted, falls back to self.formula or
            auto-generates via generate_patsy_formula(self.dataset).
            The ``cell`` term required by forestatrisk is appended
            automatically if absent.
        folder : str or Path, optional
            Folder for saving the model pickle and rho raster. Defaults to
            the project icar_model folder.

        Returns
        -------
        self
        """
        import forestatrisk as far
        from patsy import dmatrices

        # Auto-save full training CSV if samples_path not already set
        if self.samples_path is None:
            _folder = (
                Path(folder)
                if folder is not None
                else (self._default_folder() or Path.cwd())
            )
            Path(_folder).mkdir(parents=True, exist_ok=True)
            _csv = (
                Path(_folder) / f"samples_{self.model_type}_{self.name or 'model'}.csv"
            )
        else:
            _csv = None

        df, formula = self._prepare_samples(formula, output_csv=_csv)

        if "cell_id" not in df.columns:
            raise ValueError(
                "DataFrame must contain a 'cell_id' column. "
                "Use dataset.to_dataframe() to generate samples."
            )

        # Target raster path — available directly from self.dataset
        raster_path = str(self.dataset.target.path)

        # forestatrisk expects the column to be named "cell" and values must be
        # spatial cell indices matching cellneigh(raster, csize, rank=1).
        # cell_id stores raw pixel indices (row * ncols + col), so we convert.
        df = df.copy()
        df["cell"] = compute_cell_indices(df["cell_id"].values, raster_path, self.csize)

        # Append cell term to formula if not present
        icar_formula = self.formula
        if "+ cell" not in icar_formula and "~cell" not in icar_formula:
            icar_formula = icar_formula + " + cell"

        print(
            f"\n🔧 Training iCAR model "
            f"(mcmc={self.mcmc}, burnin={self.burnin}, csize={self.csize} km)..."
        )

        df = df.dropna()
        y, x = dmatrices(icar_formula, df, NA_action="drop")

        n_obs = len(df)

        print("  Building spatial neighbourhood...")
        n_neighbors, adj = far.cellneigh(raster_path, self.csize, rank=1)

        # MCMC
        mod = far.model_binomial_iCAR(
            suitability_formula=icar_formula,
            data=df,
            n_neighbors=n_neighbors,
            neighbors=adj,
            burnin=self.burnin,
            mcmc=self.mcmc,
            thin=self.thin,
            priorVrho=self.prior_vrho,
            seed=self.random_seed if self.random_seed is not None else 1234,
            verbose=1,
        )

        # Extract only picklable fields from the forestatrisk model object
        # (the full model contains patsy design objects that cannot be pickled)
        self._ml_model = {
            "betas": np.array(mod.betas),
            "rho": np.array(mod.rho),
            "Vrho": float(mod.Vrho) if hasattr(mod, "Vrho") else None,
            "deviance": float(mod.deviance),
            "formula": icar_formula,
        }
        self.n_samples = n_obs
        self.deviance = self._ml_model["deviance"]

        self._stamp_now()
        self.trained = True
        print(
            f"✓ iCAR trained — {self.n_samples:,} samples, "
            f"deviance={self.deviance:.2f}, trained_at={self.trained_at}"
        )

        # Resolve output folder
        out_dir = (
            Path(folder)
            if folder is not None
            else (self._default_folder() or Path.cwd())
        )
        out_dir.mkdir(parents=True, exist_ok=True)

        # Persist the trained estimator via the shared ModelStore (sets model_path).
        # The payload gains a design_sample=None key vs the old inline pickle;
        # base.load_model reads it with .get(), so existing pickles still load.
        self.save(folder=out_dir)

        # Interpolate rho to full raster grid (a separate artifact, not in the pickle).
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = self.name or "model"
        print("  Interpolating rho to raster grid...")
        rho_path = out_dir / f"rho_{base}_{ts}.tif"
        far.interpolate_rho(
            rho=self._ml_model["rho"],
            input_raster=raster_path,
            output_file=str(rho_path),
            csize_orig=self.csize,
            csize_new=self.csize_interpolate,
        )
        self.rho_path = rho_path
        print(f"  Rho raster saved to: {rho_path}")

        return self

    def _check_apply_preconditions(self) -> None:
        """iCAR requires the interpolated rho raster produced by fit()."""
        if self.rho_path is None or not Path(self.rho_path).exists():
            raise RuntimeError(
                "rho_path is not set or file not found. "
                "Ensure the model was trained with fit() before predicting."
            )

    def _predict_block(self, x_arr, valid_mask, window, block_bounds, n_rows, n_cols):
        """iCAR prediction: logit(p) = X @ betas + rho (spatial random effect).

        Reads the interpolated rho raster for this block (bilinear-resampled
        onto the target grid) and adds it to the linear predictor.
        """
        betas = np.array(self._ml_model["betas"])
        with rasterio.open(self.rho_path) as rho_src:
            rho_win = rasterio.windows.from_bounds(*block_bounds, rho_src.transform)
            rho_block = (
                rho_src.read(
                    1,
                    window=rho_win,
                    out_shape=(n_rows, n_cols),
                    resampling=rasterio.enums.Resampling.bilinear,
                )
                .astype(float)
                .ravel()
            )
        rho_valid = rho_block[valid_mask]
        linear_pred = x_arr @ betas[: x_arr.shape[1]] + rho_valid
        return 1.0 / (1.0 + np.exp(-linear_pred))

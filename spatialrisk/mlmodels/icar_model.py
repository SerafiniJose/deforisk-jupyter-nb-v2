"""iCAR Bayesian risk model using forestatrisk with Patsy formulas.

The intrinsic Conditional Auto-Regressive (iCAR) model accounts for
spatial autocorrelation through a latent spatial random effect (rho).
Training uses MCMC via forestatrisk.model_binomial_iCAR.
"""

import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

import numpy as np
import pandas as pd

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

        # Save pickle
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = self.name or "model"
        pickle_path = out_dir / f"icar_{base}_{ts}.pickle"
        payload = {
            "ml_model": self._ml_model,
            "formula": self.formula,
            "samples_path": self.samples_path,
        }
        with open(pickle_path, "wb") as fh:
            pickle.dump(payload, fh)
        self.model_path = pickle_path
        print(f"  iCAR model saved to: {pickle_path}")

        # Interpolate rho to full raster grid
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

    def apply(
        self,
        output_file: Union[str, Path],
        dataset: Optional[Any] = None,
        mask: Optional[Union[str, Path]] = None,
        mask_value: Union[int, float, list] = 0,
    ) -> Path:
        """Generate a spatial deforestation probability GeoTIFF.

        Uses the stored betas and the interpolated rho raster as the
        spatial random effect.

        Parameters
        ----------
        output_file : str or Path
            Path for the output GeoTIFF.
        dataset : Dataset, optional
            Dataset with target and features configured. If omitted, uses
            self.dataset. Must contain all features in self.feature_names.
        mask : str or Path, optional
            Path to a mask raster. Pixels matching ``mask_value`` (or the
            raster's nodata) are set to nodata (0) in the output.
            If omitted, prediction runs over the full raster stack.
        mask_value : int, float, or list of int/float, optional
            Value(s) in the mask raster that identify pixels to suppress.
            Defaults to 0. Ignored when ``mask`` is None.
        """
        import forestatrisk as far
        import rasterio
        from patsy.highlevel import build_design_matrices

        if self._ml_model is None:
            self.load_model()

        active_dataset = self._resolve_dataset(dataset)

        if self._x_design_info is None:
            if self.samples_path is not None and Path(self.samples_path).exists():
                from patsy import dmatrices as _dmatrices

                _df = pd.read_csv(self.samples_path).dropna()
                _, x_ref = _dmatrices(self.formula, _df, NA_action="drop")
                self._x_design_info = x_ref.design_info
            else:
                raise RuntimeError(
                    "Cannot reconstruct design info: samples_path not set or "
                    "file missing. Re-run fit() to regenerate samples."
                )

        if self.rho_path is None or not Path(self.rho_path).exists():
            raise RuntimeError(
                "rho_path is not set or file not found. "
                "Ensure the model was trained with fit() before predicting."
            )

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        feature_paths = {var.name: var.path for var in active_dataset.features}

        print(f"\n🗺  Predicting iCAR raster → {output_file}")

        with rasterio.open(active_dataset.target.path) as ref:
            profile = ref.profile.copy()
            target_transform = ref.transform

        profile.update(dtype="uint16", count=1, nodata=0)

        mod = self._ml_model
        betas = np.array(mod["betas"])

        _mask_values = (
            (mask_value if isinstance(mask_value, (list, tuple)) else [mask_value])
            if mask is not None
            else None
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            blockinfo = far.misc.makeblock(str(active_dataset.target.path))
            nblock, nblock_x = blockinfo[0], blockinfo[1]
            x_off, y_off, nx, ny = (
                blockinfo[3],
                blockinfo[4],
                blockinfo[5],
                blockinfo[6],
            )

            for b in range(nblock):
                px = b % nblock_x
                py = b // nblock_x
                col_start, row_start = x_off[px], y_off[py]
                n_cols, n_rows = nx[px], ny[py]
                window = rasterio.windows.Window(col_start, row_start, n_cols, n_rows)

                # Geographic bounds of this block — used to read co-registered
                # rasters that may have a different pixel resolution (mask, rho).
                block_bounds = rasterio.windows.bounds(window, target_transform)

                # Apply mask before prediction
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

                # Read feature data for this block, replacing nodata with NaN
                block_dict = {}
                for name, path in feature_paths.items():
                    with rasterio.open(path) as src:
                        arr = src.read(1, window=window).astype(float)
                        if src.nodata is not None:
                            arr[arr == src.nodata] = np.nan
                    block_dict[name] = arr.ravel()

                # Read rho block — rho raster may have finer resolution than target
                with rasterio.open(self.rho_path) as rho_src:
                    rho_win = rasterio.windows.from_bounds(
                        *block_bounds, rho_src.transform
                    )
                    rho_block = rho_src.read(
                        1,
                        window=rho_win,
                        out_shape=(n_rows, n_cols),
                        resampling=rasterio.enums.Resampling.bilinear,
                    ).astype(float).ravel()

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
                    # iCAR prediction: logit(p) = X @ betas + rho
                    rho_valid = rho_block[valid_mask]
                    linear_pred = x_arr @ betas[: x_arr.shape[1]] + rho_valid
                    proba = 1.0 / (1.0 + np.exp(-linear_pred))
                    out_arr[valid_mask] = far.misc.rescale(proba).astype(np.uint16)

                dst.write(
                    out_arr.reshape(n_rows, n_cols),
                    1,
                    window=window,
                )

        print(f"✓ iCAR raster written: {output_file}")
        return output_file

"""predict_block_fn builders for SupervisedPredictor.

- supervised_block_fn(estimator): GLM/RF default — estimator.predict_proba[:,1].
- icar_block_fn(betas, rho_path): iCAR — logit = X@betas + rho(block), sigmoid.

Both are self-contained closures so they survive being passed to a worker.
"""

from pathlib import Path
from typing import Any, Sequence, Union

import numpy as np
import rasterio


def supervised_block_fn(estimator: Any):
    """Return a predict_block_fn using a classifier's predict_proba."""

    def _fn(x_arr, valid_mask, window, block_bounds, n_rows, n_cols):
        return estimator.predict_proba(x_arr)[:, 1]

    return _fn


def icar_block_fn(betas: Sequence[float], rho_path: Union[str, Path]):
    """Return a predict_block_fn adding the interpolated rho spatial effect.

    Mirrors icar_model._predict_block: read rho for the block (bilinear),
    add to the linear predictor, sigmoid.
    """
    betas_arr = np.asarray(betas, dtype=float)

    def _fn(x_arr, valid_mask, window, block_bounds, n_rows, n_cols):
        with rasterio.open(rho_path) as rho_src:
            rho_win = rasterio.windows.from_bounds(
                *block_bounds, rho_src.transform
            )
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
        linear_pred = x_arr @ betas_arr[: x_arr.shape[1]] + rho_valid
        return 1.0 / (1.0 + np.exp(-linear_pred))

    return _fn

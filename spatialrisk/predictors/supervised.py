"""SupervisedPredictor — the shared block-iterate-and-write kernel.

Extracted verbatim from BaseRiskModel.apply() (base.py:300-360) into a pure
function over (target_path, feature_paths, formula, design_info,
predict_block_fn, mask_path). Serves GLM/RF (predict_proba) and iCAR
(logit + rho) via the injected predict_block_fn.

The per-block ``for b in range(nblock)`` loop is the cooperative cancellation
checkpoint for the execution follow-on.
"""

from pathlib import Path
from typing import Callable, Dict, Optional, Union

import numpy as np
import pandas as pd
import rasterio
from patsy.highlevel import build_design_matrices

# predict_block_fn(x_arr, valid_mask, window, block_bounds, n_rows, n_cols) -> proba
PredictBlockFn = Callable[..., np.ndarray]


class SupervisedPredictor:
    """Pure raster-prediction collaborator (no model/project state)."""

    def apply(
        self,
        target_path: Union[str, Path],
        feature_paths: Dict[str, Union[str, Path]],
        formula: Optional[str],
        design_info,
        predict_block_fn: PredictBlockFn,
        mask_path: Optional[Union[str, Path]],
        output_file: Union[str, Path],
        mask_value: Union[int, float, list, tuple] = 0,
        register_prediction: Optional[Callable] = None,
        model_key: Optional[str] = None,
        dataset: Optional[object] = None,
        year: Optional[int] = None,
        model_year: Optional[int] = None,
        model_snapshot: Optional[Dict[str, object]] = None,
        window: Optional[int] = None,
    ) -> Path:
        """Write a uint16 probability raster and return its Path.

        Reproduces the legacy base.apply() block loop exactly so output is
        byte-for-byte identical to the pre-extraction pipeline.
        """
        import forestatrisk as far

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(target_path) as ref:
            profile = ref.profile.copy()
            target_transform = ref.transform
        profile.update(dtype="uint16", count=1, nodata=0)

        _mask_values = (
            (mask_value if isinstance(mask_value, (list, tuple)) else [mask_value])
            if mask_path is not None
            else None
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            blockinfo = far.misc.makeblock(str(target_path))
            nblock, nblock_x = blockinfo[0], blockinfo[1]
            x_off, y_off = blockinfo[3], blockinfo[4]
            nx, ny = blockinfo[5], blockinfo[6]

            for b in range(nblock):  # cancellation checkpoint
                px = b % nblock_x
                py = b // nblock_x
                col_start, row_start = x_off[px], y_off[py]
                n_cols, n_rows = nx[px], ny[py]
                block_window = rasterio.windows.Window(
                    col_start, row_start, n_cols, n_rows
                )
                block_bounds = rasterio.windows.bounds(
                    block_window, target_transform
                )

                mask_invalid = np.zeros(n_rows * n_cols, dtype=bool)
                if mask_path is not None:
                    with rasterio.open(mask_path) as mask_src:
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

                block_dict = {}
                for name, path in feature_paths.items():
                    with rasterio.open(path) as src:
                        arr = src.read(1, window=block_window).astype(float)
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
                        [design_info], block_df, NA_action="drop"
                    )
                    x_arr = np.asarray(x_block)
                    proba = predict_block_fn(
                        x_arr, valid_mask, block_window, block_bounds, n_rows, n_cols
                    )
                    out_arr[valid_mask] = far.misc.rescale(
                        np.asarray(proba, dtype=float)
                    ).astype(np.uint16)

                dst.write(out_arr.reshape(n_rows, n_cols), 1, window=block_window)

        if register_prediction is not None and model_key is not None:
            from spatialrisk.predictors.registration import register_supervised

            register_supervised(
                register_prediction=register_prediction,
                path=output_file,
                model_key=model_key,
                dataset=dataset,
                year=year,
                model_year=model_year,
                window=window,
                model_snapshot=model_snapshot or {},
            )

        return output_file

"""Systematic sampling — a regular grid intersected with the valid mask."""
from typing import Tuple

import numpy as np

from spatialrisk.sampling.base import SamplingStrategyBase


class SystematicSampling(SamplingStrategyBase):
    def select(self, valid_indices, *, n_samples=None, shape=None,
               spacing_m=None, res_m=None, **_) -> Tuple[np.ndarray, np.ndarray]:
        rows, cols = valid_indices
        n_valid = len(rows)

        if spacing_m is not None:
            # Distance mode.
            if spacing_m <= 0:
                raise ValueError("spacing_m must be > 0.")
            if res_m is None:
                raise ValueError(
                    "Distance-based systematic sampling requires res_m "
                    "(pixel size in metres)."
                )
            if shape is None:
                raise ValueError("Systematic sampling requires the raster shape.")
            res_y, res_x = res_m
            step_row = max(1, int(round(spacing_m / res_y)))
            step_col = max(1, int(round(spacing_m / res_x)))
        else:
            # Count mode — keep the original behaviour (no shape needed here).
            if n_samples is None or n_samples >= n_valid:
                return valid_indices
            if shape is None:
                raise ValueError("Systematic sampling requires the raster shape.")
            step = max(1, int(round(np.sqrt(n_valid / n_samples))))
            step_row = step_col = step

        valid_mask = np.zeros(shape, dtype=bool)
        valid_mask[rows, cols] = True
        gr, gc = np.meshgrid(
            np.arange(0, shape[0], step_row),
            np.arange(0, shape[1], step_col),
            indexing="ij",
        )
        gr, gc = gr.ravel(), gc.ravel()
        keep = valid_mask[gr, gc]
        return gr[keep], gc[keep]

"""Systematic sampling — a regular grid intersected with the valid mask."""
from typing import Optional, Tuple

import numpy as np

from spatialrisk.sampling.base import SamplingStrategyBase


class SystematicSampling(SamplingStrategyBase):
    def select(self, valid_indices, *, n_samples, shape=None, **_) -> Tuple[np.ndarray, np.ndarray]:
        rows, cols = valid_indices
        n_valid = len(rows)
        if n_samples is None or n_samples >= n_valid:
            return valid_indices
        if shape is None:
            raise ValueError("Systematic sampling requires the raster shape.")
        step = max(1, int(round(np.sqrt(n_valid / n_samples))))

        valid_mask = np.zeros(shape, dtype=bool)
        valid_mask[rows, cols] = True
        gr, gc = np.meshgrid(
            np.arange(0, shape[0], step), np.arange(0, shape[1], step), indexing="ij"
        )
        gr, gc = gr.ravel(), gc.ravel()
        keep = valid_mask[gr, gc]
        return gr[keep], gc[keep]

"""Simple random sampling over valid pixels."""
from typing import Optional, Tuple

import numpy as np

from spatialrisk.sampling.base import SamplingStrategyBase


class RandomSampling(SamplingStrategyBase):
    def select(self, valid_indices, *, n_samples, seed=None, **_) -> Tuple[np.ndarray, np.ndarray]:
        rows, cols = valid_indices
        n_valid = len(rows)
        if n_samples is None or n_samples >= n_valid:
            return valid_indices
        rng = np.random.default_rng(seed)
        idx = rng.choice(n_valid, size=n_samples, replace=False)
        return rows[idx], cols[idx]

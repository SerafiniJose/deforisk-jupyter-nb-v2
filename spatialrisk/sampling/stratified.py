"""Stratified sampling — per-class draws via an allocation method."""
from typing import Optional, Tuple

import numpy as np

from spatialrisk.sampling import allocation as alloc
from spatialrisk.sampling.base import SamplingStrategyBase

_ALLOCATORS = {
    "equal": lambda counts, n, adapt, pa: alloc.allocate_equal(counts, n),
    "proportional": lambda counts, n, adapt, pa: alloc.allocate_proportional(counts, n),
    "deforisk": lambda counts, n, adapt, pa: alloc.allocate_deforisk(
        counts, n, adapt=adapt, pixel_area_ha=pa
    ),
}


class StratifiedSampling(SamplingStrategyBase):
    def select(
        self,
        valid_indices,
        *,
        n_samples,
        seed=None,
        strata_values=None,
        allocation="equal",
        adapt=False,
        pixel_area_ha=None,
        **_,
    ) -> Tuple[np.ndarray, np.ndarray]:
        if strata_values is None:
            raise ValueError("Stratified sampling requires strata_values.")
        rows, cols = valid_indices
        classes = np.unique(strata_values)
        class_counts = {int(c): int((strata_values == c).sum()) for c in classes}

        if n_samples is None:
            # Uniform with random/systematic: None => draw all available per class.
            per_class = dict(class_counts)
        else:
            allocator = _ALLOCATORS.get(allocation or "equal")
            if allocator is None:
                raise ValueError(f"Unknown allocation method: {allocation}")
            per_class = allocator(class_counts, n_samples, adapt, pixel_area_ha)

        rng = np.random.default_rng(seed)
        # Event-first ordering: descending class value puts 1 (event) before 0.
        out_rows, out_cols = [], []
        for c in sorted(class_counts, reverse=True):
            n_c = per_class.get(c, 0)
            if n_c <= 0:
                continue
            members = np.where(strata_values == c)[0]
            pick = rng.choice(members, size=min(n_c, len(members)), replace=False)
            out_rows.append(rows[pick])
            out_cols.append(cols[pick])
        if not out_rows:
            return np.array([], dtype=int), np.array([], dtype=int)
        return np.concatenate(out_rows), np.concatenate(out_cols)

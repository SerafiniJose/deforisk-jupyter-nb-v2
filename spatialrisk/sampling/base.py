"""Abstract base class for sampling strategies (pure: array in, indices out)."""
from abc import ABC, abstractmethod
from typing import Optional, Tuple

import numpy as np


class SamplingStrategyBase(ABC):
    """A sampling strategy selects pixel indices from a set of valid pixels.

    Implementations are pure — they take index arrays and return index arrays,
    with no raster or file I/O (that lives in service.py). This keeps them
    unit-testable without fixtures.
    """

    @abstractmethod
    def select(
        self,
        valid_indices: Tuple[np.ndarray, np.ndarray],
        *,
        n_samples: Optional[int],
        seed: Optional[int] = None,
        strata_values: Optional[np.ndarray] = None,
        shape: Optional[Tuple[int, int]] = None,
        allocation: Optional[str] = None,
        adapt: bool = False,
        pixel_area_ha: Optional[float] = None,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Return (row_indices, col_indices) of the selected pixels."""
        raise NotImplementedError

"""
Sampling module for raster data.

Provides different sampling strategies for extracting pixel samples from rasters.
Handles random, stratified, and systematic sampling with reproducibility.
"""

from typing import Optional, Tuple, Literal
from enum import Enum
import numpy as np
from pydantic import BaseModel, Field, ConfigDict


class SamplingStrategy(str, Enum):
    """Valid sampling strategies for raster data."""

    random = "random"
    stratified = "stratified"
    systematic = "systematic"
    legacy = "legacy"


class Sampling(BaseModel):
    """Sampling strategies for raster data.

    Provides different sampling strategies for extracting pixel samples from rasters.
    Handles random, stratified, systematic, and legacy sampling with reproducibility.

    Attributes
    ----------
    strategy : SamplingStrategy or str
        Sampling strategy: 'random', 'stratified', 'systematic', or 'legacy'
    n_samples : int, optional
        Number of samples to draw. If None, uses all pixels.
        For 'legacy', this is the number of samples *per class* (deforested and
        forest), so the total output will be up to 2 x n_samples rows.
    seed : int, optional
        Random seed for reproducibility
    adapt : bool, optional
        Legacy strategy only. When True, adjusts n_samples based on total forest
        area: 1 000 samples per 1 Mha, clipped to [10 000, 50 000]. Requires
        ``pixel_area_ha`` to be set. Default is True.
    pixel_area_ha : float, optional
        Legacy strategy only. Area of a single raster pixel in hectares, used
        for the adaptive n_samples calculation.
    """

    model_config = ConfigDict(frozen=True)

    strategy: SamplingStrategy = Field(
        default=SamplingStrategy.random, description="Sampling strategy"
    )
    n_samples: Optional[int] = Field(
        default=10000, description="Number of samples to draw"
    )
    seed: Optional[int] = Field(default=None, description="Random seed")
    adapt: Optional[bool] = Field(
        default=True,
        description="Adapt n_samples to forest area (legacy strategy only)",
    )
    pixel_area_ha: Optional[float] = Field(
        default=None,
        description="Pixel area in hectares for adaptive sampling (legacy strategy only)",
    )

    def __init__(
        self,
        strategy: str = "random",
        n_samples: Optional[int] = 10000,
        seed: Optional[int] = None,
        adapt: bool = True,
        pixel_area_ha: Optional[float] = None,
        **kwargs,
    ):
        """Initialize sampling configuration.

        Parameters
        ----------
        strategy : str or SamplingStrategy, optional
            Sampling strategy: 'random', 'stratified', 'systematic', or 'legacy'
            (default: 'random')
        n_samples : int, optional
            Number of samples to draw (default: 10000). If None, uses all pixels.
            For 'legacy', this is samples *per class* (deforested / forest).
        seed : int, optional
            Random seed for reproducibility
        adapt : bool, optional
            Legacy only. Adapt n_samples to forest area (default: True).
        pixel_area_ha : float, optional
            Legacy only. Pixel area in hectares, required when adapt=True.
        """
        # Convert string to enum if needed
        if isinstance(strategy, str):
            try:
                strategy = SamplingStrategy(strategy)
            except ValueError:
                valid_strategies = [s.value for s in SamplingStrategy]
                raise ValueError(
                    f"Invalid sampling strategy '{strategy}'. "
                    f"Must be one of: {', '.join(valid_strategies)}"
                )

        super().__init__(
            strategy=strategy,
            n_samples=n_samples,
            seed=seed,
            adapt=adapt,
            pixel_area_ha=pixel_area_ha,
            **kwargs,
        )

    def sample_indices(
        self,
        valid_indices: Tuple[np.ndarray, np.ndarray],
        target_values: Optional[np.ndarray] = None,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Sample pixel indices according to the configured strategy.

        Parameters
        ----------
        valid_indices : Tuple[np.ndarray, np.ndarray]
            Tuple of (row_indices, col_indices) for valid pixels
        target_values : np.ndarray, optional
            Target variable values for stratified sampling

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            Tuple of (sampled_row_indices, sampled_col_indices)
        """
        # Set random seed if provided
        if self.seed is not None:
            np.random.seed(self.seed)

        n_valid = len(valid_indices[0])

        # Legacy strategy manages its own per-class n_samples logic
        if self.strategy == SamplingStrategy.legacy:
            if target_values is None:
                raise ValueError("Legacy sampling requires target_values parameter")
            return self._sample_legacy(valid_indices, target_values)

        # If n_samples is None or greater than valid pixels, use all
        if self.n_samples is None or self.n_samples >= n_valid:
            print(f"  Using all {n_valid:,} valid pixels")
            return valid_indices

        # Sample according to strategy
        if self.strategy == SamplingStrategy.random:
            return self._sample_random(valid_indices, n_valid)

        elif self.strategy == SamplingStrategy.stratified:
            if target_values is None:
                raise ValueError("Stratified sampling requires target_values parameter")
            return self._sample_stratified(valid_indices, target_values)

        elif self.strategy == SamplingStrategy.systematic:
            return self._sample_systematic(valid_indices, n_valid)

        else:
            raise ValueError(f"Unknown sampling strategy: {self.strategy}")

    def _sample_random(
        self, valid_indices: Tuple[np.ndarray, np.ndarray], n_valid: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Random sampling strategy."""
        sample_idx = np.random.choice(n_valid, size=self.n_samples, replace=False)
        sample_indices = (
            valid_indices[0][sample_idx],
            valid_indices[1][sample_idx],
        )
        print(f"  Sampled {self.n_samples:,} random pixels")
        return sample_indices

    def _sample_stratified(
        self, valid_indices: Tuple[np.ndarray, np.ndarray], target_values: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Stratified sampling strategy based on target variable classes."""
        # Get unique classes and their counts
        unique_classes, class_counts = np.unique(target_values, return_counts=True)
        n_per_class = self.n_samples // len(unique_classes)

        sampled_row_idx = []
        sampled_col_idx = []

        for cls in unique_classes:
            cls_indices = np.where(target_values == cls)[0]
            n_cls_samples = min(n_per_class, len(cls_indices))
            cls_sample_idx = np.random.choice(
                cls_indices, size=n_cls_samples, replace=False
            )

            sampled_row_idx.extend(valid_indices[0][cls_sample_idx])
            sampled_col_idx.extend(valid_indices[1][cls_sample_idx])

        sample_indices = (np.array(sampled_row_idx), np.array(sampled_col_idx))
        print(f"  Sampled {len(sampled_row_idx):,} stratified pixels")
        return sample_indices

    def _sample_systematic(
        self, valid_indices: Tuple[np.ndarray, np.ndarray], n_valid: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Systematic grid sampling strategy."""
        step = int(np.sqrt(n_valid / self.n_samples))
        sample_idx = np.arange(0, n_valid, step)
        sample_indices = (
            valid_indices[0][sample_idx],
            valid_indices[1][sample_idx],
        )
        print(f"  Sampled {len(sample_idx):,} systematic pixels")
        return sample_indices

    def _sample_legacy(
        self, valid_indices: Tuple[np.ndarray, np.ndarray], target_values: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Legacy balanced sampling strategy (forestatrisk-style).

        Mirrors the original forestatrisk sampling approach: draws ``n_samples``
        pixels from the deforested class (target == 1) and ``n_samples`` pixels
        from the forest class (target == 0) independently, then concatenates
        them. The deforested samples come first in the output.

        When ``adapt=True`` and ``pixel_area_ha`` is provided, ``n_samples`` is
        adjusted based on the total forested area following the rule
        1 000 samples per 1 Mha, clipped to [10 000, 50 000].

        Parameters
        ----------
        valid_indices : Tuple[np.ndarray, np.ndarray]
            (row_indices, col_indices) for all valid pixels.
        target_values : np.ndarray
            Target variable values aligned with ``valid_indices``.
            Expected values: 1 = deforested (event of interest), 0 = forest.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            (row_indices, col_indices) with deforested samples first.
        """
        # Split into deforested (1) and forest (0) index sets
        # Convention: target raster is 1 = deforestation (event), 0 = forest.
        defor_mask = target_values == 1
        forest_mask = target_values == 0

        defor_rows = valid_indices[0][defor_mask]
        defor_cols = valid_indices[1][defor_mask]
        forest_rows = valid_indices[0][forest_mask]
        forest_cols = valid_indices[1][forest_mask]

        ndc = len(defor_rows)  # number of deforested pixels
        nfc = len(forest_rows)  # number of forest pixels

        # Adaptive n_samples based on total forest area
        nsamp = self.n_samples
        if self.adapt and self.pixel_area_ha is not None:
            total_area_ha = self.pixel_area_ha * (ndc + nfc)
            nsamp_prop = 1000 * total_area_ha / 1e6  # 1 000 per 1 Mha
            if nsamp_prop >= 50000:
                nsamp = 50000
            elif nsamp_prop <= 10000:
                nsamp = 10000
            else:
                nsamp = int(np.rint(nsamp_prop))
            print(
                f"  Adapted n_samples to {nsamp:,} "
                f"(total area: {total_area_ha / 1e6:.3f} Mha)"
            )
        elif self.adapt and self.pixel_area_ha is None:
            print(
                "  Warning: adapt=True but pixel_area_ha not set; using n_samples as-is"
            )

        # Draw deforested pixels
        if nsamp < ndc:
            idx = np.random.choice(ndc, size=nsamp, replace=False)
            sampled_defor = (defor_rows[idx], defor_cols[idx])
        else:
            if nsamp > ndc:
                print(
                    f"  Warning: only {ndc:,} deforested pixels available "
                    f"(requested {nsamp:,}); using all"
                )
            sampled_defor = (defor_rows, defor_cols)

        # Draw forest pixels
        if nsamp < nfc:
            idx = np.random.choice(nfc, size=nsamp, replace=False)
            sampled_forest = (forest_rows[idx], forest_cols[idx])
        else:
            if nsamp > nfc:
                print(
                    f"  Warning: only {nfc:,} forest pixels available "
                    f"(requested {nsamp:,}); using all"
                )
            sampled_forest = (forest_rows, forest_cols)

        # Concatenate — deforested first, matching the original notebook order
        row_indices = np.concatenate([sampled_defor[0], sampled_forest[0]])
        col_indices = np.concatenate([sampled_defor[1], sampled_forest[1]])

        n_defor_out = len(sampled_defor[0])
        n_forest_out = len(sampled_forest[0])
        print(
            f"  Sampled {n_defor_out:,} deforested + {n_forest_out:,} forest "
            f"= {n_defor_out + n_forest_out:,} total pixels (legacy)"
        )
        return (row_indices, col_indices)

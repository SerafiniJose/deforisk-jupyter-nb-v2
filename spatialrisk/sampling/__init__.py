"""Sampling package — location sampling strategies and point generation.

`Sampling` is re-exported from `legacy` for backward compatibility during the
decoupling migration and is removed in the final cleanup task.
"""
from spatialrisk.sampling.types import SamplingStrategy, AllocationMethod
from spatialrisk.sampling.base import SamplingStrategyBase
from spatialrisk.sampling.legacy import Sampling  # TEMP back-compat

__all__ = [
    "SamplingStrategy",
    "AllocationMethod",
    "SamplingStrategyBase",
    "Sampling",
]

"""Sampling package — location sampling strategies and point generation.

`Sampling` is re-exported from `legacy` for backward compatibility during the
decoupling migration and is removed in the final cleanup task.
"""
from spatialrisk.sampling.types import SamplingStrategy, AllocationMethod
from spatialrisk.sampling.base import SamplingStrategyBase
from spatialrisk.sampling.legacy import Sampling  # TEMP back-compat
from spatialrisk.sampling.random import RandomSampling
from spatialrisk.sampling.stratified import StratifiedSampling
from spatialrisk.sampling.systematic import SystematicSampling

__all__ = [
    "SamplingStrategy",
    "AllocationMethod",
    "SamplingStrategyBase",
    "Sampling",
    "RandomSampling",
    "StratifiedSampling",
    "SystematicSampling",
]

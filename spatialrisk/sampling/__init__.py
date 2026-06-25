"""Sampling package — location sampling strategies and point generation."""
from spatialrisk.sampling.types import SamplingStrategy, AllocationMethod
from spatialrisk.sampling.base import SamplingStrategyBase
from spatialrisk.sampling.random import RandomSampling
from spatialrisk.sampling.stratified import StratifiedSampling
from spatialrisk.sampling.systematic import SystematicSampling
from spatialrisk.sampling.service import generate_points

__all__ = [
    "SamplingStrategy",
    "AllocationMethod",
    "SamplingStrategyBase",
    "RandomSampling",
    "StratifiedSampling",
    "SystematicSampling",
    "generate_points",
]

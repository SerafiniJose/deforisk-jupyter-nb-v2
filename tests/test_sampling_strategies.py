import numpy as np
import pytest

from spatialrisk.sampling.types import SamplingStrategy, AllocationMethod


def test_strategy_enum_values():
    assert {s.value for s in SamplingStrategy} == {"random", "stratified", "systematic"}


def test_allocation_enum_values():
    assert {a.value for a in AllocationMethod} == {"equal", "proportional", "deforisk"}


def test_legacy_sampling_still_importable():
    # Back-compat re-export during migration (removed in cleanup task).
    from spatialrisk.sampling import Sampling
    assert Sampling(strategy="random", n_samples=5).n_samples == 5

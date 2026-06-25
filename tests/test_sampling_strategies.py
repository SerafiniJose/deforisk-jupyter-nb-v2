import numpy as np
import pytest

from spatialrisk.sampling.types import SamplingStrategy, AllocationMethod
from spatialrisk.sampling.random import RandomSampling
from spatialrisk.sampling.stratified import StratifiedSampling
from spatialrisk.sampling.systematic import SystematicSampling


def _valid_indices(n):
    # n valid pixels laid out on a single row for simplicity.
    return (np.zeros(n, dtype=int), np.arange(n, dtype=int))


def test_strategy_enum_values():
    assert {s.value for s in SamplingStrategy} == {"random", "stratified", "systematic"}


def test_allocation_enum_values():
    assert {a.value for a in AllocationMethod} == {"equal", "proportional", "deforisk"}


def test_random_draws_requested_count_reproducibly():
    vi = _valid_indices(1000)
    r1 = RandomSampling().select(vi, n_samples=100, seed=42)
    r2 = RandomSampling().select(vi, n_samples=100, seed=42)
    assert len(r1[0]) == 100
    assert np.array_equal(r1[0], r2[0]) and np.array_equal(r1[1], r2[1])


def test_random_uses_all_when_n_exceeds_valid():
    vi = _valid_indices(50)
    r = RandomSampling().select(vi, n_samples=100, seed=1)
    assert len(r[0]) == 50


def test_stratified_equal_balances_classes():
    rows = np.zeros(10_300, dtype=int)
    cols = np.arange(10_300, dtype=int)
    strata = np.concatenate([np.zeros(10_000, dtype=int), np.ones(300, dtype=int)])
    r = StratifiedSampling().select(
        (rows, cols), n_samples=1000, seed=1, strata_values=strata, allocation="equal"
    )
    drawn = strata[cols.searchsorted(r[1])]  # strata of selected (cols are unique ids)
    assert (drawn == 0).sum() == 500
    assert (drawn == 1).sum() == 300  # capped at availability


def test_stratified_deforisk_orders_event_first():
    rows = np.zeros(2000, dtype=int)
    cols = np.arange(2000, dtype=int)
    strata = np.concatenate([np.zeros(1000, dtype=int), np.ones(1000, dtype=int)])
    r = StratifiedSampling().select(
        (rows, cols), n_samples=200, seed=1, strata_values=strata,
        allocation="deforisk",
    )
    drawn = strata[r[1]]
    assert (drawn[:200] == 1).all()   # event (1) rows first
    assert (drawn[200:] == 0).all()
    assert len(r[0]) == 400           # 200 per class


def test_systematic_spreads_over_grid_within_mask():
    # 100x100 grid fully valid; ask for ~100 -> step ~10 -> ~100 points.
    H = W = 100
    rr, cc = np.meshgrid(np.arange(H), np.arange(W), indexing="ij")
    vi = (rr.ravel(), cc.ravel())
    r = SystematicSampling().select(vi, n_samples=100, shape=(H, W))
    assert 80 <= len(r[0]) <= 121
    # regular spacing: unique rows are multiples of the step
    assert len(set(np.diff(np.unique(r[0])))) == 1


def test_stratified_none_draws_all_per_class():
    rows = np.zeros(300, dtype=int)
    cols = np.arange(300, dtype=int)
    strata = np.concatenate([np.zeros(200, dtype=int), np.ones(100, dtype=int)])
    r = StratifiedSampling().select(
        (rows, cols), n_samples=None, strata_values=strata, allocation="equal"
    )
    drawn = strata[r[1]]
    assert (drawn == 0).sum() == 200   # all class-0 pixels
    assert (drawn == 1).sum() == 100   # all class-1 pixels

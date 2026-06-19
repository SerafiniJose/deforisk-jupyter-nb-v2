"""BaseRiskModel uses a materialized sample table when one is attached."""

from pathlib import Path

import pandas as pd

from spatialrisk.mlmodels.base import BaseRiskModel


class _FakeSampleSet:
    target_name = "y"
    feature_names = ["a", "b"]
    year = 2020
    table_path = Path("/tmp/does-not-need-to-exist.csv")

    def load_table(self):
        return pd.DataFrame({"y": [0, 1, 0], "a": [1, 2, 3], "b": [4, 5, 6]})


def test_prepare_samples_uses_sample_set_table():
    m = BaseRiskModel()
    m.sample_set = _FakeSampleSet()
    m.dataset = None  # dataset deleted / not needed for fit

    df, formula = m._prepare_samples()

    assert list(df.columns) == ["y", "a", "b"]
    assert len(df) == 3
    assert m.target_name == "y"
    assert m.feature_names == ["a", "b"]
    assert m.year == 2020
    assert m.samples_path == _FakeSampleSet.table_path
    # With no dataset, the formula is built from the denormalized names.
    assert formula == "y ~ a + b"


def test_prepare_samples_without_sample_set_requires_dataset():
    m = BaseRiskModel()
    m.sample_set = None
    m.dataset = None
    try:
        m._prepare_samples()
        assert False, "expected ValueError when neither sample_set nor dataset set"
    except ValueError:
        pass

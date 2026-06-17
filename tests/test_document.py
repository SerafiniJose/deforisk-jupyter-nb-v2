import pytest
from pydantic import ValidationError
from spatialrisk.sampling import Sampling, SamplingStrategy


def test_sampling_is_frozen_and_json_safe():
    s = Sampling(strategy="legacy", n_samples=5000, seed=42, adapt=False, pixel_area_ha=0.09)
    # JSON round-trip is lossless
    dumped = s.model_dump_json()
    loaded = Sampling.model_validate_json(dumped)
    assert loaded == s
    assert loaded.strategy == SamplingStrategy.legacy
    assert loaded.n_samples == 5000
    assert loaded.seed == 42
    assert loaded.adapt is False
    assert loaded.pixel_area_ha == 0.09
    # hashable (frozen)
    assert hash(s) == hash(loaded)
    # mutation raises
    with pytest.raises(ValidationError):
        s.n_samples = 10


def test_sampling_rejects_non_json_value():
    # arbitrary_types_allowed removed -> object() cannot be a field value
    with pytest.raises(ValidationError):
        Sampling(strategy="random", n_samples=object())

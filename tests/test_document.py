import pytest
from pydantic import ValidationError
from spatialrisk.sampling import Sampling, SamplingStrategy
from spatialrisk.document import VariableId, VarRef


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


def test_variableid_frozen_and_qualified():
    a = VariableId(source="raw", name="forest_gfc", year=2020)
    b = VariableId(source="processed", name="forest_gfc", year=2020)
    assert a != b                      # same name, different source -> unambiguous
    assert VarRef is VariableId
    # defaults
    assert VariableId(source="raw", name="altitude").year is None
    # frozen + hashable
    assert hash(a) == hash(VariableId(source="raw", name="forest_gfc", year=2020))
    with pytest.raises(ValidationError):
        a.name = "x"


def test_variableid_rejects_bad_source():
    with pytest.raises(ValidationError):
        VariableId(source="other", name="x")

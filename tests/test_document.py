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


from spatialrisk.document import FrozenDict


def test_frozendict_copies_input_and_is_immutable():
    src = {"a": 1, "b": 2}
    fd = FrozenDict(src)
    src["c"] = 3                       # mutating the source must not leak in
    assert "c" not in fd
    assert dict(fd) == {"a": 1, "b": 2}
    assert fd["a"] == 1
    assert len(fd) == 2
    assert set(fd) == {"a", "b"}
    with pytest.raises(TypeError):
        fd["a"] = 99
    with pytest.raises(TypeError):
        del fd["a"]


def test_frozendict_hashable_and_equal():
    assert FrozenDict({"a": 1}) == FrozenDict({"a": 1})
    assert hash(FrozenDict({"a": 1})) == hash(FrozenDict({"a": 1}))
    assert FrozenDict() == FrozenDict({})


def test_frozendict_validates_values_in_pydantic_field():
    # core schema validates VALUES; a model with FrozenDict[str, int] rejects bad values
    class M(BaseModel):
        model_config = ConfigDict(frozen=True)
        m: FrozenDict[str, int] = FrozenDict()

    ok = M(m={"x": 1})
    assert isinstance(ok.m, FrozenDict)
    assert ok.m["x"] == 1
    with pytest.raises(ValidationError):
        M(m={"x": "not-an-int"})
    # round-trips through JSON as a normal object
    assert M.model_validate_json(ok.model_dump_json()).m["x"] == 1


# need BaseModel/ConfigDict in this test module
from pydantic import BaseModel, ConfigDict

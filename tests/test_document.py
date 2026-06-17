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
from typing import Annotated, Union
from pydantic import Field as PField, TypeAdapter
from spatialrisk.document import CatalogueRecipe, AssetRecipe, GEERecipe


def test_catalogue_recipe_defaults_and_construction():
    r = CatalogueRecipe(
        catalogue_key="forest_gfc",
        params={"tree_cover_threshold": 10, "year": 2020},
        export_kind="raster",
    )
    assert r.source == "catalogue"
    assert r.unmask_value == 255 and r.nodata_value == 255
    assert r.aoi is None and r.scale is None and r.vector_selectors is None
    assert r.params["year"] == 2020


def test_asset_recipe_construction():
    r = AssetRecipe(asset_id="projects/x/assets/y", band="B1", export_kind="raster")
    assert r.source == "asset"
    assert r.band == "B1"


def test_geerecipe_discriminator_dispatch():
    ta = TypeAdapter(GEERecipe)
    cat = ta.validate_python(
        {"source": "catalogue", "catalogue_key": "altitude", "export_kind": "raster"}
    )
    assert isinstance(cat, CatalogueRecipe)
    ass = ta.validate_python(
        {"source": "asset", "asset_id": "users/a/b", "export_kind": "vector"}
    )
    assert isinstance(ass, AssetRecipe)


def test_catalogue_recipe_rejects_non_json_param():
    with pytest.raises(ValidationError):
        CatalogueRecipe(catalogue_key="x", params={"bad": object()}, export_kind="raster")


from pydantic import computed_field  # noqa: F401  (sanity that pydantic exports it)
from spatialrisk.document import (
    LocalRasterSpec,
    LocalVectorSpec,
    GEESpec,
    VariableSpec,
)
from spatialrisk.variables.models import (
    DataType,
    RasterType,
    RasterizationMethod,
    PostProcessing,
)


def test_local_raster_spec_data_type_and_defaults():
    s = LocalRasterSpec(name="altitude", path="/d/altitude.tif", raster_type=RasterType.continuous)
    assert s.kind == "local_raster"
    assert s.data_type == DataType.raster
    assert s.year is None and s.active is True
    assert s.tags == () and s.post_processing == () and s.processing_history == ()
    assert s.derived_from is None


def test_local_vector_spec_data_type():
    s = LocalVectorSpec(name="aoi", year=2020, active=True, path="/d/aoi.shp",
                        rasterization_method=RasterizationMethod.binary)
    assert s.kind == "local_vector"
    assert s.data_type == DataType.vector


def test_gee_spec_carries_explicit_data_type():
    rec = CatalogueRecipe(catalogue_key="forest_gfc", export_kind="raster")
    s = GEESpec(name="forest_gfc", year=2020, data_type=DataType.raster,
                raster_type=RasterType.categorical, recipe=rec)
    assert s.kind == "gee"
    assert s.data_type == DataType.raster
    assert s.materialized_key is None


def test_variablespec_discriminator_dispatch_and_frozen():
    ta = TypeAdapter(VariableSpec)
    r = ta.validate_python(
        {"kind": "local_raster", "name": "x", "path": "/x.tif", "raster_type": "continuous"}
    )
    assert isinstance(r, LocalRasterSpec)
    v = ta.validate_python(
        {"kind": "local_vector", "name": "y", "path": "/y.shp", "rasterization_method": "unique"}
    )
    assert isinstance(v, LocalVectorSpec)
    with pytest.raises(ValidationError):
        r.name = "mutated"   # frozen


def test_local_raster_spec_post_processing_is_tuple_of_enum():
    s = LocalRasterSpec(name="x", path="/x.tif", raster_type=RasterType.categorical,
                        post_processing=(PostProcessing.edge, PostProcessing.dist))
    assert s.post_processing == (PostProcessing.edge, PostProcessing.dist)
    # round-trip
    loaded = LocalRasterSpec.model_validate_json(s.model_dump_json())
    assert loaded == s


from spatialrisk.document import DatasetSpec
from spatialrisk.sampling import Sampling


def test_dataset_spec_reference_only_and_roundtrip():
    ds = DatasetSpec(
        name="calibration_2020",
        year=2020,
        target_ref=VariableId(source="processed", name="forest_loss", year=2020),
        feature_refs=(
            VariableId(source="processed", name="altitude"),
            VariableId(source="processed", name="slope"),
        ),
        sampling=Sampling(strategy="random", n_samples=10000, seed=1),
    )
    assert ds.target_ref.name == "forest_loss"
    assert len(ds.feature_refs) == 2
    assert ds.sampling.n_samples == 10000
    loaded = DatasetSpec.model_validate_json(ds.model_dump_json())
    assert loaded == ds


def test_dataset_spec_defaults_and_frozen():
    ds = DatasetSpec(name="empty")
    assert ds.year is None and ds.target_ref is None
    assert ds.feature_refs == () and ds.sampling is None
    with pytest.raises(ValidationError):
        ds.name = "x"


from spatialrisk.document import (
    GLMSpec, RFSpec, ICARSpec, JNRSpec, MWSpec, ModelSpec,
)


def _common():
    return dict(
        name="calibration", project_name="p", dataset_name="calibration_2020",
        target_name="forest_loss", feature_names=("altitude", "slope"),
        year=2020, formula="fl ~ altitude + slope", parameters={"solver": "lbfgs"},
        sampling=Sampling(strategy="random", n_samples=5000), samples_path="/d/s.csv",
        trained=True, trained_at="2026-06-17T00:00:00", n_samples=5000, deviance=123.4,
    )


def test_glm_rf_carry_estimator_pickle():
    g = GLMSpec(model_type="glm", estimator_pickle="/d/glm.pkl", **_common())
    assert g.model_type == "glm" and g.estimator_pickle == "/d/glm.pkl"
    r = RFSpec(model_type="rf", estimator_pickle="/d/rf.pkl", **_common())
    assert r.model_type == "rf"
    assert g.feature_names == ("altitude", "slope")


def test_icar_carries_rho_path():
    s = ICARSpec(model_type="icar", estimator_pickle="/d/icar.pkl", rho_path="/d/rho.tif", **_common())
    assert s.rho_path == "/d/rho.tif"


def test_jnr_typed_fields():
    s = JNRSpec(model_type="jnr", dist_thresh=120.0, dist_bins=(0.0, 30.0, 60.0),
                defrate_files={"calibration": "/d/defrate.tif"}, **_common())
    assert s.dist_thresh == 120.0
    assert s.dist_bins == (0.0, 30.0, 60.0)
    assert s.defrate_files["calibration"] == "/d/defrate.tif"


def test_mw_typed_fields():
    s = MWSpec(model_type="mw", dist_thresh=120.0, win_size_list=(5, 11, 21),
               ldefrate_files={"5": "/d/ldef5.tif"}, **_common())
    assert s.win_size_list == (5, 11, 21)
    assert s.ldefrate_files["5"] == "/d/ldef5.tif"


def test_modelspec_discriminator_dispatch_and_frozen():
    ta = TypeAdapter(ModelSpec)
    payload = {"model_type": "jnr", "name": "c", "project_name": "p",
               "dataset_name": "d", "target_name": "t", "feature_names": [],
               "year": None, "formula": None, "parameters": {}, "sampling": None,
               "samples_path": None, "trained": False, "trained_at": None,
               "n_samples": None, "deviance": None, "dist_thresh": None,
               "dist_bins": [], "defrate_files": {}}
    s = ta.validate_python(payload)
    assert isinstance(s, JNRSpec)
    with pytest.raises(ValidationError):
        s.name = "x"   # frozen


from spatialrisk.document import PredictionSpec


def test_prediction_spec_defaults_and_roundtrip():
    p = PredictionSpec(
        path="/d/pred.tif",
        model_key="calibration_glm",
        dataset_name="validation_2025",
        year=2025,
        window=11,
        tags=("forecast",),
        created_at="2026-06-17T00:00:00",
        model_snapshot={"model_type": "glm", "deviance": 1.2},
        dataset_snapshot={"name": "validation_2025", "target_name": "forest_loss",
                          "target_year": 2025, "feature_names": ["altitude"]},
        metrics={"auc": 0.81},
    )
    assert p.name is None and p.active is True
    assert p.model_key == "calibration_glm"
    loaded = PredictionSpec.model_validate_json(p.model_dump_json())
    assert loaded == p
    # no live project back-ref field exists
    assert "project" not in PredictionSpec.model_fields


def test_prediction_spec_rejects_non_json_snapshot():
    with pytest.raises(ValidationError):
        PredictionSpec(path="/d/p.tif", model_key="m", dataset_name="d",
                       model_snapshot={"bad": object()})


from spatialrisk.document import ProjectDocument


def test_project_document_defaults():
    doc = ProjectDocument(project_name="amazonia")
    assert doc.schema_version == 1
    assert doc.aoi is None and doc.base_raster_ref is None
    assert isinstance(doc.raw_variables, FrozenDict)
    assert len(doc.raw_variables) == 0
    assert isinstance(doc.datasets, FrozenDict)
    assert isinstance(doc.models, FrozenDict)
    assert isinstance(doc.predictions, FrozenDict)


def test_project_document_registries_are_frozen():
    raster = LocalRasterSpec(name="altitude", path="/d/altitude.tif", raster_type=RasterType.continuous)
    doc = ProjectDocument(project_name="p", raw_variables={"altitude": raster})
    assert doc.raw_variables["altitude"] == raster
    with pytest.raises(TypeError):
        doc.raw_variables["x"] = raster
    with pytest.raises(TypeError):
        del doc.raw_variables["altitude"]
    for reg in ("processed_variables", "datasets", "models", "predictions"):
        with pytest.raises(TypeError):
            getattr(doc, reg)["k"] = None


def test_project_document_doc_field_is_frozen():
    doc = ProjectDocument(project_name="p")
    with pytest.raises(ValidationError):
        doc.project_name = "other"
